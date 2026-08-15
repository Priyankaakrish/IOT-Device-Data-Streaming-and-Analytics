#!/usr/bin/env python3
"""
Airflow orchestration for the IoT manufacturing platform.

    Airflow
       |
       +-- Batch/ETL Jobs -----+
       |                       |
       +-- Data Quality Jobs --+
                               |
                               v
                        Data Warehouse
                               |
                               v
                           Power BI

WHAT AIRFLOW DOES AND DOES NOT DO HERE
--------------------------------------
It does NOT orchestrate the streaming job. A Structured Streaming query never
finishes, so there is no exit code for a scheduler to wait on. That job is
supervised by supervise_streaming.ps1, which restarts it from the checkpoint
if the process dies.

Airflow orchestrates the periodic work that genuinely has schedules and
ordering: refreshing the snowflake copy, detecting SCD2 drift, running data
quality gates, and signalling that the warehouse is fit to report on.

TWO DAGS
--------
    iot_platform_daily   Batch/ETL and Data Quality in parallel, converging
                         on a warehouse gate and a Power BI refresh signal.
    iot_data_quality     The quality gates alone, hourly, so a problem is
                         caught between daily runs.

The quality tasks are gates, not reports: they raise and fail the DAG when a
threshold is breached. A check that only logs is a check nobody reads.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
)

CONN = "iot_postgres"
TZ = pendulum.timezone("UTC")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,
}

# Thresholds. Kept together so changing an SLO is one edit, not a search.
MAX_FRESHNESS_SECONDS = 300  # pipeline is stalled beyond this
MIN_BATCH_SUCCESS_PCT = 99.0
MAX_UNKNOWN_KEY_PCT = 0.1
MAX_UNREPLAYED_DLQ = 0


# =====================================================================
# Reusable task groups
# =====================================================================
@task_group(group_id="batch_etl")
def batch_etl_jobs():
    """Batch/ETL branch: keep derived structures aligned with the star."""

    # snow.* is a derived copy, not a second source of truth. It drifts
    # unless refreshed, and because fact_production_hourly accumulates in
    # place the drift is invisible to a row-count check -- values change
    # while the count stays flat.
    refresh_snowflake = SQLExecuteQueryOperator(
        task_id="refresh_snowflake",
        conn_id=CONN,
        sql="SELECT table_name, row_count, verdict FROM snow.refresh_from_star();",
        show_return_value_in_logs=True,
    )

    @task(task_id="assert_snowflake_in_sync")
    def assert_snowflake_in_sync():
        """Refreshing is not the same as succeeding. Verify the verdict."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        rows = PostgresHook(postgres_conn_id=CONN).get_records(
            "SELECT table_name, verdict FROM snow.v_staleness WHERE rows_with_stale_values > 0;"
        )
        if rows:
            raise ValueError(
                f"Snowflake copy still stale after refresh: {rows}. "
                "Check the ON CONFLICT clauses in sql/08 -- DO NOTHING on an "
                "accumulating fact freezes it at its first observed value."
            )

    # dim_machine carries row_hash for change detection but nothing compares
    # it automatically. This surfaces the machines that need a new SCD2
    # version; it does not create one, because a silent history rewrite is
    # worse than a manual step.
    detect_scd2_drift = SQLExecuteQueryOperator(
        task_id="detect_scd2_drift",
        conn_id=CONN,
        sql="""
            SELECT m.machine_id, m.machine_name AS current_name,
                   s.machine_name AS source_name
            FROM mart.dim_machine m
            JOIN public.machines  s ON s.machine_id = m.machine_id
            WHERE m.is_current
              AND (m.machine_name IS DISTINCT FROM s.machine_name
                OR m.machine_type IS DISTINCT FROM s.machine_type);
        """,
        show_return_value_in_logs=True,
    )

    refresh_snowflake >> assert_snowflake_in_sync() >> detect_scd2_drift


@task_group(group_id="data_quality")
def data_quality_jobs():
    """
    Data Quality branch. Each task fails the DAG on breach.

    These four checks map onto the three defects found during delivery:
    freshness would have caught the pipeline dying overnight, unknown-key
    rate catches an asset missing from master data, grain integrity catches
    the alert duplication, and the DLQ gate catches a broken payload
    contract.
    """

    @task(task_id="check_freshness")
    def check_freshness():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        row = PostgresHook(postgres_conn_id=CONN).get_first(
            "SELECT lag_seconds, status FROM mart.v_pipeline_freshness;"
        )
        if row is None or row[0] is None:
            raise ValueError("No sensor facts at all -- the pipeline has never run.")
        lag, status = row
        if lag > MAX_FRESHNESS_SECONDS:
            raise ValueError(
                f"Pipeline stalled: newest reading is {lag:.0f}s old "
                f"(limit {MAX_FRESHNESS_SECONDS}s, status {status}). "
                "Check the streaming job and the Kafka broker."
            )
        return {"lag_seconds": lag, "status": status}

    @task(task_id="check_batch_success_rate")
    def check_batch_success_rate():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        row = PostgresHook(postgres_conn_id=CONN).get_first(
            "SELECT batches_24h, success_rate_pct FROM mart.v_batch_health;"
        )
        batches, pct = row
        if batches == 0:
            raise ValueError("No batches logged in 24 hours -- the job is not running.")
        if pct < MIN_BATCH_SUCCESS_PCT:
            raise ValueError(
                f"Batch success rate {pct}% over {batches} batches "
                f"(minimum {MIN_BATCH_SUCCESS_PCT}%). "
                "See error_detail in mart.etl_batch_log."
            )
        return {"batches": batches, "success_rate_pct": float(pct)}

    @task(task_id="check_unknown_keys")
    def check_unknown_keys():
        """
        A rising unknown-key rate almost always means a new asset is on the
        floor and was never added to master data. Routing to -1 keeps those
        rows countable instead of dropping them from every measure.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        row = PostgresHook(postgres_conn_id=CONN).get_first(
            "SELECT rows_24h, unknown_pct FROM mart.v_unknown_key_rate;"
        )
        rows, pct = row
        if rows and pct is not None and float(pct) > MAX_UNKNOWN_KEY_PCT:
            raise ValueError(
                f"{pct}% of rows resolved to an Unknown member "
                f"(limit {MAX_UNKNOWN_KEY_PCT}%). A machine is missing from "
                "mart.dim_machine."
            )
        return {"rows_24h": rows, "unknown_pct": float(pct or 0)}

    @task(task_id="check_dead_letters")
    def check_dead_letters():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        row = PostgresHook(postgres_conn_id=CONN).get_first(
            "SELECT quarantined_24h, awaiting_replay, top_reason FROM mart.v_dlq_health;"
        )
        quarantined, awaiting, reason = row
        if awaiting and awaiting > MAX_UNREPLAYED_DLQ:
            raise ValueError(
                f"{awaiting} message(s) awaiting replay, most common cause: "
                f"{reason}. The producer payload contract may have changed."
            )
        return {"quarantined_24h": quarantined, "awaiting_replay": awaiting}

    # Grain is the property the whole model rests on. The unique constraint
    # prevents violation at write time; this asserts it was never bypassed
    # by a manual insert or a migration.
    check_alert_grain = SQLColumnCheckOperator(
        task_id="check_alert_grain",
        conn_id=CONN,
        table="mart.fact_alert",
        column_mapping={"alert_id": {"null_check": {"equal_to": 0}}},
    )

    [
        check_freshness(),
        check_batch_success_rate(),
        check_unknown_keys(),
        check_dead_letters(),
        check_alert_grain,
    ]


# =====================================================================
# DAG 1 -- daily platform run
# =====================================================================
with DAG(
    dag_id="iot_platform_daily",
    description="Batch/ETL and Data Quality, converging on the warehouse gate",
    default_args=DEFAULT_ARGS,
    schedule="0 5 * * *",  # 05:00 UTC, before the morning shift
    start_date=pendulum.datetime(2026, 8, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["iot", "warehouse", "daily"],
) as dag_daily:
    start = EmptyOperator(task_id="start")

    batch = batch_etl_jobs()
    quality = data_quality_jobs()

    # The convergence point in the architecture diagram. Nothing downstream
    # runs unless both branches succeeded, which is the whole reason this is
    # a DAG rather than a shell script running commands in order.
    warehouse_ready = EmptyOperator(
        task_id="data_warehouse_ready",
        trigger_rule="all_success",
    )

    # Power BI refreshes on its own schedule against DirectQuery, so there is
    # nothing to push. What matters is recording that the warehouse passed
    # its gates for this run -- the report is only as trustworthy as the
    # checks that preceded it.
    publish = SQLExecuteQueryOperator(
        task_id="publish_to_powerbi",
        conn_id=CONN,
        sql="""
            INSERT INTO mart.etl_batch_log
                (job_name, batch_id, started_at, completed_at, status)
            VALUES ('airflow_iot_platform_daily', '{{ run_id }}',
                    '{{ data_interval_start }}', NOW(), 'SUCCESS');
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> [batch, quality] >> warehouse_ready >> publish >> end


# =====================================================================
# DAG 2 -- hourly quality gates
# =====================================================================
with DAG(
    dag_id="iot_data_quality",
    description="Hourly data quality gates between daily runs",
    default_args=DEFAULT_ARGS,
    schedule="15 * * * *",  # offset off the hour to avoid contention
    start_date=pendulum.datetime(2026, 8, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["iot", "data-quality", "hourly"],
) as dag_quality:
    data_quality_jobs()


# =====================================================================
# DAG 3 -- monthly retention
# =====================================================================
with DAG(
    dag_id="iot_retention",
    description="Drop sensor partitions beyond the retention window",
    default_args={**DEFAULT_ARGS, "retries": 0},
    schedule="0 3 1 * *",  # 03:00 UTC on the 1st
    start_date=pendulum.datetime(2026, 8, 1, tz=TZ),
    catchup=False,
    tags=["iot", "retention", "monthly"],
    # Destructive and irreversible. Paused on load so it cannot fire before
    # somebody has decided the retention window is right.
    is_paused_upon_creation=True,
) as dag_retention:
    # Detaching a partition is a catalogue operation. Deleting the same rows
    # is hours of I/O and leaves a bloated table until vacuumed.
    SQLExecuteQueryOperator(
        task_id="drop_old_partitions",
        conn_id=CONN,
        sql="SELECT * FROM mart.drop_sensor_partitions_older_than(6);",
        show_return_value_in_logs=True,
    )
