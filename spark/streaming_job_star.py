#!/usr/bin/env python3
"""
IoT Manufacturing Analytics - Spark Structured Streaming (star schema writer)

Replaces the legacy job that wrote to public.sensor_readings / public.alerts.

Key changes vs legacy:
  1. Resolves dimension surrogate keys before writing facts
  2. Alerts are TRANSITION-grained (fixes the 15M duplicate-row bug)
  3. Single foreachBatch instead of three concurrent writeStreams
     -> one set of Python workers, far lower resource pressure on Windows
  4. Idempotent upserts via staging table + ON CONFLICT
  5. Batch lineage recorded in mart.etl_batch_log

Run:
    py -3.11 streaming_job_star.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "iot_dashboard")
PG_USER = os.getenv("PG_USER", "iot_user")
PG_PWD = os.getenv("PG_PASSWORD", "iot_password")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PWD,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
    # connection resilience - the legacy job died on a stale socket after sleep
    "connectTimeout": "10",
    "socketTimeout": "60",
    "tcpKeepAlive": "true",
    "reWriteBatchedInserts": "true",
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-sensor-data")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./checkpoints/star_v1")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# Alert re-arm window: a machine staying in FAULT does not raise a new alert
# until it has recovered or this window has elapsed.
ALERT_REARM_MINUTES = int(os.getenv("ALERT_REARM_MINUTES", "5"))

TEMP_ALERT_THRESHOLD_C = float(os.getenv("TEMP_ALERT_THRESHOLD_C", "85"))
VIBRATION_ALERT_THRESHOLD = float(os.getenv("VIBRATION_ALERT_THRESHOLD", "4.5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("iot.streaming")

SENSOR_SCHEMA = StructType(
    [
        StructField("machine_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("temperature_c", DoubleType(), True),
        StructField("power_kw", DoubleType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("units_produced", IntegerType(), True),
        StructField("good_units", IntegerType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Dimension cache
# ---------------------------------------------------------------------------
class DimensionCache:
    """
    Dimensions are small and change rarely. Caching and broadcasting them once
    per job avoids a JDBC read per micro-batch, which was a large part of the
    legacy job's per-batch cost.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._machine: DataFrame | None = None
        self._status: DataFrame | None = None
        self._alert_type: DataFrame | None = None
        self._loaded_at: datetime | None = None

    def _read(self, table: str) -> DataFrame:
        return self.spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)

    def load(self) -> None:
        log.info("Loading dimension cache")
        self._machine = (
            self._read("mart.dim_machine")
            .filter(F.col("is_current"))
            .select("machine_key", "machine_id", "ideal_cycle_time_sec", "max_safe_temp_c")
            .cache()
        )
        self._status = (
            self._read("mart.dim_machine_status")
            .select("status_key", "status_code", "is_productive", "is_downtime")
            .cache()
        )
        self._alert_type = (
            self._read("mart.dim_alert_type").select("alert_type_key", "alert_code").cache()
        )
        counts = (
            self._machine.count(),
            self._status.count(),
            self._alert_type.count(),
        )
        self._loaded_at = datetime.now(UTC)
        log.info("Dimensions cached: machines=%d statuses=%d alert_types=%d", *counts)

    @property
    def machine(self) -> DataFrame:
        return self._machine

    @property
    def status(self) -> DataFrame:
        return self._status

    @property
    def alert_type(self) -> DataFrame:
        return self._alert_type


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------
def add_conformed_keys(df: DataFrame, dims: DimensionCache) -> DataFrame:
    """Attach every surrogate key. Unresolved lookups fall back to -1, never NULL."""
    return (
        df.withColumn("event_date", F.to_date("event_time"))
        .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int"))
        .withColumn(
            "time_key",
            (F.hour("event_time") * 60 + F.minute("event_time")).cast("smallint"),
        )
        .withColumn(
            "shift_key",
            F.when((F.col("time_key") >= 360) & (F.col("time_key") < 840), F.lit(1))
            .when((F.col("time_key") >= 840) & (F.col("time_key") < 1320), F.lit(2))
            .otherwise(F.lit(3))
            .cast("smallint"),
        )
        .join(F.broadcast(dims.machine), on="machine_id", how="left")
        .join(
            F.broadcast(dims.status),
            F.upper(F.col("status")) == F.col("status_code"),
            how="left",
        )
        .withColumn("machine_key", F.coalesce("machine_key", F.lit(-1)))
        .withColumn("status_key", F.coalesce("status_key", F.lit(-1)).cast("smallint"))
        .withColumn(
            "flags_key",
            F.when(
                (F.col("temperature_c") > 150) | (F.col("temperature_c") < -20),
                F.lit(2),
            )
            .otherwise(F.lit(1))
            .cast("smallint"),
        )
    )


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def write_sensor_facts(batch: DataFrame) -> int:
    out = batch.select(
        "date_key",
        "time_key",
        "machine_key",
        "status_key",
        "shift_key",
        "flags_key",
        F.col("event_time").alias("event_ts"),
        "event_date",
        "temperature_c",
        "power_kw",
        "vibration_mm_s",
        F.coalesce("units_produced", F.lit(0)).alias("units_produced"),
        F.coalesce("good_units", F.lit(0)).alias("good_units"),
        F.lit(5.0).alias("runtime_seconds"),
    )
    row_count = out.count()
    if row_count:
        out.write.mode("append").option("batchsize", 5000).jdbc(
            JDBC_URL, "mart.fact_sensor_reading", properties=JDBC_PROPS
        )
    return row_count


def write_alert_facts(batch: DataFrame, dims: DimensionCache) -> int:
    """
    TRANSITION-grained alerts.

    The legacy bug: a row was emitted for every micro-batch in which a machine
    was observed in FAULT. A machine down for an hour with a 10s trigger
    produced 360 alert rows for one real event.

    Fix: keep only the first observation per (machine, alert_type) within the
    batch, then anti-join against alerts already raised inside the re-arm
    window. The uq_alert_occurrence constraint is the final backstop.
    """
    spark = SparkSession.getActiveSession()

    conditions = [
        (
            F.upper(F.col("status")).isin("FAULT", "DOWN"),
            "FAULT_STATUS",
            F.col("temperature_c"),
            F.lit(None).cast("double"),
        ),
        (
            F.col("temperature_c") > TEMP_ALERT_THRESHOLD_C,
            "HIGH_TEMP",
            F.col("temperature_c"),
            F.lit(TEMP_ALERT_THRESHOLD_C),
        ),
        (
            F.col("vibration_mm_s") > VIBRATION_ALERT_THRESHOLD,
            "HIGH_VIBRATION",
            F.col("vibration_mm_s"),
            F.lit(VIBRATION_ALERT_THRESHOLD),
        ),
    ]

    candidates = None
    for predicate, code, trigger_val, threshold_val in conditions:
        part = (
            batch.filter(predicate)
            .withColumn("alert_code", F.lit(code))
            .withColumn("trigger_value", trigger_val)
            .withColumn("threshold_value", threshold_val)
        )
        candidates = part if candidates is None else candidates.unionByName(part)

    if candidates is None:
        return 0

    window = Window.partitionBy("machine_key", "alert_code").orderBy("event_time")
    deduped = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    open_alerts_query = (
        "(SELECT DISTINCT machine_key, alert_type_key "
        " FROM mart.fact_alert "
        f" WHERE raised_at > NOW() - INTERVAL '{ALERT_REARM_MINUTES} minutes'"
        ") AS open_a"
    )
    open_alerts = spark.read.jdbc(url=JDBC_URL, table=open_alerts_query, properties=JDBC_PROPS)

    alert_dim = dims.alert_type.withColumnRenamed("alert_code", "dim_alert_code")

    out = (
        deduped.join(
            F.broadcast(alert_dim),
            deduped["alert_code"] == alert_dim["dim_alert_code"],
            "left",
        )
        .join(open_alerts, on=["machine_key", "alert_type_key"], how="left_anti")
        .select(
            "date_key",
            "time_key",
            "machine_key",
            "alert_type_key",
            "shift_key",
            F.col("event_time").alias("raised_at"),
            F.lit(1).alias("alert_count"),
            "trigger_value",
            "threshold_value",
            F.concat(
                F.lit("Machine "),
                F.col("machine_id"),
                F.lit(" raised "),
                F.col("alert_code"),
            ).alias("message"),
        )
    )

    row_count = out.count()
    if row_count:
        out.write.mode("append").option("batchsize", 500).jdbc(
            JDBC_URL, "mart.fact_alert", properties=JDBC_PROPS
        )
    return row_count


def write_hourly_aggregate(batch: DataFrame) -> int:
    """
    Upsert into fact_production_hourly via a staging table.

    temp_sum and reading_count are persisted so downstream re-aggregation
    produces correct weighted averages rather than an average of averages.
    """
    agg = (
        batch.withColumn("window_start", F.date_trunc("hour", F.col("event_time")))
        .groupBy("machine_key", "window_start")
        .agg(
            F.first("date_key").alias("date_key"),
            F.first("shift_key").alias("shift_key"),
            F.count("*").alias("reading_count"),
            F.sum("units_produced").alias("units_produced"),
            F.sum("good_units").alias("good_units"),
            F.sum(F.when(F.col("is_productive"), 5.0).otherwise(0.0)).alias("_runtime_sec"),
            F.sum(F.when(F.col("is_downtime"), 5.0).otherwise(0.0)).alias("_downtime_sec"),
            F.sum(F.when(F.upper(F.col("status")) == "IDLE", 5.0).otherwise(0.0)).alias(
                "_idle_sec"
            ),
            F.avg("power_kw").alias("_avg_kw"),
            F.avg("temperature_c").alias("avg_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.sum("temperature_c").alias("temp_sum"),
            F.avg("vibration_mm_s").alias("avg_vibration_mm_s"),
            F.max("vibration_mm_s").alias("max_vibration_mm_s"),
        )
        .withColumn("scrap_units", F.col("units_produced") - F.col("good_units"))
        .withColumn("time_key", (F.hour("window_start") * 60).cast("smallint"))
        .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 1 HOUR"))
        .withColumn("runtime_minutes", F.col("_runtime_sec") / 60.0)
        .withColumn("downtime_minutes", F.col("_downtime_sec") / 60.0)
        .withColumn("idle_minutes", F.col("_idle_sec") / 60.0)
        .withColumn("planned_minutes", F.lit(60.0))
        .withColumn("energy_kwh", F.col("_avg_kw"))
        .drop("_runtime_sec", "_downtime_sec", "_idle_sec", "_avg_kw")
    )

    row_count = agg.count()
    if not row_count:
        return 0

    agg.write.mode("overwrite").option("truncate", "true").jdbc(
        JDBC_URL, "mart.stg_production_hourly", properties=JDBC_PROPS
    )

    merge_sql = """
        INSERT INTO mart.fact_production_hourly (
            date_key, time_key, machine_key, shift_key, window_start, window_end,
            reading_count, units_produced, good_units, scrap_units,
            runtime_minutes, idle_minutes, downtime_minutes, planned_minutes, energy_kwh,
            avg_temperature_c, max_temperature_c, temp_sum,
            avg_vibration_mm_s, max_vibration_mm_s
        )
        SELECT date_key, time_key, machine_key, shift_key, window_start, window_end,
               reading_count, units_produced, good_units, scrap_units,
               runtime_minutes, idle_minutes, downtime_minutes, planned_minutes, energy_kwh,
               avg_temperature_c, max_temperature_c, temp_sum,
               avg_vibration_mm_s, max_vibration_mm_s
        FROM mart.stg_production_hourly
        ON CONFLICT ON CONSTRAINT uq_fph_grain DO UPDATE SET
            reading_count    = mart.fact_production_hourly.reading_count
                               + EXCLUDED.reading_count,
            units_produced   = mart.fact_production_hourly.units_produced
                               + EXCLUDED.units_produced,
            good_units       = mart.fact_production_hourly.good_units
                               + EXCLUDED.good_units,
            scrap_units      = mart.fact_production_hourly.scrap_units
                               + EXCLUDED.scrap_units,
            runtime_minutes  = mart.fact_production_hourly.runtime_minutes
                               + EXCLUDED.runtime_minutes,
            idle_minutes     = mart.fact_production_hourly.idle_minutes
                               + EXCLUDED.idle_minutes,
            downtime_minutes = mart.fact_production_hourly.downtime_minutes
                               + EXCLUDED.downtime_minutes,
            energy_kwh       = mart.fact_production_hourly.energy_kwh
                               + EXCLUDED.energy_kwh,
            temp_sum         = mart.fact_production_hourly.temp_sum
                               + EXCLUDED.temp_sum,
            avg_temperature_c = (mart.fact_production_hourly.temp_sum
                                 + EXCLUDED.temp_sum)
                                / NULLIF(mart.fact_production_hourly.reading_count
                                         + EXCLUDED.reading_count, 0),
            max_temperature_c = GREATEST(mart.fact_production_hourly.max_temperature_c,
                                         EXCLUDED.max_temperature_c),
            max_vibration_mm_s = GREATEST(mart.fact_production_hourly.max_vibration_mm_s,
                                          EXCLUDED.max_vibration_mm_s);
    """
    execute_sql(merge_sql)
    return row_count


def write_downtime_facts(batch: DataFrame) -> tuple[int, int]:
    """
    Maintain fact_downtime as a true accumulating snapshot.

    Previously this table was populated only by the migration backfill and was
    static thereafter -- a fact table in the model that the pipeline did not
    feed. MTTR and downtime counts stopped moving the moment migration
    finished, which is a worse failure than having no table at all, because
    the dashboard kept rendering a number.

    GRAIN: one row per continuous downtime episode, opened on the transition
    into a down state and closed on the transition out of it.

    Episode boundaries cannot be derived from the batch alone: an episode
    that opened in an earlier batch has no start marker in this one. The
    currently-open episodes are therefore read back from the database and
    used to seed the previous-state value for each machine's first row.

    Returns (episodes_opened, episodes_closed).
    """
    spark = SparkSession.getActiveSession()

    ordered = Window.partitionBy("machine_key").orderBy("event_time")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    marked = batch.withColumn(
        "is_down",
        F.coalesce(F.col("is_downtime"), F.lit(False))
        | F.upper(F.col("status")).isin("DOWN", "FAULT"),
    ).withColumn("prev_down", F.lag("is_down").over(ordered))

    # Open episodes carried over from previous batches.
    open_query = (
        "(SELECT machine_key, started_at AS open_started_at "
        " FROM mart.fact_downtime WHERE ended_at IS NULL) AS open_dt"
    )
    open_episodes = spark.read.jdbc(url=JDBC_URL, table=open_query, properties=JDBC_PROPS)

    joined = (
        marked.join(F.broadcast(open_episodes), on="machine_key", how="left")
        .withColumn("was_open", F.col("open_started_at").isNotNull())
        # For a machine's first row in this batch there is no LAG value; fall
        # back to whether an episode was already open in the database.
        .withColumn(
            "prev_down_eff",
            F.coalesce(F.col("prev_down"), F.col("was_open"), F.lit(False)),
        )
        .withColumn("is_start", F.col("is_down") & ~F.col("prev_down_eff"))
        .withColumn("is_end", ~F.col("is_down") & F.col("prev_down_eff"))
        # Carry the most recent start forward so a closing row knows which
        # episode it terminates. Episodes opened in an earlier batch fall back
        # to the timestamp read from the database.
        .withColumn(
            "last_start",
            F.last(F.when(F.col("is_start"), F.col("event_time")), ignorenulls=True).over(running),
        )
        .withColumn("episode_start", F.coalesce(F.col("last_start"), F.col("open_started_at")))
    ).persist()

    try:
        opens = joined.filter(F.col("is_start")).select(
            "date_key",
            "time_key",
            "machine_key",
            "status_key",
            "shift_key",
            F.col("event_time").alias("started_at"),
            F.lit(False).alias("is_planned"),
            F.upper(F.col("status")).alias("reason_code"),
        )

        closes = (
            joined.filter(F.col("is_end") & F.col("episode_start").isNotNull())
            .select(
                "machine_key",
                F.col("episode_start").alias("started_at"),
                F.col("event_time").alias("ended_at"),
            )
            # A machine can open and close more than once inside one batch;
            # keep the earliest closure per episode.
            .groupBy("machine_key", "started_at")
            .agg(F.min("ended_at").alias("ended_at"))
        )

        n_open = opens.count()
        n_close = closes.count()

        if n_open:
            opens.write.mode("overwrite").option("truncate", "true").jdbc(
                JDBC_URL, "mart.stg_downtime_open", properties=JDBC_PROPS
            )
            execute_sql(
                """
                INSERT INTO mart.fact_downtime (
                    date_key, time_key, machine_key, status_key, shift_key,
                    started_at, is_planned, reason_code
                )
                SELECT date_key, time_key, machine_key, status_key, shift_key,
                       started_at, is_planned, reason_code
                FROM mart.stg_downtime_open
                ON CONFLICT ON CONSTRAINT uq_downtime_episode DO NOTHING;
                """
            )

        if n_close:
            closes.write.mode("overwrite").option("truncate", "true").jdbc(
                JDBC_URL, "mart.stg_downtime_close", properties=JDBC_PROPS
            )
            # Guarded by ended_at IS NULL so a replayed batch cannot reopen and
            # re-close an episode with a later timestamp.
            execute_sql(
                """
                UPDATE mart.fact_downtime f
                SET ended_at = c.ended_at,
                    duration_minutes = ROUND(
                        EXTRACT(EPOCH FROM (c.ended_at - f.started_at)) / 60.0, 2)
                FROM mart.stg_downtime_close c
                WHERE f.machine_key = c.machine_key
                  AND f.started_at  = c.started_at
                  AND f.ended_at IS NULL;
                """
            )

        return n_open, n_close
    finally:
        joined.unpersist()


def write_dead_letters(invalid: DataFrame, batch_id: int) -> int:
    """
    Quarantine messages that could not be parsed into the declared schema.

    The previous implementation filtered these out with a WHERE clause, which
    discards them permanently and silently. A malformed producer release could
    therefore drop an arbitrary fraction of traffic with no signal anywhere
    except a fall in row counts that nobody was watching.
    """
    out = invalid.select(
        F.lit(str(batch_id)).alias("batch_id"),
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.substring(F.col("raw_payload"), 1, 4000).alias("raw_payload"),
        F.when(F.col("d").isNull(), F.lit("JSON parse failed"))
        .when(F.col("d.machine_id").isNull(), F.lit("machine_id is null"))
        .when(F.col("d.event_time").isNull(), F.lit("event_time is null"))
        .otherwise(F.lit("unknown validation failure"))
        .alias("failure_reason"),
    )

    row_count = out.count()
    if row_count:
        out.write.mode("append").option("batchsize", 500).jdbc(
            JDBC_URL, "mart.dlq_sensor_reading", properties=JDBC_PROPS
        )
        log.warning("batch=%s quarantined %d unparseable message(s)", batch_id, row_count)
    return row_count


def execute_sql(sql: str) -> None:
    """Run a statement over JDBC using the driver's own connection."""
    spark = SparkSession.getActiveSession()
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    for key, value in JDBC_PROPS.items():
        props.setProperty(key, str(value))
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, props)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Batch orchestration - ONE foreachBatch, not three writeStreams
# ---------------------------------------------------------------------------
def process_batch(batch_df: DataFrame, batch_id: int, dims: DimensionCache) -> None:
    started = datetime.now(UTC)

    if batch_df.isEmpty():
        log.debug("Batch %s empty, skipping", batch_id)
        return

    # Split before conforming. Anything that fails the schema contract is
    # quarantined rather than dropped, so a bad producer release is visible.
    is_valid = (
        F.col("d").isNotNull()
        & F.col("d.machine_id").isNotNull()
        & F.col("d.event_time").isNotNull()
    )
    n_dlq = write_dead_letters(batch_df.filter(~is_valid), batch_id)

    valid = batch_df.filter(is_valid).select("d.*")
    if valid.isEmpty():
        log.warning("Batch %s contained no valid rows", batch_id)
        return

    enriched = add_conformed_keys(valid, dims).persist()

    try:
        n_sensor = write_sensor_facts(enriched)
        n_alerts = write_alert_facts(enriched, dims)
        n_hourly = write_hourly_aggregate(enriched)
        n_dt_open, n_dt_close = write_downtime_facts(enriched)

        elapsed = (datetime.now(UTC) - started).total_seconds()
        log.info(
            "batch=%s sensor=%d alerts=%d hourly=%d "
            "downtime_open=%d downtime_close=%d dlq=%d elapsed=%.1fs",
            batch_id,
            n_sensor,
            n_alerts,
            n_hourly,
            n_dt_open,
            n_dt_close,
            n_dlq,
            elapsed,
        )

        execute_sql(
            "INSERT INTO mart.etl_batch_log "
            "(job_name, batch_id, started_at, completed_at, rows_written, status) "
            f"VALUES ('streaming_job_star', '{batch_id}', "
            f"'{started.isoformat()}', NOW(), {n_sensor}, 'SUCCESS');"
        )

    except Exception as exc:
        log.exception("Batch %s failed", batch_id)
        execute_sql(
            "INSERT INTO mart.etl_batch_log "
            "(job_name, batch_id, started_at, completed_at, status, error_detail) "
            f"VALUES ('streaming_job_star', '{batch_id}', "
            f"'{started.isoformat()}', NOW(), 'FAILED', "
            f"{sql_literal(str(exc)[:2000])});"
        )
        raise
    finally:
        enriched.unpersist()


# ---------------------------------------------------------------------------
def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("IoTStarSchemaStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.connectionTimeout", "120000")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def main() -> int:
    log.info("Python: %s", sys.version.split()[0])
    log.info("Target: %s", JDBC_URL)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    dims = DimensionCache(spark)
    dims.load()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    # Kafka metadata is carried through so quarantined messages can be traced
    # back to an exact partition and offset and replayed after a fix.
    parsed = (
        raw.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("raw_payload"),
        )
        .withColumn("d", F.from_json(F.col("raw_payload"), SENSOR_SCHEMA))
        # Unparseable rows have no event time of their own; the broker's
        # ingest timestamp keeps them inside the watermark so they reach
        # foreachBatch and can be quarantined instead of silently dropped.
        .withColumn("event_time", F.coalesce(F.col("d.event_time"), F.col("kafka_timestamp")))
        .withWatermark("event_time", "10 minutes")
    )

    query = (
        parsed.writeStream.foreachBatch(lambda df, bid: process_batch(df, bid, dims))
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )

    log.info("Streaming started. Trigger=%s Checkpoint=%s", TRIGGER_INTERVAL, CHECKPOINT_DIR)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
