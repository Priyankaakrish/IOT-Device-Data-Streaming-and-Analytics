#!/usr/bin/env python3
"""
Tests for the star-schema streaming job.

Scope is deliberately narrow: these cover the *key derivation* logic, which is
where a silent bug corrupts every downstream fact row. The JDBC writers are not
covered here -- testing those requires a live Postgres and belongs in an
integration suite, not a unit one.

Run:
    py -3.11 -m pytest spark/ -v

Requires a JDK on PATH (same requirement as the job itself). The Spark session
is created once for the whole module; startup dominates the runtime.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)
from streaming_job_star import (
    JDBC_PROPS,
    SENSOR_SCHEMA,
    add_conformed_keys,
    sql_literal,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("iot-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # Driver host must be pinned to loopback. See conftest.py -- on a
        # Windows host running Docker Desktop, Spark otherwise binds to
        # host.docker.internal and the Python worker cannot connect back.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.connectionTimeout", "120000")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class FakeDimensionCache:
    """
    Stand-in for DimensionCache holding two machines and three statuses.

    add_conformed_keys only reads .machine and .status, so this is the full
    surface it needs. Using the real class would require a JDBC connection.
    """

    def __init__(self, spark: SparkSession) -> None:
        machine_schema = StructType(
            [
                StructField("machine_key", LongType(), False),
                StructField("machine_id", StringType(), False),
                StructField("ideal_cycle_time_sec", DoubleType(), True),
                StructField("max_safe_temp_c", DoubleType(), True),
            ]
        )
        self.machine = spark.createDataFrame(
            [
                (101, "MACHINE_001", 3.6, 85.0),
                (102, "MACHINE_002", 2.4, 90.0),
            ],
            schema=machine_schema,
        )

        status_schema = StructType(
            [
                StructField("status_key", ShortType(), False),
                StructField("status_code", StringType(), False),
                StructField("is_productive", BooleanType(), False),
                StructField("is_downtime", BooleanType(), False),
            ]
        )
        self.status = spark.createDataFrame(
            [
                (1, "RUNNING", True, False),
                (2, "IDLE", False, False),
                (3, "FAULT", False, True),
            ],
            schema=status_schema,
        )


def _reading(
    spark: SparkSession,
    machine_id: str = "MACHINE_001",
    event_time: datetime | None = None,
    temperature_c: float = 60.0,
    status: str = "RUNNING",
):
    """One-row sensor DataFrame matching the Kafka payload schema.

    event_time must be timezone-aware. PySpark converts a *naive* datetime to
    an instant using the driver's local timezone (TimestampType.toInternal ->
    time.mktime), while add_conformed_keys derives its keys under
    spark.sql.session.timeZone = UTC. On any non-UTC machine the two disagree
    and every hour-derived key (time_key, shift_key, and date_key near
    midnight) comes out shifted by the local offset. The job itself is not
    affected: the producer emits UTC ISO-8601 and from_json parses it as an
    absolute instant.
    """
    if event_time is not None and event_time.tzinfo is None:
        raise ValueError("event_time must be timezone-aware; use tzinfo=UTC")

    schema = StructType(
        [
            StructField("machine_id", StringType(), True),
            StructField("event_time", SENSOR_SCHEMA["event_time"].dataType, True),
            StructField("temperature_c", DoubleType(), True),
            StructField("power_kw", DoubleType(), True),
            StructField("vibration_mm_s", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("units_produced", IntegerType(), True),
            StructField("good_units", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(
        [
            (
                machine_id,
                event_time or datetime(2026, 8, 10, 9, 30, 0, tzinfo=UTC),
                temperature_c,
                12.5,
                1.2,
                status,
                10,
                10,
            )
        ],
        schema=schema,
    )


# ---------------------------------------------------------------------------
# sql_literal -- guards the etl_batch_log error path
# ---------------------------------------------------------------------------
# process_batch writes the exception text into etl_batch_log. Postgres error
# messages routinely contain single quotes ("column \"x\" doesn't exist"), so an
# unescaped literal turns a logged failure into a second, louder failure.


def test_sql_literal_wraps_in_quotes():
    assert sql_literal("hello") == "'hello'"


def test_sql_literal_escapes_single_quote():
    assert sql_literal("it's") == "'it''s'"


def test_sql_literal_escapes_every_quote():
    assert sql_literal("'a'b'") == "'''a''b'''"


def test_sql_literal_handles_empty_string():
    assert sql_literal("") == "''"


# ---------------------------------------------------------------------------
# Shift assignment
# ---------------------------------------------------------------------------
# Morning 06:00-13:59, Afternoon 14:00-21:59, Night 22:00-05:59.
# The boundaries are the whole risk: an off-by-one here misattributes an entire
# hour of production to the wrong shift, and the 12.6-point OEE spread between
# shifts is a headline finding on Page 2.


@pytest.mark.parametrize(
    "hour,minute,expected_shift",
    [
        (5, 59, 3),  # last minute of night
        (6, 0, 1),  # first minute of morning
        (13, 59, 1),  # last minute of morning
        (14, 0, 2),  # first minute of afternoon
        (21, 59, 2),  # last minute of afternoon
        (22, 0, 3),  # first minute of night
        (0, 0, 3),  # midnight is night
    ],
)
def test_shift_boundaries(spark, hour, minute, expected_shift):
    dims = FakeDimensionCache(spark)
    df = _reading(spark, event_time=datetime(2026, 8, 10, hour, minute, 0, tzinfo=UTC))
    row = add_conformed_keys(df, dims).collect()[0]
    assert row["shift_key"] == expected_shift


# ---------------------------------------------------------------------------
# Date and time keys
# ---------------------------------------------------------------------------


def test_date_key_is_yyyymmdd_integer(spark):
    dims = FakeDimensionCache(spark)
    df = _reading(spark, event_time=datetime(2026, 8, 10, 9, 30, 0, tzinfo=UTC))
    row = add_conformed_keys(df, dims).collect()[0]
    assert row["date_key"] == 20260810


def test_time_key_is_minutes_since_midnight(spark):
    dims = FakeDimensionCache(spark)
    df = _reading(spark, event_time=datetime(2026, 8, 10, 9, 30, 0, tzinfo=UTC))
    row = add_conformed_keys(df, dims).collect()[0]
    assert row["time_key"] == 570  # 9 * 60 + 30


# ---------------------------------------------------------------------------
# Unknown-member handling
# ---------------------------------------------------------------------------
# The star schema reserves -1 as the Unknown member on every dimension. A NULL
# surrogate key would violate the fact table's NOT NULL constraint and kill the
# batch; worse, if it ever got through it would break the join and silently drop
# rows from every measure.


def test_unknown_machine_resolves_to_minus_one(spark):
    dims = FakeDimensionCache(spark)
    df = _reading(spark, machine_id="MACHINE_NOT_IN_DIM")
    row = add_conformed_keys(df, dims).collect()[0]
    assert row["machine_key"] == -1


def test_unknown_status_resolves_to_minus_one(spark):
    dims = FakeDimensionCache(spark)
    df = _reading(spark, status="TELEPORTING")
    row = add_conformed_keys(df, dims).collect()[0]
    assert row["status_key"] == -1


def test_status_lookup_is_case_insensitive(spark):
    dims = FakeDimensionCache(spark)
    row = add_conformed_keys(_reading(spark, status="running"), dims).collect()[0]
    assert row["status_key"] == 1


def test_no_surrogate_key_is_ever_null(spark):
    """The invariant the whole design rests on."""
    dims = FakeDimensionCache(spark)
    df = _reading(spark, machine_id="GHOST", status="ALSO_GHOST")
    row = add_conformed_keys(df, dims).collect()[0]
    for key in ("date_key", "time_key", "machine_key", "status_key", "shift_key", "flags_key"):
        assert row[key] is not None, f"{key} was NULL"


# ---------------------------------------------------------------------------
# Data-quality flags
# ---------------------------------------------------------------------------
# flags_key 1 = clean, 2 = out-of-range. Readings are kept, not dropped, so the
# DQ rate is measurable rather than invisible.


@pytest.mark.parametrize(
    "temperature_c,expected_flag",
    [
        (60.0, 1),  # normal
        (150.0, 1),  # boundary is exclusive
        (150.1, 2),  # sensor fault
        (-20.0, 1),  # boundary is exclusive
        (-20.1, 2),  # sensor fault
    ],
)
def test_flags_key_marks_out_of_range_temperature(spark, temperature_c, expected_flag):
    dims = FakeDimensionCache(spark)
    df = _reading(spark, temperature_c=temperature_c)
    row = add_conformed_keys(df, dims).collect()[0]
    assert row["flags_key"] == expected_flag


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------


def test_sensor_schema_requires_identity_and_time():
    """machine_id and event_time cannot be nullable: both feed surrogate keys."""
    assert SENSOR_SCHEMA["machine_id"].nullable is False
    assert SENSOR_SCHEMA["event_time"].nullable is False


def test_jdbc_keepalive_is_configured():
    """
    Regression guard. The legacy job died with 'Read timed out' every time the
    host slept: the JDBC socket went stale and nothing detected it. Removing
    these three properties reintroduces that failure, and it presents as a
    random crash hours later rather than as a test failure -- hence the test.
    """
    assert JDBC_PROPS["tcpKeepAlive"] == "true"
    assert "connectTimeout" in JDBC_PROPS
    assert "socketTimeout" in JDBC_PROPS
