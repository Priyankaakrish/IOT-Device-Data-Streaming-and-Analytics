"""
Spark Structured Streaming ETL: Kafka -> PostgreSQL
=====================================================
Reads raw IoT telemetry from the `iot-sensor-data` Kafka topic and:

  1. Writes every raw reading to `sensor_readings` (append-only, JDBC).
  2. Computes 1-minute tumbling-window KPI aggregates per machine and
     upserts them into `machine_kpi_1min` (temperature, power, run/idle/
     down seconds, units produced/good -- the building blocks for the
     Power BI KPIs, including OEE).
  3. Detects DOWN/FAULT -> RUNNING/IDLE transitions and opens/closes rows
     in `downtime_events` (drives the Downtime KPI).
  4. Raises rows in `failure_alerts` when temperature/vibration cross
     thresholds or status == FAULT (drives the Failure Alerts KPI).

Run locally:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
      streaming_job.py

Windows prerequisites (see README "Running on Windows"):
  * Python 3.11 -- pyspark 3.5.x does not support 3.12+.
  * winutils.exe + hadoop.dll for Hadoop 3.3.x in %HADOOP_HOME%\bin, otherwise
    SparkContext fails to start as soon as --packages is used:
        java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
"""

import os
from datetime import UTC

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, avg, max as _max, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Config (env-overridable so the same script runs locally or in containers)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "iot-sensor-data")

PG_HOST = os.environ.get("PG_HOST", "localhost")
# docker-compose publishes Postgres on host port 5433 (5433:5432) to avoid
# clashing with a local Postgres install. Inside the compose network use 5432.
PG_PORT = os.environ.get("PG_PORT", "5433")
PG_DB = os.environ.get("PG_DB", "iot_dashboard")
PG_USER = os.environ.get("PG_USER", "iot_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "iot_password")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# Streaming checkpoints. Keep this off "/tmp" so it resolves sanely on Windows
# (where "/tmp/..." lands at C:\tmp\...) and survives restarts.
CHECKPOINT_DIR = os.environ.get(
    "CHECKPOINT_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoints"),
)

# Timezone pinned for the JVM *and* Spark's SQL session.
#
# pgjdbc sends the JVM's default timezone to the server in its startup packet.
# On Windows the JDK still reports legacy Olson aliases (e.g. India Standard
# Time -> "Asia/Calcutta"), which Postgres 16 no longer accepts -- the JDBC
# sink then dies with:
#     FATAL: invalid value for parameter "TimeZone": "Asia/Calcutta"
# Forcing UTC sidesteps the alias problem entirely and keeps event_time
# consistent with the producer, which emits UTC ISO-8601 timestamps.
SPARK_TIMEZONE = os.environ.get("SPARK_TIMEZONE", "UTC")

TEMP_ALERT_THRESHOLD_C = 85.0
VIBRATION_ALERT_THRESHOLD = 5.0

# Driver-side state for downtime detection: {machine_id: {"status": str, "since": datetime}}
# NOTE: this lives in driver memory only -- fine for a single-driver demo job.
# A production version would persist this in Postgres itself (query last
# open downtime_events row) so it survives job restarts.
_machine_state = {}


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def to_utc_naive(dt):
    """Normalise a collect()-ed Spark timestamp to a naive UTC datetime.

    PySpark materialises TimestampType as a naive datetime in the *driver's
    local* timezone. The JDBC sink (sensor_readings) writes UTC, so inserting
    those values as-is via psycopg2 would silently offset machine_kpi_1min,
    downtime_events and failure_alerts by the driver's UTC offset.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.astimezone()  # interpret the naive value as local time
    return dt.astimezone(UTC).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Kafka message schema
# ---------------------------------------------------------------------------
SCHEMA = StructType(
    [
        StructField("machine_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("temperature_c", DoubleType()),
        StructField("power_kw", DoubleType()),
        StructField("vibration_mm_s", DoubleType()),
        StructField("status", StringType()),
        StructField("units_produced", IntegerType()),
        StructField("good_units", IntegerType()),
    ]
)


def write_raw_readings(batch_df, batch_id):
    """Sink 1: append every raw reading to sensor_readings."""
    if batch_df.isEmpty():
        return
    (
        batch_df.select(
            "machine_id",
            "event_time",
            "temperature_c",
            "power_kw",
            "vibration_mm_s",
            "status",
            "units_produced",
            "good_units",
        )
        .write.mode("append")
        .jdbc(JDBC_URL, "sensor_readings", properties=JDBC_PROPS)
    )


def write_kpi_aggregates(batch_df, batch_id):
    """Sink 2: upsert 1-min windowed KPI aggregates into machine_kpi_1min."""
    rows = batch_df.collect()
    if not rows:
        return

    records = [
        (
            r["machine_id"],
            to_utc_naive(r["window_start"]),
            to_utc_naive(r["window_end"]),
            r["avg_temperature_c"],
            r["max_temperature_c"],
            r["avg_power_kw"],
            r["total_power_kwh"],
            r["running_seconds"],
            r["idle_seconds"],
            r["down_seconds"],
            r["units_produced"],
            r["good_units"],
        )
        for r in rows
    ]

    upsert_sql = """
        INSERT INTO machine_kpi_1min (
            machine_id, window_start, window_end, avg_temperature_c, max_temperature_c,
            avg_power_kw, total_power_kwh, running_seconds, idle_seconds, down_seconds,
            units_produced, good_units
        ) VALUES %s
        ON CONFLICT (machine_id, window_start) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            avg_temperature_c = EXCLUDED.avg_temperature_c,
            max_temperature_c = EXCLUDED.max_temperature_c,
            avg_power_kw = EXCLUDED.avg_power_kw,
            total_power_kwh = EXCLUDED.total_power_kwh,
            running_seconds = EXCLUDED.running_seconds,
            idle_seconds = EXCLUDED.idle_seconds,
            down_seconds = EXCLUDED.down_seconds,
            units_produced = EXCLUDED.units_produced,
            good_units = EXCLUDED.good_units;
    """

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, upsert_sql, records)
        conn.commit()
    finally:
        conn.close()


def write_alerts_and_downtime(batch_df, batch_id):
    """Sink 3 & 4: raise failure_alerts and open/close downtime_events.

    Processes rows machine-by-machine, in event_time order, comparing each
    reading against the driver-held last-known status per machine.
    """
    rows = batch_df.orderBy("machine_id", "event_time").collect()
    if not rows:
        return

    alerts = []
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            for r in rows:
                mid = r["machine_id"]
                status = r["status"]
                ts = to_utc_naive(r["event_time"])
                temp = r["temperature_c"]
                vib = r["vibration_mm_s"]

                # --- Threshold / FAULT alerts ---
                if status == "FAULT":
                    alerts.append(
                        (
                            mid,
                            ts,
                            "FAULT_STATUS",
                            "CRITICAL",
                            f"Machine {mid} reported FAULT status",
                        )
                    )
                elif temp is not None and temp >= TEMP_ALERT_THRESHOLD_C:
                    alerts.append(
                        (mid, ts, "HIGH_TEMP", "HIGH", f"Temperature {temp}C exceeds threshold")
                    )
                elif vib is not None and vib >= VIBRATION_ALERT_THRESHOLD:
                    alerts.append(
                        (
                            mid,
                            ts,
                            "HIGH_VIBRATION",
                            "MEDIUM",
                            f"Vibration {vib}mm/s exceeds threshold",
                        )
                    )

                # --- Downtime open/close state machine ---
                prev = _machine_state.get(mid, {"status": "RUNNING", "since": ts})
                is_down_now = status in ("DOWN", "FAULT")
                was_down = prev["status"] in ("DOWN", "FAULT")

                if is_down_now and not was_down:
                    # Downtime started
                    cur.execute(
                        "INSERT INTO downtime_events (machine_id, start_time, reason_code) VALUES (%s, %s, %s)",
                        (mid, ts, status),
                    )
                    _machine_state[mid] = {"status": status, "since": ts}
                elif not is_down_now and was_down:
                    # Downtime ended -> close the most recent open event for this machine
                    cur.execute(
                        """
                        UPDATE downtime_events
                        SET end_time = %s,
                            duration_minutes = ROUND(EXTRACT(EPOCH FROM (%s - start_time)) / 60.0, 2)
                        WHERE event_id = (
                            SELECT event_id FROM downtime_events
                            WHERE machine_id = %s AND end_time IS NULL
                            ORDER BY start_time DESC LIMIT 1
                        )
                        """,
                        (ts, ts, mid),
                    )
                    _machine_state[mid] = {"status": status, "since": ts}
                else:
                    _machine_state[mid] = {"status": status, "since": prev["since"]}

            if alerts:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO failure_alerts (machine_id, event_time, alert_type, severity, message) VALUES %s",
                    alerts,
                )
        conn.commit()
    finally:
        conn.close()


def main():
    spark = (
        SparkSession.builder.appName("IoTDeviceDataStreamlining")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", SPARK_TIMEZONE)
        # Executors run the actual JDBC inserts, so they need the same JVM
        # timezone as the driver (see SPARK_TIMEZONE above).
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={SPARK_TIMEZONE}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # The driver JVM is already running by the time we get here (spark-submit
    # started it), so `-Duser.timezone` can't be applied via SparkConf -- set
    # the default directly before any JDBC connection is opened.
    jvm_tz = spark.sparkContext._jvm.java.util.TimeZone
    jvm_tz.setDefault(jvm_tz.getTimeZone(SPARK_TIMEZONE))

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withWatermark("event_time", "2 minutes")
    )

    # --- Sink 1: raw readings ---
    raw_query = (
        parsed.writeStream.foreachBatch(write_raw_readings)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw_readings"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Sink 2: 1-minute windowed KPI aggregates ---
    kpi_agg = (
        parsed.groupBy(col("machine_id"), window(col("event_time"), "1 minute"))
        .agg(
            avg("temperature_c").alias("avg_temperature_c"),
            _max("temperature_c").alias("max_temperature_c"),
            avg("power_kw").alias("avg_power_kw"),
            (_sum("power_kw") / 60.0).alias(
                "total_power_kwh"
            ),  # crude kWh proxy from per-second kW samples
            _sum(when(col("status") == "RUNNING", 1).otherwise(0)).alias("running_seconds"),
            _sum(when(col("status") == "IDLE", 1).otherwise(0)).alias("idle_seconds"),
            _sum(when(col("status").isin("DOWN", "FAULT"), 1).otherwise(0)).alias("down_seconds"),
            _sum("units_produced").alias("units_produced"),
            _sum("good_units").alias("good_units"),
        )
        .select(
            "machine_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_temperature_c",
            "max_temperature_c",
            "avg_power_kw",
            "total_power_kwh",
            "running_seconds",
            "idle_seconds",
            "down_seconds",
            "units_produced",
            "good_units",
        )
    )

    kpi_query = (
        kpi_agg.writeStream.foreachBatch(write_kpi_aggregates)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "kpi_agg"))
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Sink 3 & 4: alerts + downtime state machine ---
    alerts_query = (
        parsed.writeStream.foreachBatch(write_alerts_and_downtime)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "alerts_downtime"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
