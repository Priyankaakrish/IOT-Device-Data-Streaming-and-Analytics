-- =====================================================================
-- IoT Device Data Streamlining -- Core Schema (PostgreSQL)
-- =====================================================================
-- Run this once against the target database before starting the
-- Spark Structured Streaming job. The Spark job only INSERTs into
-- these tables; it never creates them.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Machine master data (dimension table)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS machines (
    machine_id            VARCHAR(20)   PRIMARY KEY,
    machine_name          VARCHAR(100)  NOT NULL,
    machine_type          VARCHAR(50)   NOT NULL,      -- e.g. CNC, Conveyor, Pump
    location               VARCHAR(100),
    ideal_cycle_time_sec     NUMERIC(10,2) NOT NULL,      -- used for OEE performance
    installed_date             DATE,
    is_active                    BOOLEAN       DEFAULT TRUE
);

-- ---------------------------------------------------------------------
-- 2. Raw sensor readings (fact table, append-only, written by Spark)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_readings (
    reading_id        BIGSERIAL     PRIMARY KEY,
    machine_id           VARCHAR(20)   NOT NULL REFERENCES machines(machine_id),
    event_time              TIMESTAMP     NOT NULL,      -- time the sensor recorded the value
    ingestion_time             TIMESTAMP     NOT NULL DEFAULT NOW(),
    temperature_c                 NUMERIC(6,2),
    power_kw                        NUMERIC(8,3),
    vibration_mm_s                     NUMERIC(6,3),
    status                                VARCHAR(20)   NOT NULL,  -- RUNNING | IDLE | DOWN | FAULT
    units_produced                          INT           DEFAULT 0,
    good_units                                 INT           DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_machine_time
    ON sensor_readings (machine_id, event_time DESC);

-- ---------------------------------------------------------------------
-- 3. 1-minute windowed KPI aggregates (written by Spark foreachBatch)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS machine_kpi_1min (
    machine_id       VARCHAR(20)   NOT NULL REFERENCES machines(machine_id),
    window_start        TIMESTAMP     NOT NULL,
    window_end             TIMESTAMP     NOT NULL,
    avg_temperature_c         NUMERIC(6,2),
    max_temperature_c            NUMERIC(6,2),
    avg_power_kw                    NUMERIC(8,3),
    total_power_kwh                    NUMERIC(10,3),
    running_seconds                       INT,
    idle_seconds                             INT,
    down_seconds                                INT,
    units_produced                                 INT,
    good_units                                        INT,
    PRIMARY KEY (machine_id, window_start)
);

-- ---------------------------------------------------------------------
-- 4. Downtime events (derived when status flips to DOWN/FAULT and back)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS downtime_events (
    event_id       BIGSERIAL     PRIMARY KEY,
    machine_id        VARCHAR(20)   NOT NULL REFERENCES machines(machine_id),
    start_time           TIMESTAMP     NOT NULL,
    end_time                 TIMESTAMP,
    reason_code                  VARCHAR(50),
    duration_minutes                NUMERIC(10,2)
);

-- ---------------------------------------------------------------------
-- 5. Failure / anomaly alerts (e.g. temp threshold breach, FAULT status)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS failure_alerts (
    alert_id     BIGSERIAL     PRIMARY KEY,
    machine_id      VARCHAR(20)   NOT NULL REFERENCES machines(machine_id),
    event_time         TIMESTAMP     NOT NULL,
    alert_type            VARCHAR(50)   NOT NULL,   -- HIGH_TEMP | HIGH_VIBRATION | FAULT_STATUS
    severity                 VARCHAR(20)   NOT NULL,   -- LOW | MEDIUM | HIGH | CRITICAL
    message                     TEXT,
    acknowledged                   BOOLEAN       DEFAULT FALSE
);

-- ---------------------------------------------------------------------
-- 6. Planned production time (denominator for OEE Availability)
-- One row per machine per shift/day. Populate manually or via a
-- production-scheduling feed; a sample loader is in 03_sample_planned_time.sql
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planned_production_time (
    machine_id      VARCHAR(20)   NOT NULL REFERENCES machines(machine_id),
    production_date    DATE          NOT NULL,
    planned_minutes        INT           NOT NULL,
    PRIMARY KEY (machine_id, production_date)
);
