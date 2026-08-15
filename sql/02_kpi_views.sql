-- =====================================================================
-- IoT Device Data Streamlining -- KPI Views (PostgreSQL)
-- =====================================================================
-- Power BI connects directly to these views. Each view maps to one
-- dashboard KPI. Views are cheap in Postgres (no materialization),
-- so Power BI's scheduled/manual refresh always sees the latest data
-- the Spark job has written.
-- =====================================================================

-- ---------------------------------------------------------------------
-- KPI 1: Live Machine Status
-- Latest known status per machine (last reading in the last 5 min =
-- "live"; otherwise flagged OFFLINE because the sensor stopped sending).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_live_machine_status AS
SELECT DISTINCT ON (sr.machine_id)
    m.machine_id,
    m.machine_name,
    m.machine_type,
    m.location,
    sr.status,
    sr.event_time            AS last_event_time,
    sr.temperature_c,
    sr.power_kw,
    CASE
        WHEN sr.event_time >= NOW() - INTERVAL '5 minutes' THEN 'ONLINE'
        ELSE 'OFFLINE'
    END                        AS connectivity
FROM sensor_readings sr
JOIN machines m ON m.machine_id = sr.machine_id
ORDER BY sr.machine_id, sr.event_time DESC;

-- ---------------------------------------------------------------------
-- KPI 2: Average Temperature (rolling, by machine, last 24h + all-time)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_avg_temperature AS
SELECT
    machine_id,
    AVG(temperature_c) FILTER (WHERE event_time >= NOW() - INTERVAL '1 hour')  AS avg_temp_last_1h,
    AVG(temperature_c) FILTER (WHERE event_time >= NOW() - INTERVAL '24 hours') AS avg_temp_last_24h,
    MAX(temperature_c) FILTER (WHERE event_time >= NOW() - INTERVAL '24 hours') AS max_temp_last_24h
FROM sensor_readings
GROUP BY machine_id;

-- ---------------------------------------------------------------------
-- KPI 3: Failure Alerts (unacknowledged + trend)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_failure_alerts AS
SELECT
    fa.alert_id,
    fa.machine_id,
    m.machine_name,
    fa.event_time,
    fa.alert_type,
    fa.severity,
    fa.message,
    fa.acknowledged
FROM failure_alerts fa
JOIN machines m ON m.machine_id = fa.machine_id
ORDER BY fa.event_time DESC;

-- ---------------------------------------------------------------------
-- KPI 4: Machine Utilization
-- Utilization % = RUNNING time / total observed time, per machine per day
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_machine_utilization AS
SELECT
    machine_id,
    DATE(window_start)                              AS production_date,
    SUM(running_seconds)                               AS running_seconds,
    SUM(idle_seconds)                                     AS idle_seconds,
    SUM(down_seconds)                                        AS down_seconds,
    SUM(running_seconds + idle_seconds + down_seconds)          AS total_seconds,
    ROUND(
        100.0 * SUM(running_seconds) /
        NULLIF(SUM(running_seconds + idle_seconds + down_seconds), 0)
    , 2)                                                            AS utilization_pct
FROM machine_kpi_1min
GROUP BY machine_id, DATE(window_start);

-- ---------------------------------------------------------------------
-- KPI 5: Power Consumption
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_power_consumption AS
SELECT
    machine_id,
    DATE(window_start)              AS production_date,
    SUM(total_power_kwh)               AS total_kwh,
    AVG(avg_power_kw)                     AS avg_kw
FROM machine_kpi_1min
GROUP BY machine_id, DATE(window_start);

-- ---------------------------------------------------------------------
-- KPI 6: Downtime (events + total minutes per machine per day)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_downtime AS
SELECT
    machine_id,
    DATE(start_time)                                   AS production_date,
    COUNT(*)                                               AS downtime_events,
    SUM(COALESCE(duration_minutes, 0))                        AS total_downtime_minutes,
    AVG(COALESCE(duration_minutes, 0))                            AS avg_downtime_minutes
FROM downtime_events
GROUP BY machine_id, DATE(start_time);

-- ---------------------------------------------------------------------
-- KPI 7: Predictive Maintenance signal
-- Simple leading-indicator view: machines trending toward failure based
-- on rising temperature/vibration + recent alert frequency. Swap the
-- threshold logic for a real ML model's output table if you productionize.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_predictive_maintenance AS
WITH recent AS (
    SELECT
        machine_id,
        AVG(temperature_c) FILTER (WHERE event_time >= NOW() - INTERVAL '1 hour')  AS temp_1h,
        AVG(temperature_c) FILTER (WHERE event_time >= NOW() - INTERVAL '6 hours') AS temp_6h,
        AVG(vibration_mm_s) FILTER (WHERE event_time >= NOW() - INTERVAL '1 hour') AS vib_1h,
        AVG(vibration_mm_s) FILTER (WHERE event_time >= NOW() - INTERVAL '6 hours') AS vib_6h
    FROM sensor_readings
    GROUP BY machine_id
),
alert_counts AS (
    SELECT machine_id, COUNT(*) AS alerts_last_24h
    FROM failure_alerts
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY machine_id
)
SELECT
    r.machine_id,
    r.temp_1h,
    r.temp_6h,
    ROUND(r.temp_1h - r.temp_6h, 2)          AS temp_drift,
    r.vib_1h,
    r.vib_6h,
    ROUND(r.vib_1h - r.vib_6h, 2)               AS vib_drift,
    COALESCE(ac.alerts_last_24h, 0)                AS alerts_last_24h,
    CASE
        WHEN COALESCE(ac.alerts_last_24h, 0) >= 3
             OR r.temp_1h - r.temp_6h > 5
             OR r.vib_1h - r.vib_6h > 2
        THEN 'HIGH_RISK'
        WHEN COALESCE(ac.alerts_last_24h, 0) >= 1
             OR r.temp_1h - r.temp_6h > 2
        THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END                                                AS maintenance_risk
FROM recent r
LEFT JOIN alert_counts ac ON ac.machine_id = r.machine_id;

-- ---------------------------------------------------------------------
-- KPI 8: OEE = Availability x Performance x Quality
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_oee AS
WITH daily AS (
    SELECT
        k.machine_id,
        DATE(k.window_start)                          AS production_date,
        SUM(k.running_seconds)                            AS run_seconds,
        SUM(k.units_produced)                                AS units_produced,
        SUM(k.good_units)                                       AS good_units
    FROM machine_kpi_1min k
    GROUP BY k.machine_id, DATE(k.window_start)
)
SELECT
    d.machine_id,
    d.production_date,
    p.planned_minutes,
    ROUND(d.run_seconds / 60.0, 2)                                            AS run_minutes,
    ROUND(100.0 * (d.run_seconds / 60.0) / NULLIF(p.planned_minutes, 0), 2)      AS availability_pct,
    ROUND(
        100.0 * (m.ideal_cycle_time_sec * d.units_produced) /
        NULLIF(d.run_seconds, 0)
    , 2)                                                                            AS performance_pct,
    ROUND(100.0 * d.good_units / NULLIF(d.units_produced, 0), 2)                       AS quality_pct,
    ROUND(
        (
            (d.run_seconds / 60.0) / NULLIF(p.planned_minutes, 0)
        ) *
        (
            (m.ideal_cycle_time_sec * d.units_produced) / NULLIF(d.run_seconds, 0)
        ) *
        (
            d.good_units::NUMERIC / NULLIF(d.units_produced, 0)
        ) * 100
    , 2)                                                                                AS oee_pct
FROM daily d
JOIN machines m ON m.machine_id = d.machine_id
LEFT JOIN planned_production_time p
    ON p.machine_id = d.machine_id AND p.production_date = d.production_date;
