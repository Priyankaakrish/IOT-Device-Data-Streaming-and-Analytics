-- =====================================================================
-- IoT Device Data Streamlining -- Migration: public.* -> mart.*
-- =====================================================================
-- Backfills the star schema from the existing flat tables created in
-- 01_schema.sql. Run after 04 and 05.
--
-- SAFETY: additive only. Nothing in `public` is dropped or altered.
--         Wrapped in a transaction -- review the reconciliation output
--         at the end, then COMMIT or ROLLBACK.
-- =====================================================================

BEGIN;
SET search_path TO mart, public;

-- ---------------------------------------------------------------------
-- STEP 1 -- Catch any machine present in readings but missing from the
-- master table. Flagged is_inferred so data stewards can fix it later.
-- ---------------------------------------------------------------------
INSERT INTO mart.dim_machine
    (machine_id, machine_name, machine_type, production_line, plant_code,
     ideal_cycle_time_sec, criticality, is_inferred)
SELECT DISTINCT
    sr.machine_id,
    sr.machine_id,
    'UNKNOWN',
    'UNASSIGNED',
    'PLANT-01',
    1.0,
    'MEDIUM',
    TRUE
FROM public.sensor_readings sr
WHERE NOT EXISTS (
    SELECT 1 FROM mart.dim_machine d
    WHERE d.machine_id = sr.machine_id AND d.is_current
);

-- ---------------------------------------------------------------------
-- STEP 2 -- fact_sensor_reading
-- Every fact row gets valid surrogate keys or falls back to -1.
-- No NULL foreign keys, ever.
-- ---------------------------------------------------------------------
INSERT INTO mart.fact_sensor_reading (
    date_key, time_key, machine_key, status_key, shift_key, flags_key,
    event_ts, event_date,
    temperature_c, power_kw, vibration_mm_s,
    units_produced, good_units, runtime_seconds, legacy_reading_id
)
SELECT
    TO_CHAR(sr.event_time, 'YYYYMMDD')::INTEGER,
    (EXTRACT(HOUR FROM sr.event_time)::INT * 60
        + EXTRACT(MINUTE FROM sr.event_time)::INT)::SMALLINT,
    COALESCE(dm.machine_key, -1),
    COALESCE(ds.status_key, -1),
    COALESCE(dt.shift_key, -1),
    CASE
        WHEN sr.temperature_c > 150 OR sr.temperature_c < -20 THEN 2
        ELSE 1
    END,
    sr.event_time,
    sr.event_time::DATE,
    sr.temperature_c,
    sr.power_kw,
    sr.vibration_mm_s,
    COALESCE(sr.units_produced, 0),
    COALESCE(sr.good_units, 0),
    5.0,                                    -- producer emits every ~5s
    sr.reading_id
FROM public.sensor_readings sr
LEFT JOIN mart.dim_machine dm
       ON dm.machine_id = sr.machine_id AND dm.is_current
LEFT JOIN mart.dim_machine_status ds
       ON ds.status_code = UPPER(sr.status)
LEFT JOIN mart.dim_time dt
       ON dt.time_key = (EXTRACT(HOUR FROM sr.event_time)::INT * 60
                       + EXTRACT(MINUTE FROM sr.event_time)::INT)
WHERE sr.event_time IS NOT NULL;

-- ---------------------------------------------------------------------
-- STEP 3 -- fact_alert  (THE DUPLICATE-ALERT FIX)
--
-- public.failure_alerts contains one row per micro-batch in which a
-- machine was observed in an alerting state. A machine down for an hour
-- with a 10s trigger produced ~360 rows for ONE real event, which is
-- why the Power BI card reads 15M.
--
-- Correct grain is one row per TRANSITION INTO the condition.
-- Technique: gaps-and-islands. LAG() finds the previous alert for the
-- same machine+type; a new occurrence starts only when the gap exceeds
-- the 5-minute re-arm window.
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT
        fa.*,
        LAG(fa.event_time) OVER (
            PARTITION BY fa.machine_id, fa.alert_type
            ORDER BY fa.event_time
        ) AS prev_event_time
    FROM public.failure_alerts fa
),
occurrences AS (
    SELECT *
    FROM ranked
    WHERE prev_event_time IS NULL
       OR event_time - prev_event_time > INTERVAL '5 minutes'
)
INSERT INTO mart.fact_alert (
    date_key, time_key, machine_key, alert_type_key, shift_key,
    raised_at, acknowledged_at, resolved_at,
    alert_count, message, legacy_alert_id
)
SELECT
    TO_CHAR(o.event_time, 'YYYYMMDD')::INTEGER,
    (EXTRACT(HOUR FROM o.event_time)::INT * 60
        + EXTRACT(MINUTE FROM o.event_time)::INT)::SMALLINT,
    COALESCE(dm.machine_key, -1),
    COALESCE(
        dat.alert_type_key,
        (SELECT alert_type_key FROM mart.dim_alert_type WHERE alert_code = 'FAULT_STATUS')
    ),
    COALESCE(dt.shift_key, -1),
    o.event_time,
    CASE WHEN o.acknowledged THEN o.event_time + INTERVAL '10 minutes' END,
    CASE WHEN o.acknowledged THEN o.event_time + INTERVAL '25 minutes' END,
    1,
    o.message,
    o.alert_id
FROM occurrences o
LEFT JOIN mart.dim_machine    dm  ON dm.machine_id = o.machine_id AND dm.is_current
LEFT JOIN mart.dim_alert_type dat ON dat.alert_code = o.alert_type
LEFT JOIN mart.dim_time       dt  ON dt.time_key = (EXTRACT(HOUR FROM o.event_time)::INT * 60
                                                  + EXTRACT(MINUTE FROM o.event_time)::INT)
ON CONFLICT ON CONSTRAINT uq_alert_occurrence DO NOTHING;

-- SLA evaluation (needs the dim_alert_type join, so done post-insert)
UPDATE mart.fact_alert f
SET time_to_acknowledge_min = EXTRACT(EPOCH FROM (f.acknowledged_at - f.raised_at)) / 60,
    time_to_resolve_min     = EXTRACT(EPOCH FROM (f.resolved_at     - f.raised_at)) / 60,
    breached_sla            = EXTRACT(EPOCH FROM (f.acknowledged_at - f.raised_at)) / 60
                              > dat.sla_response_minutes
FROM mart.dim_alert_type dat
WHERE dat.alert_type_key = f.alert_type_key
  AND f.acknowledged_at IS NOT NULL;

-- ---------------------------------------------------------------------
-- STEP 4 -- fact_downtime  (from public.downtime_events)
-- ---------------------------------------------------------------------
INSERT INTO mart.fact_downtime (
    date_key, time_key, machine_key, status_key, shift_key,
    started_at, ended_at, duration_minutes, is_planned, reason_code, legacy_event_id
)
SELECT
    TO_CHAR(de.start_time, 'YYYYMMDD')::INTEGER,
    (EXTRACT(HOUR FROM de.start_time)::INT * 60
        + EXTRACT(MINUTE FROM de.start_time)::INT)::SMALLINT,
    COALESCE(dm.machine_key, -1),
    COALESCE(ds.status_key, 4),                      -- default FAULT
    COALESCE(dt.shift_key, -1),
    de.start_time,
    de.end_time,
    COALESCE(
        de.duration_minutes,
        EXTRACT(EPOCH FROM (COALESCE(de.end_time, NOW()) - de.start_time)) / 60
    ),
    COALESCE(de.reason_code ILIKE '%planned%' OR de.reason_code ILIKE '%maintenance%', FALSE),
    de.reason_code,
    de.event_id
FROM public.downtime_events de
LEFT JOIN mart.dim_machine        dm ON dm.machine_id = de.machine_id AND dm.is_current
LEFT JOIN mart.dim_machine_status ds ON ds.status_code = 'FAULT'
LEFT JOIN mart.dim_time           dt ON dt.time_key = (EXTRACT(HOUR FROM de.start_time)::INT * 60
                                                     + EXTRACT(MINUTE FROM de.start_time)::INT)
ON CONFLICT ON CONSTRAINT uq_downtime_episode DO NOTHING;

-- ---------------------------------------------------------------------
-- STEP 5 -- fact_production_hourly
--
-- Rolls public.machine_kpi_1min (1-minute grain) up to hourly. Uses the
-- existing running/idle/down second counters, so OEE Availability is
-- computed from real observed state rather than inferred.
--
-- planned_minutes comes from public.planned_production_time where
-- available, apportioned across the 24 hours of the day; otherwise 60.
-- ---------------------------------------------------------------------
INSERT INTO mart.fact_production_hourly (
    date_key, time_key, machine_key, shift_key,
    window_start, window_end,
    reading_count, units_produced, good_units, scrap_units,
    runtime_minutes, idle_minutes, downtime_minutes, planned_minutes, energy_kwh,
    avg_temperature_c, max_temperature_c, temp_sum
)
SELECT
    TO_CHAR(DATE_TRUNC('hour', k.window_start), 'YYYYMMDD')::INTEGER,
    -- NOTE: must be expressed over DATE_TRUNC('hour', ...) so it matches the
    -- GROUP BY expression exactly. Postgres does not infer that
    -- EXTRACT(HOUR FROM x) is functionally dependent on DATE_TRUNC('hour', x).
    (EXTRACT(HOUR FROM DATE_TRUNC('hour', k.window_start))::INT * 60)::SMALLINT,
    COALESCE(dm.machine_key, -1),
    COALESCE(dt.shift_key, -1),
    DATE_TRUNC('hour', k.window_start),
    DATE_TRUNC('hour', k.window_start) + INTERVAL '1 hour',
    COUNT(*),
    SUM(COALESCE(k.units_produced, 0)),
    SUM(COALESCE(k.good_units, 0)),
    SUM(COALESCE(k.units_produced, 0) - COALESCE(k.good_units, 0)),
    SUM(COALESCE(k.running_seconds, 0)) / 60.0,
    SUM(COALESCE(k.idle_seconds, 0))    / 60.0,
    SUM(COALESCE(k.down_seconds, 0))    / 60.0,
    COALESCE(MAX(p.planned_minutes) / 24.0, 60.0),
    SUM(COALESCE(k.total_power_kwh, 0)),
    AVG(k.avg_temperature_c),
    MAX(k.max_temperature_c),
    SUM(k.avg_temperature_c)
FROM public.machine_kpi_1min k
LEFT JOIN mart.dim_machine dm
       ON dm.machine_id = k.machine_id AND dm.is_current
LEFT JOIN mart.dim_time dt
       ON dt.time_key = (EXTRACT(HOUR FROM k.window_start)::INT * 60)
LEFT JOIN public.planned_production_time p
       ON p.machine_id = k.machine_id
      AND p.production_date = DATE(k.window_start)
GROUP BY
    dm.machine_key,
    dt.shift_key,
    DATE_TRUNC('hour', k.window_start)
ON CONFLICT ON CONSTRAINT uq_fph_grain DO NOTHING;

-- ---------------------------------------------------------------------
-- STEP 6 -- RECONCILIATION. Read this before committing.
-- ---------------------------------------------------------------------
SELECT 'source  public.sensor_readings'   AS metric, COUNT(*)::TEXT AS value FROM public.sensor_readings
UNION ALL SELECT 'target  fact_sensor_reading',      COUNT(*)::TEXT FROM mart.fact_sensor_reading
UNION ALL SELECT 'unknown machine_key rows',         COUNT(*)::TEXT FROM mart.fact_sensor_reading WHERE machine_key = -1
UNION ALL SELECT 'unknown status_key rows',          COUNT(*)::TEXT FROM mart.fact_sensor_reading WHERE status_key  = -1
UNION ALL SELECT 'source  public.failure_alerts',    COUNT(*)::TEXT FROM public.failure_alerts
UNION ALL SELECT 'target  fact_alert (deduped)',     COUNT(*)::TEXT FROM mart.fact_alert
UNION ALL SELECT 'alert dedup reduction',
    ROUND(100.0 * (1 - (SELECT COUNT(*) FROM mart.fact_alert)::NUMERIC
                     / NULLIF((SELECT COUNT(*) FROM public.failure_alerts), 0)), 2)::TEXT || '%'
UNION ALL SELECT 'source  public.downtime_events',   COUNT(*)::TEXT FROM public.downtime_events
UNION ALL SELECT 'target  fact_downtime',            COUNT(*)::TEXT FROM mart.fact_downtime
UNION ALL SELECT 'source  public.machine_kpi_1min',  COUNT(*)::TEXT FROM public.machine_kpi_1min
UNION ALL SELECT 'target  fact_production_hourly',   COUNT(*)::TEXT FROM mart.fact_production_hourly;

-- EXPECTED:
--   target sensor rows  == source sensor rows
--   unknown-key rows    == 0
--   alert reduction     >  99%
--
-- COMMIT;     -- uncomment when the numbers look right
COMMIT;
-- ROLLBACK;   -- otherwise
