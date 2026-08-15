-- =====================================================================
-- IoT Device Data Streamlining -- Dimension Population
-- Run after 04_star_schema.sql
-- =====================================================================
SET search_path TO mart, public;

-- ---------------------------------------------------------------------
-- dim_date : 2024-01-01 .. 2034-12-31
-- Fiscal year assumed to start 1 April. Change the offsets if yours differs.
-- ---------------------------------------------------------------------
INSERT INTO dim_date (
    date_key, full_date, day_of_week, day_name, day_of_month, day_of_year,
    week_of_year, iso_week, month_number, month_name, month_year,
    quarter_number, quarter_name, year_number,
    fiscal_month, fiscal_quarter, fiscal_year,
    is_weekend, is_working_day, month_start_date, month_end_date
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(ISODOW FROM d),
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(DAY FROM d),
    EXTRACT(DOY FROM d),
    EXTRACT(WEEK FROM d),
    TO_CHAR(d, 'IYYY-"W"IW'),
    EXTRACT(MONTH FROM d),
    TRIM(TO_CHAR(d, 'Month')),
    TO_CHAR(d, 'Mon YYYY'),
    EXTRACT(QUARTER FROM d),
    'Q' || EXTRACT(QUARTER FROM d),
    EXTRACT(YEAR FROM d),
    ((EXTRACT(MONTH FROM d)::INT + 8) % 12) + 1,
    (((EXTRACT(MONTH FROM d)::INT + 8) % 12) / 3) + 1,
    CASE WHEN EXTRACT(MONTH FROM d) >= 4
         THEN EXTRACT(YEAR FROM d) + 1 ELSE EXTRACT(YEAR FROM d) END,
    EXTRACT(ISODOW FROM d) IN (6,7),
    EXTRACT(ISODOW FROM d) NOT IN (6,7),
    DATE_TRUNC('month', d)::DATE,
    (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE
FROM GENERATE_SERIES('2024-01-01'::DATE, '2034-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- ---------------------------------------------------------------------
-- dim_shift : three-shift pattern
-- ---------------------------------------------------------------------
INSERT INTO dim_shift (shift_key, shift_code, shift_name, start_time, end_time,
                       planned_minutes, crosses_midnight)
VALUES
    (1, 'A', 'Morning Shift',   '06:00', '14:00', 480, FALSE),
    (2, 'B', 'Afternoon Shift', '14:00', '22:00', 480, FALSE),
    (3, 'C', 'Night Shift',     '22:00', '06:00', 480, TRUE)
ON CONFLICT (shift_key) DO UPDATE
    SET shift_name = EXCLUDED.shift_name,
        start_time = EXCLUDED.start_time,
        end_time   = EXCLUDED.end_time;

-- ---------------------------------------------------------------------
-- dim_time : 1440 minute rows, shift derived from the boundaries above
-- ---------------------------------------------------------------------
INSERT INTO dim_time (time_key, time_value, hour_24, hour_12, minute_of_hour,
                      am_pm, hour_bucket, shift_key, day_part)
SELECT
    m,
    MAKE_TIME(m / 60, m % 60, 0),
    m / 60,
    CASE WHEN (m/60) % 12 = 0 THEN 12 ELSE (m/60) % 12 END,
    m % 60,
    CASE WHEN m / 60 < 12 THEN 'AM' ELSE 'PM' END,
    LPAD((m/60)::TEXT, 2, '0') || ':00 - ' || LPAD(((m/60 + 1) % 24)::TEXT, 2, '0') || ':00',
    CASE
        WHEN m >= 360  AND m < 840  THEN 1
        WHEN m >= 840  AND m < 1320 THEN 2
        ELSE 3
    END,
    CASE
        WHEN m/60 BETWEEN 5  AND 11 THEN 'Morning'
        WHEN m/60 BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN m/60 BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END
FROM GENERATE_SERIES(0, 1439) AS m
ON CONFLICT (time_key) DO NOTHING;

-- ---------------------------------------------------------------------
-- dim_machine_status
-- Codes 1-4 match the values written by the producer/Spark job into
-- public.sensor_readings.status. 5 and 6 support planned stoppage.
-- ---------------------------------------------------------------------
INSERT INTO dim_machine_status
    (status_key, status_code, status_name, status_category, is_productive,
     is_downtime, is_planned_downtime, counts_toward_oee, display_colour_hex, sort_order)
VALUES
    (1, 'RUNNING',     'Running',             'PRODUCTIVE', TRUE,  FALSE, FALSE, TRUE,  '#2E7D32', 1),
    (2, 'IDLE',        'Idle / Starved',      'UNPLANNED',  FALSE, TRUE,  FALSE, TRUE,  '#F9A825', 2),
    (3, 'DOWN',        'Stopped',             'UNPLANNED',  FALSE, TRUE,  FALSE, TRUE,  '#EF6C00', 3),
    (4, 'FAULT',       'Fault / Breakdown',   'UNPLANNED',  FALSE, TRUE,  FALSE, TRUE,  '#C62828', 4),
    (5, 'MAINTENANCE', 'Planned Maintenance', 'PLANNED',    FALSE, TRUE,  TRUE,  FALSE, '#1565C0', 5),
    (6, 'SETUP',       'Changeover / Setup',  'PLANNED',    FALSE, TRUE,  TRUE,  FALSE, '#6A1B9A', 6)
ON CONFLICT (status_key) DO UPDATE
    SET status_name         = EXCLUDED.status_name,
        is_productive       = EXCLUDED.is_productive,
        is_downtime         = EXCLUDED.is_downtime,
        is_planned_downtime = EXCLUDED.is_planned_downtime;

-- ---------------------------------------------------------------------
-- dim_alert_type
-- The first three codes match public.failure_alerts.alert_type exactly.
-- ---------------------------------------------------------------------
INSERT INTO dim_alert_type
    (alert_code, alert_name, alert_category, severity, severity_rank,
     sla_response_minutes, requires_shutdown, display_colour_hex)
VALUES
    ('FAULT_STATUS',    'Machine Fault Reported',      'MECHANICAL', 'CRITICAL', 1, 15,  TRUE,  '#C62828'),
    ('HIGH_TEMP',       'Temperature Above Threshold', 'THERMAL',    'HIGH',     2, 30,  FALSE, '#EF6C00'),
    ('HIGH_VIBRATION',  'Vibration Above Threshold',   'MECHANICAL', 'HIGH',     2, 30,  FALSE, '#EF6C00'),
    ('POWER_ANOMALY',   'Power Draw Anomaly',          'ELECTRICAL', 'MEDIUM',   3, 60,  FALSE, '#F9A825'),
    ('QUALITY_DRIFT',   'Scrap Rate Elevated',         'PROCESS',    'MEDIUM',   3, 120, FALSE, '#F9A825'),
    ('PREDICTIVE_RISK', 'Predictive Maintenance Flag', 'PROCESS',    'LOW',      4, 480, FALSE, '#1565C0')
ON CONFLICT (alert_code) DO UPDATE
    SET alert_name           = EXCLUDED.alert_name,
        sla_response_minutes = EXCLUDED.sla_response_minutes;

-- ---------------------------------------------------------------------
-- dim_reading_flags : all 8 combinations
-- ---------------------------------------------------------------------
INSERT INTO dim_reading_flags (flags_key, is_outlier, is_backfilled, is_estimated, flag_description)
VALUES
    (1, FALSE, FALSE, FALSE, 'Clean reading'),
    (2, TRUE,  FALSE, FALSE, 'Statistical outlier'),
    (3, FALSE, TRUE,  FALSE, 'Backfilled from late-arriving data'),
    (4, FALSE, FALSE, TRUE,  'Estimated / interpolated'),
    (5, TRUE,  TRUE,  FALSE, 'Outlier, backfilled'),
    (6, TRUE,  FALSE, TRUE,  'Outlier, estimated'),
    (7, FALSE, TRUE,  TRUE,  'Backfilled, estimated'),
    (8, TRUE,  TRUE,  TRUE,  'Outlier, backfilled, estimated')
ON CONFLICT (flags_key) DO NOTHING;

-- ---------------------------------------------------------------------
-- dim_machine : loaded FROM public.machines (the existing master table)
--
-- production_line is derived from public.machines.location. Adjust the
-- CASE if your location values differ -- this is the only place the
-- physical plant layout is encoded.
-- ---------------------------------------------------------------------
INSERT INTO dim_machine (
    machine_id, machine_name, machine_type, location,
    production_line, plant_code, installed_date,
    ideal_cycle_time_sec, criticality, is_active
)
SELECT
    m.machine_id,
    m.machine_name,
    m.machine_type,
    m.location,
    COALESCE(
        CASE
            WHEN m.location ILIKE '%line a%' OR m.location ILIKE '%line-a%' THEN 'LINE-A'
            WHEN m.location ILIKE '%line b%' OR m.location ILIKE '%line-b%' THEN 'LINE-B'
            WHEN m.location ILIKE '%line c%' OR m.location ILIKE '%line-c%' THEN 'LINE-C'
        END,
        -- fall back to splitting the fleet so RLS has something to test against
        CASE WHEN RIGHT(m.machine_id, 1)::INT <= 3 THEN 'LINE-A' ELSE 'LINE-B' END
    ),
    'PLANT-01',
    m.installed_date,
    m.ideal_cycle_time_sec,
    CASE m.machine_type
        WHEN 'CNC'   THEN 'HIGH'
        WHEN 'PRESS' THEN 'HIGH'
        ELSE 'MEDIUM'
    END,
    COALESCE(m.is_active, TRUE)
FROM public.machines m
WHERE NOT EXISTS (
    SELECT 1 FROM mart.dim_machine d
    WHERE d.machine_id = m.machine_id AND d.is_current
);

-- Unknown member
INSERT INTO dim_machine
    (machine_key, machine_id, machine_name, machine_type, production_line,
     plant_code, ideal_cycle_time_sec, criticality, is_inferred)
VALUES (-1, 'UNKNOWN', 'Unknown Machine', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 1.0, 'LOW', TRUE)
ON CONFLICT (machine_key) DO NOTHING;

-- ---------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_time',           COUNT(*) FROM dim_time
UNION ALL SELECT 'dim_shift',          COUNT(*) FROM dim_shift
UNION ALL SELECT 'dim_machine',        COUNT(*) FROM dim_machine
UNION ALL SELECT 'dim_machine_status', COUNT(*) FROM dim_machine_status
UNION ALL SELECT 'dim_alert_type',     COUNT(*) FROM dim_alert_type
UNION ALL SELECT 'dim_reading_flags',  COUNT(*) FROM dim_reading_flags
ORDER BY table_name;
