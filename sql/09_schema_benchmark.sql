-- =====================================================================
-- Star vs Snowflake -- Benchmark
-- =====================================================================
-- Runs equivalent business questions against both schemas and reports
-- planning time, execution time and join count.
--
-- Run after 08_snowflake_schema.sql.
--
-- Usage:
--   Get-Content sql\09_schema_benchmark.sql | docker exec -i iot-postgres `
--       psql -U iot_user -d iot_dashboard
--
-- Caveat, stated honestly: at this data volume (a few hundred fact rows,
-- six machines) BOTH schemas are fast and differences are in the noise.
-- The benchmark demonstrates the SHAPE of the difference -- join count
-- and plan complexity -- which is what scales. Do not present a 2 ms
-- delta as a performance finding.
-- =====================================================================

SET search_path TO mart, snow, public;
\timing on

\echo ''
\echo '======================================================================'
\echo 'Q1: OEE components by machine type and production line'
\echo '======================================================================'

\echo ''
\echo '--- STAR: 1 join from fact to dimension ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT
    m.machine_type,
    m.production_line,
    SUM(f.units_produced)   AS units,
    SUM(f.good_units)       AS good,
    SUM(f.runtime_minutes)  AS runtime_min
FROM mart.fact_production_hourly f
JOIN mart.dim_machine m ON m.machine_key = f.machine_key
GROUP BY m.machine_type, m.production_line;

\echo ''
\echo '--- SNOWFLAKE: 3 joins for the same attributes ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT
    mt.machine_type_code    AS machine_type,
    pl.production_line_code AS production_line,
    SUM(f.units_produced)   AS units,
    SUM(f.good_units)       AS good,
    SUM(f.runtime_minutes)  AS runtime_min
FROM snow.fact_production_hourly f
JOIN snow.dim_machine          m  ON m.machine_key = f.machine_key
JOIN snow.dim_machine_type     mt ON mt.machine_type_key = m.machine_type_key
JOIN snow.dim_production_line  pl ON pl.production_line_key = m.production_line_key
GROUP BY mt.machine_type_code, pl.production_line_code;

\echo ''
\echo '======================================================================'
\echo 'Q2: Alert counts by severity and owning team'
\echo '======================================================================'

\echo ''
\echo '--- STAR: 1 join ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT a.severity, a.alert_category, SUM(f.alert_count) AS alerts
FROM mart.fact_alert f
JOIN mart.dim_alert_type a ON a.alert_type_key = f.alert_type_key
GROUP BY a.severity, a.alert_category;

\echo ''
\echo '--- SNOWFLAKE: 3 joins ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT sv.severity_code, ac.alert_category_code, ac.owning_team, SUM(f.alert_count) AS alerts
FROM snow.fact_alert f
JOIN snow.dim_alert_type     a  ON a.alert_type_key = f.alert_type_key
JOIN snow.dim_severity       sv ON sv.severity_key = a.severity_key
JOIN snow.dim_alert_category ac ON ac.alert_category_key = a.alert_category_key
GROUP BY sv.severity_code, ac.alert_category_code, ac.owning_team;

\echo ''
\echo '======================================================================'
\echo 'Q3: Full plant rollup -- the deepest hierarchy traversal'
\echo '======================================================================'

\echo ''
\echo '--- STAR: 1 join (plant_code lives on dim_machine) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT m.plant_code, SUM(f.energy_kwh) AS kwh, SUM(f.units_produced) AS units
FROM mart.fact_production_hourly f
JOIN mart.dim_machine m ON m.machine_key = f.machine_key
GROUP BY m.plant_code;

\echo ''
\echo '--- SNOWFLAKE: 3 joins to reach plant ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT p.plant_code, SUM(f.energy_kwh) AS kwh, SUM(f.units_produced) AS units
FROM snow.fact_production_hourly f
JOIN snow.dim_machine         m  ON m.machine_key = f.machine_key
JOIN snow.dim_production_line pl ON pl.production_line_key = m.production_line_key
JOIN snow.dim_plant           p  ON p.plant_key = pl.plant_key
GROUP BY p.plant_code;

\echo ''
\echo '======================================================================'
\echo 'Storage comparison -- DIMENSIONS ONLY'
\echo ''
\echo 'Restricted to dimension tables deliberately. Comparing whole schemas'
\echo 'would be meaningless: mart also holds fact_sensor_reading (200k+ rows'
\echo 'across monthly partitions) which snow does not replicate. Dimensions'
\echo 'are where normalisation actually changes the storage picture.'
\echo '======================================================================'
-- Compare ONLY the dimensions whose design actually differs.
-- mart.dim_date and mart.dim_time are SHARED by both schemas (snow's facts
-- reference them directly), so including them would compare 5,452 rows
-- against 31 and tell us nothing about normalisation.
SELECT
    'mart (star)'                                      AS design,
    COUNT(*)                                           AS tables,
    PG_SIZE_PRETTY(SUM(PG_TOTAL_RELATION_SIZE(c.oid))) AS storage
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'mart' AND c.relkind = 'r'
  AND c.relname IN ('dim_machine','dim_alert_type')
UNION ALL
SELECT
    'snow (normalised)',
    COUNT(*),
    PG_SIZE_PRETTY(SUM(PG_TOTAL_RELATION_SIZE(c.oid)))
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'snow' AND c.relkind = 'r'
  AND c.relname IN ('dim_machine','dim_machine_type','dim_manufacturer',
                    'dim_production_line','dim_plant',
                    'dim_alert_type','dim_alert_category','dim_severity');

\echo ''
\echo 'Two tables become eight to hold the same information. Expect snow to'
\echo 'use MORE storage, not less: each table carries page headers, a primary'
\echo 'key index and catalog entries, and that fixed overhead dwarfs any'
\echo 'saving from de-duplicating a handful of short text values.'
\echo ''
\echo 'Normalisation only pays for itself when the repeated attribute is'
\echo 'large AND the dimension has many rows. Neither holds here.'

\echo ''
\echo '======================================================================'
\echo 'Result equivalence -- CLOSED HOURS ONLY'
\echo ''
\echo 'Restricting to production_hour_id alone is NOT sufficient. The'
\echo 'streaming job upserts with'
\echo '    ON CONFLICT DO UPDATE SET units_produced = existing + EXCLUDED'
\echo 'so the CURRENT hour keeps accumulating: row count stays flat while'
\echo 'values climb. A row-count drift check misses this entirely.'
\echo ''
\echo 'The fix is to compare only hours that have closed and can no longer'
\echo 'change.'
\echo '======================================================================'
WITH closed AS (
    SELECT production_hour_id
    FROM snow.fact_production_hourly
    WHERE window_end <= DATE_TRUNC('hour', NOW())
),
star AS (
    SELECT SUM(units_produced) u, SUM(good_units) g, ROUND(SUM(energy_kwh),2) e
    FROM mart.fact_production_hourly
    WHERE production_hour_id IN (SELECT production_hour_id FROM closed)
),
snowf AS (
    SELECT SUM(units_produced) u, SUM(good_units) g, ROUND(SUM(energy_kwh),2) e
    FROM snow.fact_production_hourly
    WHERE production_hour_id IN (SELECT production_hour_id FROM closed)
)
SELECT
    star.u AS star_units,  snowf.u AS snow_units,
    star.g AS star_good,   snowf.g AS snow_good,
    star.e AS star_kwh,    snowf.e AS snow_kwh,
    CASE WHEN star.u = snowf.u AND star.g = snowf.g AND star.e = snowf.e
         THEN 'MATCH -- like-for-like on closed hours'
         ELSE 'MISMATCH -- the copy in 08 is wrong, investigate'
    END AS verdict
FROM star, snowf;

\echo ''
\echo 'Drift detail -- rows added AND values changed since the 08 snapshot'
SELECT
    (SELECT COUNT(*) FROM mart.fact_production_hourly)
      - (SELECT COUNT(*) FROM snow.fact_production_hourly)  AS rows_added,
    (SELECT COUNT(*)
     FROM mart.fact_production_hourly m
     JOIN snow.fact_production_hourly s USING (production_hour_id)
     WHERE m.units_produced IS DISTINCT FROM s.units_produced)  AS rows_value_changed,
    (SELECT COUNT(*) FROM snow.fact_production_hourly
     WHERE window_end > DATE_TRUNC('hour', NOW()))              AS still_open_hours;

\timing off
