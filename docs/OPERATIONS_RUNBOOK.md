# Operations Runbook

## Service level objectives

| SLO | Target | Measurement |
|---|---|---|
| Pipeline freshness | Data < 5 min old, 99% of the time | `MAX(event_ts)` vs `NOW()` |
| Streaming job uptime | 99.5% monthly | `etl_batch_log` gaps |
| Dashboard page load | < 2 s p95 | Power BI usage metrics |
| Model refresh success | 99% | Refresh history |
| Data completeness | > 99.9% of expected readings | Expected vs actual per hour |

---

## Daily checks

```sql
-- 1. Freshness
SELECT MAX(event_ts) AS latest,
       NOW() - MAX(event_ts) AS lag
FROM mart.fact_sensor_reading;

-- 2. Batch health, last 24h
SELECT status, COUNT(*), MAX(completed_at)
FROM mart.etl_batch_log
WHERE started_at > NOW() - INTERVAL '24 hours'
GROUP BY status;

-- 3. Unknown-member leakage (should be zero)
SELECT COUNT(*) FROM mart.fact_sensor_reading WHERE machine_key = -1;

-- 4. Alert volume sanity (a spike means the dedup logic regressed)
SELECT date_key, COUNT(*)
FROM mart.fact_alert
WHERE raised_at > NOW() - INTERVAL '7 days'
GROUP BY date_key ORDER BY date_key;

-- 5. Gap detection - hours with no readings
SELECT d.full_date, t.hour_24, COUNT(f.reading_id) AS readings
FROM mart.dim_date d
CROSS JOIN (SELECT DISTINCT hour_24 FROM mart.dim_time) t
LEFT JOIN mart.fact_sensor_reading f
       ON f.date_key = d.date_key
      AND EXTRACT(HOUR FROM f.event_ts) = t.hour_24
WHERE d.full_date BETWEEN CURRENT_DATE - 2 AND CURRENT_DATE
GROUP BY d.full_date, t.hour_24
HAVING COUNT(f.reading_id) = 0
ORDER BY 1, 2;
```

---

## Incident playbook

### Streaming job dead

**Symptoms:** freshness lag growing, terminal returned to prompt, `etl_batch_log` silent.

1. Confirm containers: `docker compose ps` — all four `Up`
2. Confirm broker reachable: `docker exec iot-kafka kafka-topics --bootstrap-server localhost:9092 --list`
3. Clear orphaned processes (Windows): Task Manager → Details → end `java.exe` / `python.exe`
4. Relaunch with the standard env block
5. Kafka retention covers the gap; the checkpoint resumes from the last committed offset — no data loss unless the outage exceeded topic retention

### `Python worker failed to connect back`

Root cause is host resource exhaustion, not code. Mitigations already applied in `streaming_job_star.py`: single `foreachBatch`, `spark.python.worker.reuse=true`, `local[2]`, 1 GB driver.

If it recurs: raise `TRIGGER_INTERVAL` to 60 s, lower `maxOffsetsPerTrigger`, close Power BI Desktop while the pipeline runs.

### Machine sleep killed the pipeline

The dominant failure mode observed in development. Sleep suspends the JVM; on wake, the JDBC socket and Python worker sockets are stale.

```powershell
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE   # expect AC index 0x00000000
```

Battery (DC) sleep remains active by design — keep the machine on mains during runs.

### Alert count implausible

1. `SELECT COUNT(*) FROM mart.fact_alert WHERE raised_at > NOW() - INTERVAL '1 hour';`
2. If growing per micro-batch, the re-arm anti-join is not firing — verify `ALERT_REARM_MINUTES` and that `uq_alert_occurrence` exists
3. Backfill correction: re-run the gaps-and-islands block in `04_migration_backfill.sql` against the affected window

---

## Partition maintenance

Run monthly:

```sql
-- create next month's partition
DO $$
DECLARE
    nxt   DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
    after DATE := nxt + INTERVAL '1 month';
    part  TEXT := 'fact_sensor_reading_' || TO_CHAR(nxt, 'YYYY_MM');
BEGIN
    EXECUTE FORMAT(
        'CREATE TABLE IF NOT EXISTS mart.%I PARTITION OF mart.fact_sensor_reading
         FOR VALUES FROM (%L) TO (%L)', part, nxt, after);
END $$;

-- archive partitions older than 90 days
-- ALTER TABLE mart.fact_sensor_reading DETACH PARTITION mart.fact_sensor_reading_2026_05;
```

---

## Backup and recovery

```bash
# nightly logical backup
docker exec iot-postgres pg_dump -U iot_user -d iot_dashboard -Fc -f /tmp/iot_$(date +%F).dump
docker cp iot-postgres:/tmp/iot_$(date +%F).dump ./backups/

# restore
docker exec -i iot-postgres pg_restore -U iot_user -d iot_dashboard --clean < backups/iot_2026-08-09.dump
```

RPO 24 h, RTO 1 h. For tighter RPO enable WAL archiving with `archive_mode = on`.

---

## Environments

| Env | Purpose | Data | Refresh |
|---|---|---|---|
| DEV | Local development | Synthetic producer | Continuous |
| UAT | Business validation | Masked copy of prod | Nightly |
| PROD | Live reporting | Real telemetry | Continuous + 15 min model refresh |

Promotion path: Git → CI validates SQL with `pglast` and Python with `ruff` → deploy to UAT → sign-off → PROD. Power BI uses deployment pipelines with parameterised data source rules.

---

## Monitoring and alerting

| Signal | Threshold | Action |
|---|---|---|
| Freshness lag | > 15 min | Page on-call |
| Batch failures | 3 consecutive | Page on-call |
| Unknown-member rows | > 0 | Ticket to data engineering |
| Model refresh failure | 1 | Email BI team |
| Alert insert rate | > 100/min | Investigate dedup regression |

Emit `etl_batch_log` to Prometheus via `postgres_exporter`; alert in Grafana or Azure Monitor.

---

## Runbook ownership

| Component | Owner | Escalation |
|---|---|---|
| Kafka / Docker | Platform | Infrastructure lead |
| Spark job | Data Engineering | DE lead |
| Postgres / star schema | Data Engineering | DBA |
| Semantic model / RLS | BI Team | BI lead |
| Report content | Business Analyst | Ops manager |
