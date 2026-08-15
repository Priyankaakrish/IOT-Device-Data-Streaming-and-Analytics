# 🏭 IoT Manufacturing Analytics Pipeline

> End-to-end real-time analytics for factory IoT telemetry — Kafka ingestion, Spark Structured Streaming ETL, a Kimball star schema in PostgreSQL with row-level security, Apache Airflow orchestrating the batch and data-quality layer, and a 3-page Power BI report tracking OEE.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange?logo=apachespark)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20?logo=apachekafka)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![Power BI](https://img.shields.io/badge/Power%20BI-DirectQuery-F2C811?logo=powerbi&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![Tests](https://img.shields.io/badge/tests-24%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

<!-- Check the repo name below matches yours; the badge 404s silently if not. -->

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Dashboard](#dashboard)
- [Data Model](#data-model)
- [KPI Reference](#kpi-reference)
- [Row-Level Security](#row-level-security)
- [Star vs Snowflake](#star-vs-snowflake)
- [Local Setup](#local-setup)
- [Running It Unattended](#running-it-unattended)
- [Apache Airflow Implementation](#apache-airflow-implementation)
- [Docker Configuration](#docker-configuration)
- [Running on Windows](#running-on-windows)
- [Tests](#tests)
- [Results](#results)
- [Bugs Found and Fixed](#bugs-found-and-fixed)
- [Scope and Limitations](#scope-and-limitations)
- [License](#license)
- [Author](#author)

---

## Overview

This project builds a real-time analytics platform for a simulated factory floor of six machines. Sensor readings stream through Kafka, are transformed and conformed by Spark Structured Streaming, land in a dimensionally-modelled PostgreSQL warehouse, and surface as an operations dashboard in Power BI. Apache Airflow orchestrates the batch and data-quality layer alongside it.

The pipeline is the straightforward part. What the project actually demonstrates:

- **A star schema built to Kimball rules**, not just named after one — declared grain, surrogate keys, SCD Type 2, conformed dimensions, and grain enforced by database constraints
- **Idempotent streaming writes** via staging table plus `ON CONFLICT DO UPDATE`, so replaying a checkpoint does not double-count
- **Row-level security tested across four access tiers**, applied once to `dim_machine` and cascading to all four fact tables
- **A benchmark that argues against its own conclusion** — the star-vs-snowflake comparison reports no reliable speed advantage and documents three earlier versions that produced convincing but wrong numbers
- **Data quality as enforced gates, not reports** — Airflow DAGs that fail when freshness, batch success, unknown-key rate or dead-letter thresholds are breached
- **Three real data-quality bugs found and fixed**, including an OEE reading of 174.9%

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          DOCKER COMPOSE NETWORK                            │
│                                                                            │
│   ┌──────────────┐        ┌──────────────────┐                             │
│   │  Zookeeper   │◄──────►│   Apache Kafka   │                             │
│   │    :2181     │        │  :9092 (host)    │                             │
│   └──────────────┘        │  :29092 (net)    │                             │
│                           │                  │                             │
│                           │  topic:          │                             │
│                           │  iot-sensor-data │                             │
│                           │  1 partition     │                             │
│                           └────────┬─────────┘                             │
│                                    │                                       │
│   ┌──────────────┐        ┌────────▼─────────┐        ┌──────────────────┐ │
│   │   Adminer    │◄──────►│   PostgreSQL 16  │◄──────►│  Apache Airflow  │ │
│   │    :8081     │        │  :5433 (host)    │        │  :8080 (host)    │ │
│   └──────────────┘        │  :5432 (net)     │        │                  │ │
│                           │                  │        │  standalone      │ │
│                           │  public.*  (v1)  │        │  LocalExecutor   │ │
│                           │  mart.*    star  │        │  3 DAGs          │ │
│                           │  snow.*    snow  │        │  OPTIONAL        │ │
│                           └────────▲─────────┘        └──────────────────┘ │
└────────────────────────────────────┼───────────────────────────────────────┘
                                     │ JDBC
              ┌──────────────────────┴───────────────────────┐
              │                                              │
   ┌──────────▼─────────────┐                    ┌───────────▼────────────┐
   │  HOST: Python 3.11     │                    │  HOST: Power BI Desktop│
   │                        │                    │                        │
   │  producer/             │                    │  Storage mode: Mixed   │
   │   iot_sensor_producer  │───► Kafka          │  DirectQuery + Import  │
   │   6 machines, 1 Hz     │                    │                        │
   │                        │                    │  3 pages               │
   │  spark/                │                    │  ~45 DAX measures      │
   │   streaming_job_star   │◄─── Kafka          │  Dynamic RLS role      │
   │   foreachBatch, 30s    │───► Postgres       │                        │
   └────────────────────────┘                    └────────────────────────┘

Ports
  ┌────────┬──────────────┬─────────────────────────────────────┐
  │  9092  │ Kafka        │ producer + Spark connect here       │
  │  5433  │ PostgreSQL   │ shifted off 5432 to avoid collision │
  │  8081  │ Adminer      │ browse the warehouse                │
  │  8080  │ Airflow      │ DAG UI -- optional compose overlay  │
  └────────┴──────────────┴─────────────────────────────────────┘
```

Spark runs on the host via `spark-submit`, not in a container — this keeps the Kafka and JDBC networking simple and matches how the job would be launched by a scheduler in a real deployment.

Airflow is an **optional overlay**. `docker-compose.yml` brings up four containers and the pipeline works without it; `docker-compose.airflow.yml` adds the fifth. It reuses the existing PostgreSQL instance for its metadata rather than running one of its own.

### What is orchestrated, and what isn't

The platform has two paths with different execution models. Conflating them is the most common way this kind of architecture gets described wrongly.

```
  STREAMING PATH — continuous, supervised, not scheduled

  Sensors ──► Kafka ──► Spark Structured Streaming ──► PostgreSQL
   1 Hz               30s trigger, foreachBatch          mart.*
                      supervised by supervise_streaming.ps1  │
                                                             │
  BATCH PATH — periodic, dependency-ordered, genuinely schedulable
                                                             │
   ┌──► snow.refresh_from_star()          daily              │
   ├──► SCD2 change detection             daily              │
   ├──► drop_sensor_partitions_older_than() monthly          │
   ├──► DLQ replay                        on demand          │
   └──► v_health_dashboard report         hourly             │
                                                             ▼
                                                        Power BI
```

**The streaming job is supervised, not orchestrated.** A scheduler like Airflow launches a task, waits for it to finish, and reads an exit code. A Structured Streaming query never finishes, so there is nothing for a scheduler to wait on. What it needs is a process that notices when it dies and restarts it from the checkpoint — which is what `supervise_streaming.ps1` does.

**The batch path is where an orchestrator would earn its place.** Snowflake refresh, SCD2 detection, partition retention and DLQ replay are finite tasks with real schedules and real ordering. They run through `run_batch_jobs.ps1`, which writes every run to `mart.etl_batch_log` using the same `job_name` / `status` / `error_detail` contract as the streaming job — so batch and streaming lineage live in one table.

```powershell
.\run_batch_jobs.ps1 -Job hourly     # health + dead-letter report
.\run_batch_jobs.ps1 -Job daily      # snowflake refresh, SCD2 drift, health
.\run_batch_jobs.ps1 -Job monthly    # partition retention (asks to confirm)
.\run_batch_jobs.ps1 -Job snowflake-refresh -DryRun
```

The same work is also defined as Airflow DAGs in `airflow_dags.py`, as an **optional overlay**. The base stack is unchanged; nothing runs unless you ask for it:

```powershell
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
# UI at http://localhost:8080 -- credentials in: docker logs iot-airflow
```

| DAG | Schedule | Does |
|---|---|---|
| `iot_platform_daily` | 05:00 UTC | Batch/ETL and Data Quality in parallel, converging on a warehouse gate |
| `iot_data_quality` | Hourly at :15 | The five quality gates alone, between daily runs |
| `iot_retention` | Monthly, paused | Partition drop — paused on creation because it is irreversible |

```
                        Airflow
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       Batch/ETL Jobs            Data Quality Jobs
   refresh_snowflake            check_freshness
   assert_snowflake_in_sync     check_batch_success_rate
   detect_scd2_drift            check_unknown_keys
              │                 check_dead_letters
              │                 check_alert_grain
              └────────────┬────────────┘
                           ▼
                 data_warehouse_ready        (all_success gate)
                           ▼
                   publish_to_powerbi
```

**One container, not four.** The standard Airflow compose runs a webserver, scheduler, triggerer and its own metadata database. For three DAGs that is ceremony, so this uses `airflow standalone` and borrows the PostgreSQL instance the warehouse already runs on, in a separate `airflow` database. Memory is capped at 2 GB so the scheduler cannot starve the Spark job.

**The quality tasks are gates, not reports.** Each one raises and fails the DAG when a threshold is breached — freshness over 5 minutes, batch success under 99%, unknown-key rate over 0.1%, any unreplayed dead letters. A check that only logs is a check nobody reads.

**What Airflow does not do here:** orchestrate the streaming job. A Structured Streaming query never finishes, so there is no exit code to wait on. That stays with `supervise_streaming.ps1`.

`.airflowignore` restricts DAG parsing to `airflow_dags.py`. Without it the scheduler would import the PySpark job and the Kafka producer on every parse cycle — starting a JVM and opening a broker connection every 60 seconds.

---

## Data Flow

```
 Simulated machine fleet (Markov chain: RUNNING → IDLE → DOWN → FAULT)
        │
        ▼
 JSON payload  { machine_id, event_time, temperature_c, power_kw,
                 vibration_mm_s, status, units_produced, good_units }
        │
        ▼
 Kafka topic: iot-sensor-data
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│         Spark Structured Streaming — foreachBatch           │
│                                                             │
│   1. Parse JSON, watermark 10 min, quarantine bad rows      │
│   2. add_conformed_keys()                                   │
│        ├── date_key      yyyyMMdd                           │
│        ├── time_key      minutes since midnight             │
│        ├── shift_key     Morning / Afternoon / Night        │
│        ├── machine_key   broadcast join, -1 fallback        │
│        ├── status_key    broadcast join, -1 fallback        │
│        └── flags_key     DQ junk dimension                  │
│   3. Four writers, one persisted DataFrame                  │
│        ├── write_sensor_facts()      append                 │
│        ├── write_alert_facts()       transition-grained     │
│        ├── write_hourly_aggregate()  staging + upsert       │
│        └── write_downtime_facts()    open/close episodes    │
│   4. Batch lineage -> etl_batch_log, bad rows -> DLQ        │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                PostgreSQL — mart schema (star)               │
│                                                             │
│  DIMENSIONS                        FACTS                    │
│   dim_date          4,018 rows      fact_sensor_reading     │
│   dim_time          1,440 rows       (partitioned by month) │
│   dim_machine       6 rows, SCD2     fact_production_hourly │
│   dim_shift         4 rows           fact_alert             │
│   dim_machine_status 7 rows          fact_downtime          │
│   dim_alert_type    6 rows                                  │
│   dim_reading_flags 8 rows          + dq_exception          │
│                                     + etl_batch_log         │
│  Row-level security on dim_machine cascades to all facts    │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
 Power BI  ──►  Overview · Production Performance · Maintenance & Reliability
```

---

## Features

- **Real-time ingestion** — Kafka topic consumed on a 30-second trigger with `maxOffsetsPerTrigger` backpressure
- **Single `foreachBatch` writer** — replaces three concurrent `writeStream` queries, cutting Python worker pressure dramatically on Windows
- **Broadcast dimension cache** — dimensions loaded once per job rather than per micro-batch
- **Idempotent upserts** — `UNLOGGED` staging table plus `ON CONFLICT DO UPDATE`; weighted averages stay correct across merges because `temp_sum` and `reading_count` are persisted
- **Transition-grained alerts** — re-arm window plus anti-join, backed by a unique constraint so the duplicate-row bug cannot silently return
- **SCD Type 2** on `dim_machine` with `row_hash` change detection
- **Table partitioning** — `fact_sensor_reading` RANGE-partitioned by month with BRIN indexes
- **Row-level security** — Postgres policies via `SECURITY DEFINER` function and session GUC, mirrored in Power BI with `USERPRINCIPALNAME()`
- **Data quality tracking** — junk dimension for reading flags, `dq_exception` table, `etl_batch_log` for lineage
- **Two schemas, benchmarked** — star and snowflake variants of the same facts, with `EXPLAIN ANALYZE` comparison and a refresh/staleness mechanism
- **24 unit tests** on key derivation, plus a regression guard on the JDBC keepalive settings
- **Airflow orchestration** — three DAGs covering batch/ETL, hourly data-quality gates and monthly retention, converging on a warehouse-ready gate before the BI layer is considered trustworthy
- **Operations runbook** — SLOs, daily checks, incident playbooks

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Language** | Python 3.11 |
| **Streaming** | Apache Spark 3.5.0 (Structured Streaming, PySpark) |
| **Message broker** | Apache Kafka 7.5 (Confluent images) + Zookeeper |
| **Orchestration** | Apache Airflow 2.9.3 — batch/ETL and data-quality DAGs |
| **Warehouse** | PostgreSQL 16 — partitioning, BRIN, RLS, generated columns |
| **BI** | Power BI Desktop — composite model, DirectQuery + Import, DAX |
| **Containerization** | Docker + Docker Compose |
| **DB admin** | Adminer |
| **Testing** | pytest 8.x |
| **Connectivity** | PostgreSQL JDBC 42.7.3, spark-sql-kafka-0-10 |
| **Modelling** | Kimball dimensional modelling — star and snowflake |

---

## Project Structure

```
iot-streaming-dashboard/
│
├── docker-compose.yml              Kafka, Zookeeper, Postgres, Adminer
├── .env.example                    connection settings template
│
├── producer/
│   ├── iot_sensor_producer.py      Markov-chain machine simulator → Kafka
│   └── requirements.txt
│
├── spark/
│   ├── streaming_job_star.py       v2: Kafka → star schema (current)
│   ├── streaming_job.py            v1: Kafka → flat tables (migration story)
│   ├── test_spark.py               24 unit tests on key derivation
│   ├── conftest.py                 Spark test environment setup
│   └── requirements.txt
│
├── sql/
│   ├── 01_schema.sql               v1 flat tables
│   ├── 02_kpi_views.sql            one view per v1 KPI
│   ├── 03_sample_data.sql          machine master, planned production time
│   ├── 04_star_schema.sql          6 dimensions, 4 facts, partitions, DQ
│   ├── 05_seed_dimensions.sql      date, time, shift, status, alert, machine
│   ├── 06_migration_backfill.sql   flat → star, with alert deduplication
│   ├── 07_row_level_security.sql   policies, roles, masking, audit
│   ├── 08_snowflake_schema.sql     normalised variant + refresh function
│   ├── 09_schema_benchmark.sql     star vs snowflake, EXPLAIN ANALYZE
│   └── 10_production_hardening.sql DLQ, downtime staging, health views
│
├── .github/workflows/
│   └── ci.yml                      lint, tests, SQL parse, integration
│
├── powerbi/
│   ├── IOT Dashboard.pbix          the report
│   ├── POWERBI_SETUP.md            model, storage modes, RLS, page spec
│   ├── dax_measures.txt            ~45 measures in 9 display folders
│   └── theme.json                  navy/orange corporate theme
│
└── docs/
    ├── DATA_MODEL.md               grain, bus matrix, SCD strategy
    ├── STAR_VS_SNOWFLAKE.md        both ERDs, benchmark, when each wins
    ├── DASHBOARD_AS_BUILT.md       what the .pbix actually contains
    ├── OPERATIONS_RUNBOOK.md       SLOs, daily checks, incident playbooks
    └── images/                     dashboard screenshots
```

---

## Dashboard

Three pages built on the star schema, each answering one question.

| Page | Question | Key visuals |
|---|---|---|
| **Overview** | Are we hitting OEE, and which machine is dragging? | OEE / Availability / Quality bands, 30-day trend, machine summary table, alerts / downtime / energy panels |
| **Production Performance** | Where is output being lost — availability, speed, or scrap? | OEE components by machine, OEE by shift, energy trend, OEE loss waterfall |
| **Maintenance & Reliability** | What is failing, how often, how long to recover? | MTBF / MTTR, alert volume by severity, reliability quadrant, live alert log |

### Page 1 — Overview

<img width="658" height="331" alt="Executive overview page showing OEE, Availability and Quality KPI bands above a 30-day trend, a machine summary table, and alerts, downtime and energy panels" src="https://github.com/user-attachments/assets/43cb8249-0ea9-4a2e-a7db-35e2c1faca23" />
<h4>Executive overview page showing OEE, Availability and Quality KPI bands above a 30-day trend, a machine summary table, and alerts, downtime and energy panels</h4>

### Page 2 — Production Performance

<img width="661" height="348" alt="image" src="https://github.com/user-attachments/assets/8488de90-58bb-45a1-8aa5-38daca5e3dfb" />
<h4>Production performance page showing units produced, performance and scrap KPIs, OEE components by machine, OEE by shift, energy trend and an OEE loss waterfall</h4>

### Page 3 — Maintenance &amp; Reliability

<img width="655" height="338" alt="image" src="https://github.com/user-attachments/assets/90f92ef8-68d4-4220-9628-304703d852f3" />
<h4>Maintenance and reliability page showing MTBF and MTTR, alert volume by type and severity, a reliability quadrant scatter, downtime by machine and a live alert log</h4>

**Design:** navy `#0F2537` / orange `#E8622C` theme applied via `powerbi/theme.json`, sidebar page navigation with selected-state highlighting, coloured KPI bands seated flush above each chart panel.

---

## Data Model

Six dimensions, four facts, in the `mart` schema. `public` is left untouched so the v1 job and original report keep working through the migration.

| Fact table | Declared grain | Type | Rows |
|---|---|---|---|
| `fact_sensor_reading` | One row per sensor reading per machine | Transaction | ~290,000 |
| `fact_production_hourly` | One row per machine per hour | Accumulating | ~140/day |
| `fact_alert` | One row per alert **occurrence** | Transaction | ~250 |
| `fact_downtime` | One row per continuous downtime **episode** | Accumulating snapshot | 1,318 |

### v1 → v2

| Area | v1 (flat) | v2 (star) |
|---|---|---|
| Model | 5 tables + 8 views | 6 dimensions, 4 facts |
| Keys | Natural (`machine_id`) | Surrogate + reserved `-1` Unknown |
| History | Overwritten | SCD Type 2 on `dim_machine` |
| Alerts | One row per micro-batch (~15M rows) | One row per state transition |
| Spark writes | 3 concurrent `writeStream` | 1 `foreachBatch`, worker reuse |
| Idempotency | Append-only, duplicates on replay | Staging + `ON CONFLICT` upsert |
| Security | None | Postgres RLS + Power BI dynamic RLS |
| Observability | Console logs | `etl_batch_log`, `dq_exception` |

Full reasoning, bus matrix and SCD strategy in [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md).

---

## KPI Reference

| KPI | Definition | Notes |
|---|---|---|
| **OEE** | Availability × Performance × Quality | Capped at 100% — an uncapped version read 174.9% |
| **Availability %** | Runtime / Planned time | `MIN(..., 1)` guard applied |
| **Performance %** | (Units × ideal cycle time) / Runtime | Only as credible as `ideal_cycle_time_sec` |
| **Quality %** | Good units / Total units | |
| **MTBF** | Runtime / failure count | Reported in **minutes** — simulator failures are ~6 min apart |
| **MTTR** | Downtime minutes / episode count | Reported in minutes |
| **Active Alerts** | Count of alert occurrences | Row count, not `SUM(alert_id)` — that bug read "15M" |
| **Energy per Unit** | kWh / units produced | Most differentiated metric in the dataset |

`powerbi/dax_measures.txt` holds ~45 measures across 9 display folders, including the OEE component set, time intelligence, alerting, and energy.

---

## Row-Level Security

Applied once to `dim_machine`, which sits on the one-side of every fact relationship, so the filter cascades to all four facts automatically.

```sql
SET app.user_upn = 'operator.linea@example.com';
SELECT COUNT(*) FROM mart.dim_machine;   -- 4
```

| Access tier | Machines visible |
|---|:--:|
| Line A operator | 4 |
| Plant manager | 6 |
| Executive | 6 |
| Unmapped user | 1 (Unknown member only) |

The session variable is deliberately named `app.user_upn` and **not** `app.current_user` — `current_user` is a reserved keyword in PostgreSQL, and `SET app.current_user = '...'` fails to parse.

Mirrored in Power BI with a dynamic RLS role driven by `USERPRINCIPALNAME()`.

---

## Star vs Snowflake

Both schemas are implemented so the design decision can be **demonstrated rather than asserted**.

| Question | Star joins | Snowflake joins |
|---|:--:|:--:|
| OEE by machine type and line | 1 | 3 |
| Alerts by severity and owning team | 1 | 3 |
| Energy rollup to plant | 1 | 3 |

**Storage**, restricted to the dimensions whose design actually differs (`dim_date` and `dim_time` are shared and excluded):

| Design | Tables | Storage |
|---|:--:|---|
| mart (star) | 2 | **120 kB** |
| snow (normalised) | 8 | **336 kB** |

Snowflaking costs 2.8× more storage here. Per-table overhead — page headers, PK indexes, catalog entries — outweighs de-duplicating a handful of short strings.

**Execution time showed no reliable difference**, and snowflake won several runs. The same query on the same data swung from 0.381 ms to 8.246 ms between runs, with the planner flipping from Nested Loop to Hash Join. Any millisecond figure from this dataset would be noise dressed as evidence.

The honest conclusion in [`docs/STAR_VS_SNOWFLAKE.md`](docs/STAR_VS_SNOWFLAKE.md): the star wins on **RLS simplicity, VertiPaq fit and DAX readability** — not on query speed. The document also records three benchmarking traps, each of which produced a wrong answer that looked right.

---

## Local Setup

### Prerequisites

- Docker Desktop
- Python 3.11 (pyspark 3.5 does not support 3.12+)
- JDK 17
- Power BI Desktop (Windows only)

### Steps

```bash
# 1. Clone
git clone https://github.com/Priyankaakrish/iot-streaming-dashboard.git
cd iot-streaming-dashboard

# 2. Configure
cp .env.example .env

# 3. Start infrastructure
docker compose up -d
docker compose ps          # expect 4 containers running
```

Postgres is on host port **5433**. Adminer at http://localhost:8081 (server `postgres`, user `iot_user`, db `iot_dashboard`).

```powershell
# 4. Build the warehouse
Get-Content sql\04_star_schema.sql        | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
Get-Content sql\05_seed_dimensions.sql    | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
Get-Content sql\06_migration_backfill.sql | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
Get-Content sql\07_row_level_security.sql | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
```

> `06` runs inside a transaction and ends with a reconciliation query. **Read that output before uncommenting `COMMIT;`** — expect target sensor rows to equal source rows, zero unknown-key rows, and a >99% reduction in alert rows.

```powershell
# 5. Start the producer (terminal 1)
py -3.11 -m pip install -r producer\requirements.txt
py -3.11 producer\iot_sensor_producer.py --bootstrap-servers localhost:9092 --interval 1.0
```

```powershell
# 6. Start the streaming job (terminal 2)
$env:PYSPARK_SUBMIT_ARGS = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 pyspark-shell"
py -3.11 spark\streaming_job_star.py
```

```powershell
# 7. Verify data is landing
docker exec -it iot-postgres psql -U iot_user -d iot_dashboard `
  -c "SELECT COUNT(*) FROM mart.fact_sensor_reading;"
```

**8. Open Power BI** — `powerbi/IOT Dashboard.pbix`, or build from scratch following `powerbi/POWERBI_SETUP.md`.

### Optional — build and benchmark the snowflake variant

```powershell
Get-Content sql\08_snowflake_schema.sql  | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
Get-Content sql\09_schema_benchmark.sql  | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
```

---

## Running It Unattended

The pipeline can be left running rather than babysat.

**Apply the hardening objects** — dead-letter table, downtime staging, health views:

```powershell
Get-Content sql\10_production_hardening.sql | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
```

**Supervise the streaming job.** It restarts on exit with linear backoff, brings the stack up if Kafka is down, and stops trying after three rapid failures — a job dying inside a minute is a startup fault, not a transient one:

```powershell
.\supervise_streaming.ps1
```

Because the sink is idempotent and the job is checkpointed, a restart resumes from the last committed offset with no duplication.

**Check health** — one command, exits non-zero if anything is wrong, so it works as a scheduled task:

```powershell
.\healthcheck.ps1
.\healthcheck.ps1 -Quiet     # speaks only on failure
```

Or query directly:

```sql
SELECT * FROM mart.v_health_dashboard;
```

| Check | Healthy when |
|---|---|
| Pipeline freshness | Newest reading under 2 minutes old |
| Batch success rate | ≥ 99% over 24 hours |
| Unknown key rate | < 0.1% of rows |
| Dead-letter queue | Zero quarantined in 24 hours |
| Stuck downtime episodes | None open beyond 4 hours |

**Dead-letter queue.** Messages failing the schema contract are written to `mart.dlq_sensor_reading` with topic, partition and offset retained, so a corrected payload can be replayed rather than lost:

```sql
SELECT failure_reason, COUNT(*), MIN(quarantined_at)
FROM mart.dlq_sensor_reading
WHERE replayed_at IS NULL
GROUP BY failure_reason;
```

**Retention.** Sensor partitions are dropped, not deleted — a catalogue operation instead of hours of I/O:

```sql
SELECT * FROM mart.drop_sensor_partitions_older_than(6);
```

---

## Apache Airflow Implementation

Airflow 2.9.3 orchestrates the batch and data-quality layer. It is an **optional overlay** — the base four-container stack works without it.

### Topology

One container, not the usual four. The standard Airflow deployment runs a webserver, a scheduler, a triggerer and its own metadata database; for three DAGs that is ceremony. This uses `airflow standalone` (all three processes in one) with `LocalExecutor`, and borrows the PostgreSQL instance the warehouse already runs on, in a separate `airflow` database.

| Setting | Value | Why |
|---|---|---|
| Image | `apache/airflow:2.9.3-python3.11` | Matches the project's Python version |
| Executor | `LocalExecutor` | Parallel tasks without a Celery broker |
| Metadata DB | `airflow` database on `iot-postgres` | Avoids a second database container |
| DAGs folder | Project root, via `AIRFLOW__CORE__DAGS_FOLDER` | No `dags/` directory needed |
| Connection | `AIRFLOW_CONN_IOT_POSTGRES` env var | A fresh start needs no manual UI setup |
| Memory cap | 2 GB | Prevents the scheduler starving the host-side Spark job |
| Project mount | Read-only | Airflow can read DAGs, not modify the repo |

`.airflowignore` restricts parsing to `airflow_dags.py`. Without it the scheduler recursively imports `spark/` and `producer/` on every parse cycle — starting a JVM and opening a Kafka connection every 60 seconds.

### DAG inventory

| DAG | Schedule | Tasks | Purpose |
|---|---|:--:|---|
| `iot_platform_daily` | `0 5 * * *` | 12 | Batch/ETL and Data Quality in parallel, converging on a warehouse gate |
| `iot_data_quality` | `15 * * * *` | 5 | Quality gates alone, hourly, between daily runs |
| `iot_retention` | `0 3 1 * *` | 1 | Partition retention — **ships paused**, it is irreversible |

<img width="1360" height="602" alt="image" src="https://github.com/user-attachments/assets/af2523ab-7ff7-4a76-9d7a-c895fdf501d4" />


### `iot_platform_daily` structure

```
                          start
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
      ┌───────────────┐          ┌─────────────────┐
      │   batch_etl   │          │  data_quality   │
      ├───────────────┤          ├─────────────────┤
      │ refresh_      │          │ check_freshness │
      │  snowflake    │          │ check_batch_    │
      │      ↓        │          │  success_rate   │
      │ assert_       │          │ check_unknown_  │
      │  snowflake_   │          │  keys           │
      │  in_sync      │          │ check_dead_     │
      │      ↓        │          │  letters        │
      │ detect_scd2_  │          │ check_alert_    │
      │  drift        │          │  grain          │
      └───────┬───────┘          └────────┬────────┘
              └─────────────┬─────────────┘
                            ▼
                  data_warehouse_ready          trigger_rule: all_success
                            ▼
                    publish_to_powerbi          writes lineage to etl_batch_log
                            ▼
                           end
```

The convergence point is the reason this is a DAG rather than a script running commands in order: nothing downstream executes unless **both** branches succeeded. A stale snowflake copy or a breached quality threshold stops the warehouse being marked fit to report on.

### Data quality gates

Each task **raises and fails the DAG** on breach. A check that only logs is a check nobody reads.

| Task | Threshold | Catches |
|---|---|---|
| `check_freshness` | Newest reading under 300s | The streaming job dying overnight |
| `check_batch_success_rate` | ≥ 99% over 24h | Persistent batch failures |
| `check_unknown_keys` | < 0.1% of rows | A new asset missing from master data |
| `check_dead_letters` | Zero awaiting replay | A broken producer payload contract |
| `check_alert_grain` | No null `alert_id` | Grain violation bypassing the constraint |

Thresholds are constants at the top of `airflow_dags.py`, so changing an SLO is one edit rather than a search. The five gates map directly onto the defects in [Bugs Found and Fixed](#bugs-found-and-fixed) — each one would have caught a real failure earlier.

### Batch/ETL tasks

**`refresh_snowflake`** calls `snow.refresh_from_star()`. **`assert_snowflake_in_sync`** then verifies the verdict, because refreshing is not the same as succeeding — an `ON CONFLICT DO NOTHING` on an accumulating fact freezes it at its first observed value while row counts still match.

**`detect_scd2_drift`** compares `dim_machine` against the source and reports assets whose attributes have moved. It *reports* rather than inserting a new version: a silent history rewrite is worse than a manual step.

### Running it

```powershell
# Prerequisite -- the DQ tasks read views this creates
Get-Content sql\10_production_hardening.sql | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard

docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
docker exec iot-airflow cat /opt/airflow/standalone_admin_password.txt
```

UI at **http://localhost:8080**, username `admin`. Stop Airflow without touching the pipeline:

```powershell
docker compose -f docker-compose.yml -f docker-compose.airflow.yml stop airflow
```

### Two implementation notes

**The metadata database is created by a separate container.** `airflow-init` runs once, creates the `airflow` database if absent, and exits; Airflow waits on `service_completed_successfully`. This cannot be done inside the Airflow container — its entrypoint verifies the metadata connection *before* reaching any command you give it, so a bootstrap step in `command:` never executes and the container dies with `database "airflow" does not exist`.

**Airflow does not orchestrate the streaming job, and cannot.** A Structured Streaming query never finishes, so there is no exit code for a scheduler to wait on. That job needs supervision, not scheduling — `supervise_streaming.ps1` restarts it from the checkpoint if the process dies. Conflating the two is the most common way this architecture gets described wrongly.

---

## Docker Configuration

Four services on the default Compose network. Spark deliberately runs on the host rather than in a container — it needs the JDBC driver, the Kafka connector and a specific Python version, and containerising it hides the very networking problems worth understanding.

### Services

| Container | Image | Host port | Purpose |
|---|---|:--:|---|
| `iot-zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | 2181 | Kafka coordination |
| `iot-kafka` | `confluentinc/cp-kafka:7.6.0` | 9092, 29092 | Message broker |
| `iot-postgres` | `postgres:16` | 5433 | Warehouse |
| `iot-adminer` | `adminer:latest` | 8081 | Browse the database |

### docker-compose.yml

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    container_name: iot-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: iot-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  postgres:
    image: postgres:16
    container_name: iot-postgres
    environment:
      POSTGRES_USER: iot_user
      POSTGRES_PASSWORD: iot_password
      POSTGRES_DB: iot_dashboard
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "5433:5432"
    volumes:
      - iot_pg_data:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d

  adminer:
    image: adminer:latest
    container_name: iot-adminer
    ports:
      - "8081:8080"
    depends_on:
      - postgres

volumes:
  iot_pg_data:
```

### Configuration decisions

**Kafka advertises two listeners.** This is the single most common Kafka-in-Docker failure and it is worth understanding rather than copying:

```
PLAINTEXT://kafka:29092          ← for clients inside the Compose network
PLAINTEXT_HOST://localhost:9092  ← for the producer and Spark on the host
```

A broker tells clients where to reach it via `advertised.listeners`. With a single listener, either the host or the container network gets an address it cannot resolve. The producer and Spark run on the host, so they use **9092**; anything added inside Compose later would use **29092**.

**Postgres is published on 5433, not 5432.** A local PostgreSQL install would otherwise collide silently — you would connect successfully to the wrong database. Inside the Compose network it is still 5432.

**`POSTGRES_HOST_AUTH_METHOD: trust`** is for local development only. It disables password authentication entirely. Do not carry this into anything reachable from a network.

**Replication factor 1** on the offsets topic — a single broker cannot replicate. Fine locally, unacceptable in production, and noted in [Scope and Limitations](#scope-and-limitations).

**Named volume `iot_pg_data`** persists the warehouse across `docker compose down`. Use `docker compose down -v` to wipe it and start clean.

### The init-scripts gotcha

`./sql:/docker-entrypoint-initdb.d` runs **every** `.sql` file in that folder alphabetically, but **only on the first boot of an empty data volume**. Two consequences:

1. On a fresh clone, `04`–`09` run automatically alongside `01`–`03`. That mostly works, but `06_migration_backfill.sql` ends with a reconciliation query whose output you are meant to read before committing, and `09` is a benchmark you probably do not want in your boot sequence. Run them manually as shown in [Local Setup](#local-setup) and check the reconciliation output.
2. Editing a `.sql` file and running `docker compose up -d` again does **nothing** — the volume already exists. To re-run the init scripts you must destroy the volume:

```powershell
docker compose down -v
docker compose up -d
```

### Common commands

```powershell
docker compose up -d              # start everything
docker compose ps                 # expect 4 containers running
docker compose logs -f kafka      # follow broker logs
docker compose restart kafka      # broker went unreachable
docker compose down               # stop, keep data
docker compose down -v            # stop and wipe the warehouse

# open a psql shell
docker exec -it iot-postgres psql -U iot_user -d iot_dashboard

# list Kafka topics
docker exec -it iot-kafka kafka-topics --bootstrap-server localhost:9092 --list

# tail the topic to confirm the producer is publishing
docker exec -it iot-kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 --topic iot-sensor-data --max-messages 5
```

> If the streaming job dies with `kafka.errors.NoBrokersAvailable`, check `docker compose ps` first — the broker container exiting is a far more common cause than anything in the Spark job.

---

## Running on Windows

Five things bite on Windows. All were hit while building this.

**1. Python 3.11.** pyspark 3.5.x does not support 3.12+:

```powershell
$env:PYSPARK_PYTHON = "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
```

**2. winutils.** Without it `SparkContext` dies the moment `--packages` is used, because Spark shells out to `winutils.exe` to `chmod` the resolved jars:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
    at org.apache.hadoop.util.Shell.getWinUtilsPath
```

Drop `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x into `C:\hadoop\bin` (from [cdarlint/winutils](https://github.com/cdarlint/winutils)), set `HADOOP_HOME=C:\hadoop`, add `C:\hadoop\bin` to PATH.

**3. Timezone.** The JDBC driver forwards the JVM's default timezone to Postgres, and the JDK still reports legacy Olson aliases on Windows (`India Standard Time` → `Asia/Calcutta`), which Postgres 16 rejects:

```
FATAL: invalid value for parameter "TimeZone": "Asia/Calcutta"
```

The job pins the JVM and Spark session to UTC.

**4. Sleep.** Windows sleeping mid-run kills the JDBC socket; the job then dies with `Read timed out` roughly 45 minutes later, which looks like a random crash:

```powershell
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
```

**5. Docker Desktop breaks the test suite.** Docker writes `host.docker.internal` into the Windows hosts file, Java's `getLocalHost()` resolves to it, Spark binds the driver there, and the Python worker cannot connect back — `SocketTimeoutException: Accept timed out`. `spark/conftest.py` pins the driver to `127.0.0.1`.

> **Note:** PowerShell has no `<` input redirection. Use `Get-Content file.sql | docker exec -i ...`.

---

## Tests

```powershell
py -3.11 -m pip install -r spark/requirements.txt
py -3.11 -m pytest spark/ -v
```

```
24 passed in 31.2s
```

| Area | What it guards |
|---|---|
| Shift boundaries | An off-by-one misattributes an entire hour of production to the wrong shift |
| `date_key` / `time_key` | Derivation format — `yyyyMMdd` and minutes since midnight |
| Unknown members | Every surrogate key falls back to `-1`, never NULL |
| DQ flags | Out-of-range temperature thresholds, boundary-exclusive |
| SQL escaping | Postgres error text contains quotes; an unescaped literal turns a logged failure into a second failure |
| JDBC keepalive | Regression guard — removing it reintroduces the sleep crash, which presents hours later rather than as a test failure |

---

## Results

### Dashboard KPIs (4-day window, 6 machines)

| Metric | Value |
|---|---|
| OEE | 50.6% |
| Availability | 62.8% |
| Performance | 83.0% |
| Quality | 97.0% |
| Units produced | 51K |
| Total energy | 146.22K kWh |
| Energy per unit | 2.84 kWh |
| Active alerts | 1,434 |
| Downtime events | 1,318 |
| MTBF | 5.7 min |
| MTTR | 1.4 min |

### Two findings the model surfaced

**Shift matters more than machine.** OEE by shift spans 15 points — Morning 59.6%, Afternoon 46.6%, Night 29.6% — while the six machines sit within 1.3 points of each other. Per-machine and daily views hide this completely. In a real plant the question would be about shift handover, not equipment.

**Energy per unit varies 7× across machines.** The hydraulic press consumes 8.4 kWh per unit against the conveyor's 1.2. It is the most differentiated metric in the dataset and the most obvious cost lever.

---

## Bugs Found and Fixed

Each of these produced a plausible-looking dashboard number that was wrong.

| Bug | Symptom | Root cause | Fix |
|---|---|---|---|
| **Alert grain** | 15M alert rows | v1 wrote an alert per micro-batch in `FAULT`, not per transition into it | Gaps-and-islands dedup in backfill, re-arm window + anti-join in the job, `uq_alert_occurrence` constraint |
| **Downtime grain** | MTBF = 0.01 hours | 4,698 "events" were 1,318 continuous episodes | Episode consolidation; avg episode 1.38 min |
| **Planned minutes** | Night shift OEE = **174.9%** | `MAX(planned_minutes) / 24.0` gave ~20 min planned per hour | Set to 60, plus `MIN(..., 1)` cap on Availability. Overall OEE corrected 61.8% → 50.8% |
| **Card aggregation** | "15M" active alerts | Card showed `SUM(alert_id)`, not a row count | Changed to `COUNTROWS` |
| **DAX row context** | `Performance % = 100.0%` | `AVERAGEX(VALUES(col), other_col)` — `VALUES()` gives row context for one column only | Rewrote as `SUMX` + `CALCULATE` |

An OEE above 100% gets noticed in a review. The two grain bugs do not.

---

## Scope and Limitations

This is a portfolio project at laptop scale. The **modelling** is production-standard; the **operations** deliberately are not.

**Implemented:** CI (lint, tests, SQL parse, integration against a real Postgres), dead-letter queue with Kafka coordinates for replay, container healthchecks and restart policies, a supervisor that restarts the streaming job, and a health-check view set.

**Still not implemented:** monitoring platform and paging, schema registry, high availability (single Kafka broker, replication factor 1, single Postgres), secrets management beyond a `.env` file, disaster recovery.

**Orchestration is optional, deliberately.** The streaming job runs continuously, so there is no task boundary for a scheduler to act on — it needs supervision, not scheduling, and `supervise_streaming.ps1` provides that. The batch work that *does* justify an orchestrator is available two ways: `run_batch_jobs.ps1` for a plain run with lineage in `etl_batch_log`, or `airflow_dags.py` behind the optional compose overlay. Both call the same SQL. The script is enough at this scale; Airflow earns its place once the jobs acquire cross-task dependencies, backfill requirements or an SLA — which is why the DAGs exist but the overlay is opt-in.

**Data caveats:**

- Readings come from a Markov-chain simulator, not real equipment. **MTBF of ~5.7 minutes is an artifact of the simulator's transition probabilities**, not a plausible plant figure.
- `ideal_cycle_time_sec` is derived from observed throughput (90% of best observed), not equipment specifications.
- `dim_date.is_holiday` is unpopulated. `production_line` is derived from `machines.location` with a fallback split, purely so RLS has something to test against.
- Energy cost uses a flat 0.14/kWh; time-of-use pricing would need a `dim_tariff`.
- SCD Type 2 change detection is designed (`row_hash` exists) but the comparison job is not automated.
- Volume is ~290k rows. Nothing here has been stressed at a scale where the design decisions would be tested.

---

## License

MIT License — feel free to fork and build on this.

---

## Author

Built end-to-end as a demonstration of a production-pattern data platform — from streaming ingestion and dimensional modelling through security, benchmarking and BI delivery. The design decisions, the benchmark that failed to support one of them, and the bugs found along the way are all documented rather than smoothed over.
