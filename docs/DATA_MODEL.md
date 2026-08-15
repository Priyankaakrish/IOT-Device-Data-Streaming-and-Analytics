# Data Model Design — IoT Manufacturing Analytics

**Status:** Approved for implementation
**Owner:** Data Engineering
**Consumers:** Power BI semantic model, ad-hoc SQL, downstream ML

---

## 1. Decision: Star Schema (not Snowflake)

### The choice

We implement a **Kimball star schema**: denormalised dimensions joined directly to fact tables, one join hop from any fact to any descriptive attribute.

### Why not snowflake

Snowflake schemas normalise dimensions into sub-dimensions (`dim_machine` → `dim_machine_type` → `dim_manufacturer`). This is the wrong trade for an analytics workload:

| Factor | Star | Snowflake |
|---|---|---|
| Joins per query | 1 per dimension | 2–4 per dimension chain |
| Power BI VertiPaq performance | Optimal — engine is built for star | Degraded; multi-hop relationship chains |
| DAX complexity | Direct `RELATED()` | Requires traversal or bridge logic |
| DirectQuery SQL generated | Flat, index-friendly | Nested joins, worse plan stability |
| Storage saved | — | Marginal (dimensions are tiny) |
| Business user comprehension | High | Low |

Our largest dimension (`dim_machine`) will hold tens of rows even at full plant scale. Normalising a 50-row table to save kilobytes, at the cost of query performance across a 200M-row fact, is a net loss.

**Where snowflaking is justified** (and we deliberately avoid it here): a dimension with millions of rows and a genuinely repetitive high-cardinality attribute — e.g. a customer dimension carrying full address history. Not our case.

### The one deliberate exception

`dim_date` and `dim_time` are kept as **separate** dimensions rather than a single datetime dimension. This is standard Kimball practice: combining them would produce a 3.15M-row dimension per decade (365 × 10 × 86400). Splitting gives 3,650 date rows + 1,440 minute rows.

---

## 2. Bus Matrix

Rows are business processes; columns are conformed dimensions. A conformed dimension means `dim_machine` has identical keys and meaning across every fact — enabling drill-across queries.

| Business Process | Grain | Date | Time | Machine | Shift | Status | Alert Type |
|---|---|:--:|:--:|:--:|:--:|:--:|:--:|
| Sensor telemetry | 1 row per machine per reading | X | X | X | X | X | |
| Hourly production | 1 row per machine per hour | X | X | X | X | | |
| Alert lifecycle | 1 row per alert occurrence | X | X | X | X | | X |
| Downtime episode | 1 row per downtime event | X | X | X | X | X | |

---

## 3. Grain Declarations

Grain is declared before columns are chosen. Every fact row must mean exactly one thing.

### `fact_sensor_reading`
> One row per machine per telemetry emission.

Atomic grain. Never aggregate on write — aggregation is a query-time or ETL-derived concern. Retention: 90 days hot, then roll to `fact_production_hourly` and archive.

### `fact_production_hourly`
> One row per machine per clock hour.

Derived from the atomic fact by the Spark job's windowed aggregation. This is the table Power BI hits for trend visuals; it is ~720 rows/machine/month versus ~518,000 at atomic grain.

### `fact_alert`
> One row per alert **occurrence** — i.e. per state transition into an alerting condition.

Critical: this is a **transition-grained** fact, not a state-sampled one. The original implementation wrote a row every micro-batch while a machine remained in FAULT, producing 15M rows from a handful of real events. See `06_migration_backfill.sql` for the deduplication logic.

### `fact_downtime`
> One row per continuous downtime episode, closed when the machine returns to a productive state.

Accumulating snapshot pattern: the row is inserted on downtime start with `end_time` NULL, then updated on recovery. `duration_minutes` is a computed column.

---

## 4. Dimension Specifications

### `dim_machine` — Slowly Changing Dimension Type 2

Machines get recommissioned, moved between lines, and re-rated. Type 2 preserves history so that a reading from March is attributed to the line the machine was on *in March*, not where it sits today.

| Column | Type | Notes |
|---|---|---|
| `machine_key` | BIGINT | Surrogate PK. Meaningless integer. |
| `machine_id` | VARCHAR(20) | Natural/business key, e.g. `M-101` |
| `machine_name` | VARCHAR(100) | `CNC Mill 2` |
| `machine_type` | VARCHAR(50) | `CNC`, `Conveyor`, `Press`, `Robot` |
| `production_line` | VARCHAR(50) | Used for RLS scoping |
| `plant_code` | VARCHAR(20) | Used for RLS scoping |
| `manufacturer` | VARCHAR(100) | |
| `install_date` | DATE | |
| `ideal_cycle_time_sec` | NUMERIC(10,3) | **Required for OEE Performance** |
| `rated_power_kw` | NUMERIC(10,3) | Baseline for energy anomaly detection |
| `criticality` | VARCHAR(10) | `HIGH`/`MEDIUM`/`LOW` — drives alert SLA |
| `valid_from` | TIMESTAMPTZ | SCD2 effective start |
| `valid_to` | TIMESTAMPTZ | SCD2 effective end, `9999-12-31` if current |
| `is_current` | BOOLEAN | Filter target for "as-is" reporting |
| `row_hash` | CHAR(64) | SHA-256 of tracked attributes; drives change detection |

**Surrogate keys are mandatory here.** Without them, a Type 2 change would either overwrite history or create a duplicate natural key that breaks the 1:many relationship in Power BI.

### `dim_date` — Role-playing dimension

Pre-generated 2024–2034. Includes fiscal calendar (assumed April start; adjust to your org), ISO week, and manufacturing-specific flags.

Role-playing: the same physical table serves `event_date`, `alert_raised_date`, and `downtime_start_date`. In Power BI, create one active relationship and use `USERELATIONSHIP()` in DAX for the inactive ones — do **not** duplicate the table unless users need independent slicers.

### `dim_time`
Minute grain (1,440 rows). Carries `shift_key` so shift analysis needs no time-range predicate.

### `dim_shift`
Three shifts (Morning/Afternoon/Night) plus an `UNASSIGNED` member. Shift boundaries are data, not hardcoded logic — changing a shift pattern is an UPDATE, not a code deploy.

### `dim_machine_status`
Decodes `RUNNING`/`IDLE`/`FAULT`/`MAINTENANCE` and, importantly, carries `is_productive` and `is_planned_downtime` booleans. OEE Availability depends on distinguishing *planned* from *unplanned* stoppage — a distinction the raw status string does not make.

### `dim_alert_type`
Carries `sla_response_minutes` per severity, enabling MTTA-versus-SLA measures.

---

## 5. Junk and Degenerate Dimensions

- **Degenerate:** `batch_id` lives on `fact_production_hourly` with no dimension table. It is an identifier with no attributes — creating a dimension for it would be a single-column table joined for nothing.
- **Junk dimension** `dim_reading_flags`: collapses low-cardinality boolean flags (`is_outlier`, `is_backfilled`, `is_estimated`) into one small dimension instead of three columns on a 200M-row fact. Saves fact width; keeps flags filterable.

---

## 6. Handling Late-Arriving and Missing Data

| Scenario | Handling |
|---|---|
| Reading arrives for unknown machine | Insert inferred member into `dim_machine` with `is_inferred = TRUE`; do not drop the fact |
| Null dimension reference | Point to reserved key `-1` (`UNKNOWN`) — never NULL foreign keys |
| Event timestamp before dimension `valid_from` | Assign to earliest version; log to `dq_exceptions` |
| Duplicate telemetry (Kafka at-least-once) | Dedup on `(machine_key, event_ts, source_offset)` unique constraint, `ON CONFLICT DO NOTHING` |

The reserved `-1` "Unknown" member in every dimension is non-negotiable. Inner joins silently drop rows with NULL keys, which turns a data-quality problem into a *silent under-reporting* problem — the worst failure mode in analytics.

---

## 7. Physical Design

- **Partitioning:** `fact_sensor_reading` partitioned by RANGE on `event_date` (monthly). Enables partition pruning and cheap archival via `DETACH PARTITION`.
- **Indexes:** BRIN on `event_ts` (naturally ordered, tiny index); B-tree on `machine_key` for dimension pushdown.
- **Clustering:** facts physically ordered by `(event_date, machine_key)` to align with the dominant query pattern.
- **Compression:** default; consider TimescaleDB hypertables if telemetry volume exceeds ~500M rows.

---

## 8. Entity Relationship Overview

```
                    ┌──────────────┐
                    │   dim_date   │
                    └──────┬───────┘
                           │
       ┌───────────────────┼───────────────────┬──────────────────┐
       │                   │                   │                  │
┌──────┴─────────┐  ┌──────┴──────────┐  ┌────┴──────┐  ┌────────┴────────┐
│ fact_sensor_   │  │ fact_production │  │fact_alert │  │  fact_downtime  │
│    reading     │  │    _hourly      │  │           │  │                 │
└──────┬─────────┘  └──────┬──────────┘  └────┬──────┘  └────────┬────────┘
       │                   │                   │                  │
       └───────────────────┼───────────────────┴──────────────────┘
                           │
       ┌───────────┬───────┼────────┬────────────────┐
       │           │       │        │                │
┌──────┴─────┐ ┌───┴────┐ ┌┴──────┐ ┌──────────────┐ ┌──────────────────┐
│dim_machine │ │dim_time│ │dim_   │ │dim_machine_  │ │ dim_alert_type   │
│  (SCD2)    │ │        │ │shift  │ │   status     │ │                  │
└────────────┘ └────────┘ └───────┘ └──────────────┘ └──────────────────┘
```

All relationships are **1 (dimension) → many (fact)**, single-direction filter flow. No bidirectional relationships except the RLS security bridge, which is documented separately in `07_row_level_security.sql`.

---

## 9. What Reviewers Will Look For

If this project is being read by a hiring manager, these are the signals that distinguish it from a tutorial build:

1. **Declared grain** before columns — shows Kimball literacy
2. **Surrogate keys + SCD2** — shows understanding of history preservation
3. **Unknown member handling** — shows production instincts about silent data loss
4. **Separate date/time dimensions** — shows awareness of cardinality consequences
5. **Transition-grained alerts** — shows the ability to spot and fix a semantic modelling bug
6. **A justified *rejection*** of snowflaking — shows judgement, not cargo-culting

---

*Next: `sql/04_star_schema.sql` for the DDL implementing this design.*
