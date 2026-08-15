# Star vs Snowflake — Design Comparison

Both schemas are implemented in this project and hold identical facts:

- **`mart`** — star schema (`sql/04`–`06`)
- **`snow`** — snowflake schema (`sql/08`)
- **`sql/09_schema_benchmark.sql`** — runs the same questions against both

Having both is deliberate. Knowing the patterns matters less than being able to say *why* you chose one.

---

## 1. The structural difference

### Star

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴──────────────┐    ┌───────────────────┐
│ dim_machine  ├───►│ fact_production_    │◄───┤ dim_shift         │
│              │    │      hourly         │    │                   │
│ machine_name │    └─────────────────────┘    └───────────────────┘
│ machine_type │
│ prod_line    │   All descriptive attributes live on the
│ plant_code   │   dimension. One join from fact to any attribute.
│ manufacturer │
│ criticality  │
└──────────────┘
```

### Snowflake

```
┌──────────────────┐
│ dim_manufacturer │
└────────┬─────────┘
         │
┌────────┴──────────┐     ┌──────────────┐
│ dim_machine_type  │     │  dim_plant   │
└────────┬──────────┘     └──────┬───────┘
         │                       │
         │              ┌────────┴────────────┐
         │              │ dim_production_line │
         │              └────────┬────────────┘
         │                       │
      ┌──┴───────────────────────┴──┐
      │        dim_machine          │   Leaf dimension is thin:
      │  machine_name               │   mostly foreign keys.
      │  machine_type_key   ────────┘
      │  production_line_key
      └──────────────┬──────────────┘
                     │
      ┌──────────────┴──────────────┐
      │  fact_production_hourly     │
      └─────────────────────────────┘
```

Reaching `plant_code` from a fact costs **1 join** in the star and **3** in the snowflake.

---

## 2. Measured comparison

Run `sql/09_schema_benchmark.sql`. It reports `EXPLAIN ANALYZE` plans for three questions plus a storage comparison and an equivalence check.

### Join count

| Question | Star joins | Snowflake joins |
|---|:--:|:--:|
| OEE by machine type and line | 1 | 3 |
| Alerts by severity and owning team | 1 | 3 |
| Energy rollup to plant | 1 | 3 |

Join count is what scales. At 200M fact rows with concurrent dashboard users, three hash joins per query instead of one is a real cost.

### Measured results (140 hourly rows, ~590 alerts, 6 machines)

**Storage — the one clean, repeatable measurement.**

Comparing only the dimensions whose design differs (`mart` needs 2 tables where `snow` needs 8; `dim_date` and `dim_time` are shared and excluded):

| Design | Tables | Storage |
|---|:--:|---|
| mart (star) | 2 | **120 kB** |
| snow (normalised) | 8 | **336 kB** |

Snowflaking costs **2.8× more storage** for identical information. Each table carries page headers, a primary-key index and catalog entries; that fixed overhead dwarfs any saving from de-duplicating a handful of short strings. Normalisation pays only when the repeated attribute is large *and* the dimension has many rows — neither is true here.

**Timings — do not quote these.**

Execution times were unstable across runs of the *same* query on the *same* data:

| | Run 1 | Run 2 |
|---|---|---|
| Q3 snowflake execution | 0.381 ms | **8.246 ms** |
| Q2 star plan | Nested Loop + Memoize | **Hash Join** |

A 21× swing and a plan flip, with nothing changed. At this volume the planner's choices are not stable and timings are dominated by buffer-cache state. Any millisecond figure quoted from this dataset would be noise dressed as evidence.

Planning time did trend higher for snowflake (roughly 1.4–3.6× across runs), which is consistent with a larger plan search space — but the variance is wide enough that it should be described as a direction, not a measurement.

**What the benchmark actually establishes:**

1. Join count: 1 vs 3 for every question tested — structural, not measured, and what scales
2. Storage: snowflake costs 2.8× more here — measured and stable
3. Execution speed: **no reliable difference at this volume** — and snowflake was faster in several runs

The case for the star in this project therefore rests on VertiPaq behaviour, RLS simplicity and DAX readability — **not** on query speed. Claiming a performance win from this data would be wrong.

### Three traps in benchmarking this

All three were hit while building this, each one producing a plausible-looking but wrong result. They are documented because the failure modes generalise.

**Trap 1 — comparing whole schemas for storage.**
`mart` also holds `fact_sensor_reading`: 200k+ rows across monthly partitions that `snow` does not replicate. An unrestricted `pg_total_relation_size` reported *"mart 51 MB vs snow 544 kB"* and looked like a decisive snowflake win. It measured the presence of an unrelated fact table.

**Trap 2 — comparing all dimensions.**
Restricting to `dim%` gave *"mart 1,136 kB / 5,452 rows vs snow 336 kB / 31 rows"*. Still wrong: `mart.dim_date` (4,018 rows) and `mart.dim_time` (1,440) are **shared** — `snow`'s facts reference them directly rather than duplicating them. The comparison was 5,452 rows against 31.

The valid comparison is only the dimensions whose *design* differs: `mart` needs 2 tables (`dim_machine`, `dim_alert_type`) where `snow` needs 8. That is what the benchmark now reports, and it shows snowflake using **more** storage — per-table overhead (page headers, PK indexes, catalog entries) outweighs de-duplicating a few short strings.

**Trap 3 — row-count drift checks miss value drift.**
The obvious guard against a live pipeline is to compare row counts. That reported `rows_added = 0` while totals still differed by 74 units.

The cause: `write_hourly_aggregate` upserts with

```sql
ON CONFLICT DO UPDATE SET units_produced = existing + EXCLUDED
```

so the **current hour's row keeps accumulating**. Row count stays flat; values climb. The equivalence check now compares only hours whose `window_end` has passed, and separately reports rows added, rows whose values changed, and how many hours are still open.

The general lesson: when a fact table uses accumulating upserts rather than append-only inserts, row count is not a validity signal.

---

## 3. Why this project uses the star

**Dimension size.** `dim_machine` holds six rows. Normalising to save storage on a six-row table, at the cost of two extra joins on every query, is a straightforwardly bad trade. The storage section of the benchmark makes this concrete.

**Power BI's engine.** VertiPaq is built around star schemas. A snowflaked model forces relationship chains, and Power BI must traverse each hop to resolve a filter. Microsoft's own guidance is explicit: flatten to star for the semantic layer, whatever shape the warehouse takes.

**Row-level security.** This is the strongest argument in this project. The RLS filter is applied once to `dim_machine`:

```dax
-- one expression secures all four fact tables
CONTAINS ( UserScopes, [p], dim_machine[plant_code], [l], dim_machine[production_line] )
```

Because `dim_machine` sits on the one-side of every fact relationship, the filter cascades automatically. Snowflaked, `plant_code` no longer lives on `dim_machine` — it is two hops away on `dim_plant`. The security predicate must then traverse `dim_plant → dim_production_line → dim_machine → facts`, which means either a more complex DAX expression or bidirectional relationships. Bidirectional filters on a security path are a known source of both performance problems and *leakage*.

**DAX simplicity.** `RELATED()` resolves in one hop from a star. Multi-hop chains need nested `RELATED()` calls or `TREATAS`, and each layer is another thing to get wrong.

---

## 4. When snowflaking is the right call

Not never — the honest position is that it depends on the dimension.

**Genuinely large dimensions with repetitive attributes.** A customer dimension with 50 million rows where each carries a full address block. Normalising address into `dim_address` can save meaningful storage and make address changes a single update rather than 50 million.

**Attributes that change independently and need their own history.** If a production line's supervisor changes, and you want that tracked separately from machine history, a `dim_production_line` with its own SCD2 columns is cleaner than widening `dim_machine`.

**Compliance-driven separation.** PII that must live in a separately-secured table, joined only when authorised.

**Source systems that are already normalised.** If the warehouse is fed from a 3NF operational store and the transformation cost of flattening is high, a snowflaked warehouse with a *flattened semantic layer* on top is a reasonable compromise. This is the common enterprise pattern: snowflake in the warehouse, star in the BI model.

**Ragged or variable-depth hierarchies.** Organisational structures that are not uniformly deep are often easier to model normalised.

None of these apply to a six-machine IoT fleet.

---

## 5. What the snowflake build does add here

Two things worth keeping even though the star remains the reporting model:

**A real hierarchy for drill-down.** `dim_plant → dim_production_line → dim_machine` gives Power BI an explicit path. In the star this hierarchy is implicit in three columns on one table — usable, but the snowflake expresses the parent-child relationship structurally.

**A home for attributes that do not belong on the machine.** `line_supervisor` is a property of the line, not of each machine on it. In the star it would be repeated on every machine row, and updating a supervisor means updating N rows. `owning_team` on `dim_alert_category` is the same shape of problem.

That is the honest, narrow case for snowflaking in this project: not performance, not storage — **update anomaly avoidance on attributes whose natural grain differs from the leaf dimension**.

---

## 6. Summary

| Factor | Star | Snowflake |
|---|---|---|
| Joins to any attribute | 1 | 2–3 |
| Power BI VertiPaq fit | Optimal | Degraded |
| RLS complexity | One expression, cascades | Multi-hop, may need bidirectional |
| DAX `RELATED()` | Direct | Nested / `TREATAS` |
| Storage on small dimensions | Negligible penalty | Negligible saving |
| Update anomalies | Possible on repeated attributes | Avoided |
| Hierarchy expression | Implicit in columns | Explicit in structure |
| Business user comprehension | High | Low |

**Decision for this project: star.** The snowflake is built alongside it as a working comparison, not as the reporting model.

---

## 7. Keeping `snow` in sync

`snow` is a **derived copy** of `mart`, not a second source of truth. It drifts unless refreshed, and the way it drifts is easy to miss.

**The failure mode.** The first version populated `snow` with `ON CONFLICT DO NOTHING` throughout. Re-running inserted new rows but never updated existing ones. Because `fact_production_hourly` **accumulates in place** — the streaming job upserts `units_produced = existing + EXCLUDED` — the current hour's row keeps changing while its `production_hour_id` stays fixed. So `snow` froze at whatever value it saw first, and:

- row counts matched **exactly**
- measures diverged silently

A row-count check reports "in sync" indefinitely. This is worth internalising: **for accumulating facts, row count is not a sync signal.**

**The fix**, applied in `08`:

| Table | Conflict action | Why |
|---|---|---|
| `fact_production_hourly` | `DO UPDATE` all measures | Accumulates in place |
| `fact_alert` | `DO UPDATE resolved_at` | Alerts close after being raised |
| `dim_machine` | `DO UPDATE` attributes | SCD2 rows change |
| `dim_alert_type` | `DO UPDATE` SLA | Thresholds get retuned |
| `dim_plant`, `dim_severity`, etc. | `DO NOTHING` | Natural key *is* the content; nothing to update |

Plus `DELETE` passes to remove orphans — needed because the downtime-grain fix `TRUNCATE`d and rebuilt a fact table, which would otherwise leave `snow` holding rows `mart` no longer has.

**Refreshing:**

```sql
SELECT * FROM snow.refresh_from_star();
```

Returns per-table row counts and an `IN SYNC` / `DRIFT` verdict. Every run is logged to `snow.refresh_log`.

**Checking staleness before trusting a query:**

```sql
SELECT * FROM snow.v_staleness;
```

The column that matters is `rows_with_stale_values`, not `hourly_rows_behind` — for the reason above.

---

## 8. Running it

```powershell
Get-Content sql\08_snowflake_schema.sql | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
Get-Content sql\09_schema_benchmark.sql | docker exec -i iot-postgres psql -U iot_user -d iot_dashboard
```

Both are idempotent. If the pipeline is running, re-run `08` (or call `snow.refresh_from_star()`) immediately before `09`, otherwise the benchmark compares a live star against a stale snowflake.

The benchmark's equivalence check compares only **closed hours** for this reason. A `MISMATCH` there means the copy logic in `08` is genuinely wrong, not that time has passed.
