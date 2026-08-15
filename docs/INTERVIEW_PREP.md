# Interview Preparation — IoT Manufacturing Analytics Pipeline

Questions an interviewer is likely to ask about this project, with answers grounded in what was actually built. Ordered roughly by how often they come up.

**The single most important rule:** every answer below is defensible because it is true of this codebase. Do not upgrade any of them. If you claim production operations, cloud deployment or real sensor data, the follow-up question has no good answer.

---

## Contents

- [1. Opening and framing](#1-opening-and-framing)
- [2. Dimensional modelling](#2-dimensional-modelling)
- [3. Kafka](#3-kafka)
- [4. Spark Structured Streaming](#4-spark-structured-streaming)
- [5. PostgreSQL and the warehouse](#5-postgresql-and-the-warehouse)
- [6. Row-level security](#6-row-level-security)
- [7. Airflow and orchestration](#7-airflow-and-orchestration)
- [8. Data quality](#8-data-quality)
- [9. Power BI and DAX](#9-power-bi-and-dax)
- [10. Testing, CI and Docker](#10-testing-ci-and-docker)
- [11. Scale and production readiness](#11-scale-and-production-readiness)
- [12. Trap questions](#12-trap-questions)
- [13. Questions to ask them](#13-questions-to-ask-them)

---

## 1. Opening and framing

### Q: Walk me through this project.

Keep it to 60 seconds. Structure: problem → architecture → what was hard → outcome.

> "It's a real-time analytics platform for a factory floor. Six machines emit telemetry once a second into Kafka. A Spark Structured Streaming job conforms that against a Kimball star schema in PostgreSQL — resolving surrogate keys, shift attribution and data-quality flags before anything is written. Power BI reports OEE off it over DirectQuery, and Airflow runs the batch and data-quality layer.
>
> The pipeline was the easy part. The interesting work was the modelling: declaring grain per fact table and enforcing it with constraints. That's how I found three bugs that were producing plausible but wrong dashboard numbers — including an alert table that had grown to 15 million rows representing about 250 real events."

Then stop. Let them pick where to go.

### Q: What was the hardest part?

> "Realising that a dashboard number looking reasonable is not evidence it's correct. Night shift OEE showed 174.9%, which is impossible by construction, so that got caught. But the two grain bugs — alerts and downtime — produced figures inside plausible ranges and were only found by reconciling counts back to the source. That changed how I build: grain is now a database constraint, not a convention."

### Q: What would you build differently if you started again?

> "I'd declare grain and write the constraint before writing the streaming job, not after. The v1 job had no grain enforcement, and that's the direct cause of the 15 million alert rows. Everything else followed from getting that order wrong."

---

## 2. Dimensional modelling

### Q: What is grain, and why does it matter?

> "Grain is what one row means. Declaring it constrains which measures are legitimate — anything not true at that grain belongs in a different table. In this project every fact table has a declared grain and a unique constraint enforcing it, so a regression fails at write time instead of silently inflating a measure."

### Q: Why a star schema and not just query the normalised tables?

Three reasons, in this order — the order matters because it shows you know which argument is strongest.

> "First, security: one RLS policy on `dim_machine` cascades to all four fact tables, because the dimension sits on the one-side of every relationship. Second, comprehension: a declared grain per fact is a testable statement — it's how the bugs were found. Third, engine fit: Power BI's storage engine is built around star schemas, and a snowflaked model forces relationship chains it has to traverse per filter.
>
> Query speed is deliberately not on that list — my own benchmark didn't support it."

### Q: Explain SCD Type 2. When would you *not* use it?

> "Type 2 inserts a new dimension row on change and closes the prior version, so facts keep referencing the version current when they occurred. History is preserved.
>
> Don't use it when the change is a correction rather than a change. A misspelled machine name should be overwritten in place — Type 1 — because preserving the typo as history isn't useful."

### Q: Why surrogate keys instead of natural keys?

> "Three reasons. They let SCD2 work — the natural key stays stable while the surrogate changes per version. They're narrow integers, so joins and indexes are cheaper. And they insulate the warehouse from source-system changes: if the machine ID scheme changes, the facts don't need rewriting."

### Q: Why is the Unknown member `-1` rather than NULL?

This one separates people who've read about modelling from people who've done it.

> "A NULL key violates the fact table's NOT NULL constraint and kills the batch — noisy but recoverable. The dangerous case is if a NULL got through: the row would sit in the fact table but fall out of every inner join, so totals would be quietly understated with no error anywhere. Routing to `-1` keeps the row countable and surfaces the gap as an Unknown member on the report, where someone will ask about it."

### Q: What's a conformed dimension?

> "A dimension shared across multiple facts with identical keys and meaning. Here `dim_date`, `dim_time`, `dim_machine` and `dim_shift` are conformed across all four facts. That's what makes a filter behave consistently — selecting a production line constrains sensor readings, production, alerts and downtime identically."

### Q: Junk dimension? Degenerate dimension?

> "A junk dimension collapses low-cardinality flags into one table rather than adding boolean columns to a fact — `dim_reading_flags` does that for data-quality state. A degenerate dimension is a dimensional attribute stored on the fact with no dimension table of its own, typically a transaction identifier."

### Q: Fact table types?

> "Transaction — one row per event, `fact_sensor_reading` and `fact_alert`. Periodic snapshot — a measurement at regular intervals. Accumulating snapshot — a row updated as a process progresses, which is `fact_downtime`: inserted on the transition into a down state and updated on recovery."

---

## 3. Kafka

### Q: Why Kafka rather than writing straight to the database?

> "Decoupling and replay. The producer emits at a fixed rate; the consumer processes on a 30-second trigger. Kafka absorbs that mismatch. And because offsets are retained, a failed batch can be reprocessed — writing straight to the database means a consumer outage loses data permanently."

### Q: Explain the two advertised listeners.

The most common Kafka-in-Docker question, and this project has a concrete answer.

> "A broker tells clients where to reach it via `advertised.listeners`. Clients inside the Compose network resolve `kafka:29092`; the producer and Spark run on the host and resolve `localhost:9092`. With a single listener, one of the two gets an address it can't reach — and the failure shows up as a connection timeout in the client, not as an error in the broker."

### Q: How would you scale this?

> "Partition the topic by machine ID. That preserves per-machine ordering — Kafka only guarantees ordering within a partition — while allowing parallel consumption. Then raise Spark's shuffle partitions from the current four. The dimensional model doesn't change, which is the point of conforming upstream."

### Q: What's your replication factor, and is that OK?

Be honest here; the honesty is the answer.

> "One, because there's a single broker. That's a development topology, not a production one — a broker loss means data loss. Production would need at least three brokers with replication factor three and `min.insync.replicas` of two."

---

## 4. Spark Structured Streaming

### Q: How do you achieve exactly-once?

> "You don't achieve it in transport — Structured Streaming gives at-least-once to a `foreachBatch` sink. You achieve it at the sink by making writes idempotent. The hourly aggregate goes through a staging table and merges with `ON CONFLICT` on the declared grain, so reprocessing a batch converges to the same state. The property that matters is idempotency, not exactly-once delivery."

### Q: What does a watermark do?

> "It bounds state. With a ten-minute watermark, the engine can discard state for windows more than ten minutes behind the maximum observed event time. Without it, state grows without limit. The trade is that anything arriving later than the watermark is dropped."

### Q: Why one `foreachBatch` instead of three `writeStream` queries?

> "Resource pressure. The v1 job ran three concurrent queries, each spawning its own Python worker set. Under sustained load that exhausted worker connections on the host and the job died with socket-connect failures. One `foreachBatch` over a persisted DataFrame does the same work with one set of workers, and the batch is computed once rather than three times."

### Q: Why persist `temp_sum` alongside `avg_temperature_c`?

> "So re-aggregation is correct. Averaging previously stored averages weights each batch equally regardless of how many readings it contained. Carrying the sum and the count lets the merge compute a true weighted average."

### Q: What is a broadcast join and why use it here?

> "The dimensions are tiny — six machines, seven statuses. Broadcasting sends a copy to every executor so the join needs no shuffle. They're also cached once per job rather than read per micro-batch, which was a large part of the v1 job's per-batch cost."

### Q: What happens on restart?

> "It resumes from the checkpoint at the last committed offset. Because the sinks are idempotent, reprocessing produces the same state. That's also why the supervisor script can restart it safely without any manual intervention."

---

## 5. PostgreSQL and the warehouse

### Q: Why partition `fact_sensor_reading`?

> "The access pattern is almost entirely recent-range scans, so RANGE partitioning by month makes pruning effective. It also makes retention a detach operation instead of a mass delete — dropping a partition is a catalogue change; deleting thirty million rows is hours of I/O and leaves a bloated table until vacuumed."

### Q: Why BRIN and not B-tree on the timestamp?

> "BRIN stores min/max per block range rather than an entry per row. Because rows arrive in timestamp order, the ranges are tight and the index is a fraction of a B-tree's size. B-tree would work but costs far more space for the same pruning on naturally ordered data."

### Q: What's `ON CONFLICT DO UPDATE` doing?

> "It's an upsert. When the insert violates the grain constraint, it updates the existing row instead — and here the update is additive, so the hourly fact accumulates across batches rather than being replaced."

### Q: What's the difference between `GENERATED ALWAYS` and `GENERATED BY DEFAULT` identity?

> "`ALWAYS` rejects an explicit value unless you use `OVERRIDING SYSTEM VALUE`. `BY DEFAULT` accepts one. The facts use `BY DEFAULT` so the migration could carry legacy identifiers across."

---

## 6. Row-level security

### Q: Why enforce security in the database rather than the report?

> "A report filter protects only the path it's applied to. Anyone connecting with a SQL client bypasses it entirely. A database policy holds regardless of how the data is reached."

### Q: How does it work here?

> "A `SECURITY DEFINER` function reads a session variable and resolves it to a permitted scope. Policies on `dim_machine` consult that function. Because the dimension is on the one-side of every fact relationship, the restriction propagates to all four facts without a policy on each."

### Q: Why is the session variable named `app.user_upn`?

A small detail that shows you actually ran this.

> "Because `current_user` is a reserved keyword in PostgreSQL. `SET app.current_user = '...'` fails to parse. It took a syntax error to find that."

### Q: How did you test it?

> "Four access tiers. A Line A operator sees four machines, a plant manager six, an executive six, and an unmapped identity sees one — the Unknown member. That last case matters most: default-deny returns an empty-looking report rather than an error, which is a state someone will report rather than mistake for a quiet day."

---

## 7. Airflow and orchestration

### Q: Why doesn't Airflow orchestrate your Spark job?

The question most likely to catch someone out. Your answer is strong.

> "Because it can't, and trying would be a design error. A scheduler starts a task, waits for it to terminate, and reads an exit code. A Structured Streaming query is designed never to terminate — there's no completion event to wait on. What it needs is a supervisor that notices process death and restarts it from the checkpoint. Airflow orchestrates the finite periodic work, where dependencies and schedules genuinely exist."

### Q: What's in your DAGs?

> "Three. `iot_platform_daily` runs batch/ETL and data quality in parallel and converges them on a warehouse-ready gate with `all_success`, so nothing downstream runs unless both branches passed. `iot_data_quality` runs the five gates hourly between daily runs. `iot_retention` handles partition drops and ships paused, because it's irreversible."

### Q: Why does the convergence gate matter?

> "It's the reason this is a DAG rather than a shell script running commands in order. A stale derived copy or a breached quality threshold stops the warehouse being marked fit to report on. Sequential execution can't express that."

### Q: What's the difference between `retries` and `trigger_rule`?

> "`retries` is per-task — how many times to re-attempt on failure. `trigger_rule` governs whether a task runs at all based on upstream state. Default is `all_success`; the warehouse gate uses that explicitly. `all_done` would run regardless of upstream failure, which would defeat the point here."

### Q: What's a TaskGroup?

> "A visual and logical grouping of tasks in one DAG. It's not a SubDAG — no separate scheduling, no separate executor. The batch and quality branches are TaskGroups so the graph view reads as two branches rather than eleven loose tasks."

### Q: Why LocalExecutor and not Celery?

> "Celery adds a message broker and worker fleet to distribute tasks across machines. With three DAGs on one host there's nothing to distribute. LocalExecutor runs tasks as subprocesses in parallel, which is what's actually needed here."

---

## 8. Data quality

### Q: Tell me about a bug you found.

Have all three ready; lead with the alert grain one.

> "The alert table had grown to about 15 million rows. Two separate faults compounded: a Power BI card was summing `alert_id` rather than counting rows, so it displayed '15M' when there were 5,385 rows — and separately, those 5,385 rows represented roughly 250 real events, because v1 wrote an alert on every micro-batch a machine spent in FAULT rather than once per transition into it.
>
> Fixed in three places: gaps-and-islands consolidation in the backfill, a re-arm window and anti-join in the streaming job, and a unique constraint so it can't silently return."

### Q: What's gaps-and-islands?

> "A pattern for collapsing consecutive rows into episodes. You use `LAG` to find where the gap from the previous row exceeds a threshold, mark those as episode starts, and group on a running sum of the start markers. Used here twice — for alert occurrences and for downtime episodes."

### Q: Why do your quality checks fail the DAG instead of logging?

> "Because a warning in a log is a warning nobody reads. If freshness has breached, the correct state of the pipeline is red, and everything downstream of the warehouse gate should not run. Reporting the problem and continuing publishes a dashboard that looks authoritative and is stale."

### Q: How do you know the migration was correct?

> "The migration script runs inside a transaction and ends with a reconciliation query, with the COMMIT commented out deliberately. You confirm target sensor rows equal source rows, that nothing resolved to the Unknown member, and that alert rows fell by more than 99% — and only then commit."

### Q: Your row-count check said zero drift but the data was wrong. Explain.

> "Because the hourly fact accumulates in place. The streaming job upserts additively, so the current hour's row keeps changing while its identity stays fixed — row count stayed flat while values diverged by 74 units. For accumulating facts, row count isn't a validity signal. The equivalence check now compares only hours that have closed."

---

## 9. Power BI and DAX

### Q: Import, DirectQuery or Composite — and why?

> "Composite. Dimensions are imported because they're small and change rarely. Sensor facts stay in DirectQuery so the operational view reflects current state. The hourly aggregate is Dual, so summary queries can be served from memory while the detail path stays live."

### Q: What's the difference between a calculated column and a measure?

> "A calculated column is evaluated at refresh, stored in the model, and consumes memory per row. A measure is evaluated at query time in the current filter context and stores nothing. Prefer measures — a calculated column on a 290,000-row fact is 290,000 stored values that a measure computes on demand."

### Q: Explain row context versus filter context.

> "Filter context is the set of filters applied to a calculation — from slicers, visual axes, or CALCULATE. Row context is the current row inside an iterator. They're separate, which is why an iterator over `VALUES()` on a single column gives row context for that column only.
>
> That bit me: `AVERAGEX(VALUES(dim_machine[machine_key]), dim_machine[ideal_cycle_time_sec])` pinned Performance at exactly 100%, because the cycle-time column wasn't reachable in that row context. Rewrote it as `SUMX` plus `CALCULATE`."

### Q: What does CALCULATE do?

> "It evaluates an expression in a modified filter context, and it performs context transition — converting row context into filter context. That transition is what makes `SUMX` + `CALCULATE` work where the naive iterator failed."

### Q: How is OEE calculated?

> "Availability × Performance × Quality. Availability is runtime over planned time, Performance is theoretical minimum time over actual runtime, Quality is good units over total. Availability is capped at 1 — a cap is a guard, not a fix. Night shift showed 174.9% because `planned_minutes` was being derived as a daily figure divided by 24, giving about 20 minutes of planned time per hour."

---

## 10. Testing, CI and Docker

### Q: What do your tests cover, and what don't they?

> "24 unit tests on key derivation — shift boundaries, date and time keys, unknown-member fallback, data-quality flag thresholds, SQL literal escaping. They deliberately don't cover the JDBC writers; that needs a live database and belongs in an integration suite. The CI integration job covers that separately by applying the full schema to a real PostgreSQL service."

### Q: Why test shift boundaries specifically?

> "Because an off-by-one misattributes a full hour of production to the wrong shift, and the shift comparison is the most actionable finding in the whole dataset — a 30-point OEE spread. Seven boundary cases are covered."

### Q: You have a test that only checks three config values. Why?

> "It's a regression guard. The original job died with a read timeout whenever the host suspended — the JDBC socket went stale and nothing detected it. The failure surfaced about 45 minutes after the actual cause, so it looked like an unrelated crash. Removing those keepalive properties would reintroduce a fault that presents hours later rather than as a test failure. That's exactly the kind of thing that warrants a test."

### Q: What does your CI do?

> "Four jobs. Lint with ruff. The 24 tests with a JDK. Every SQL script parsed through `pglast`, which wraps libpg_query — the actual PostgreSQL parser. And an integration job that spins up PostgreSQL 16, applies the full schema, and asserts dimensions seeded and Unknown members present. A script that parses isn't necessarily a script that runs."

### Q: Why is Postgres on 5433?

> "So it can't collide with a local PostgreSQL install on 5432. A collision connects successfully to the wrong database, which presents as missing data rather than as a connection error — much harder to diagnose."

---

## 11. Scale and production readiness

### Q: Is this production-ready?

Do not oversell. The honest answer is stronger.

> "The modelling, security and data-quality layers are production-standard. The operations aren't, and can't be on a single host — one Kafka broker, one Postgres, no HA, no paging, and the source data is simulated. I'd describe it as production-pattern at laptop scale."

### Q: What would it take to make it production?

> "Managed Kafka with three brokers minimum, managed Postgres with replication and point-in-time recovery, Spark on a managed cluster, Power BI Service with a gateway, a schema registry so the payload contract is enforced at the broker, secrets in a vault rather than a `.env`, and paging so a failed quality gate reaches a person."

### Q: Where does it break first at scale?

> "Single-node PostgreSQL write throughput on the sensor fact. The Kafka side scales by adding partitions and the Spark side by adding executors, but every row still lands in one database. That's the first thing I'd move."

### Q: Your MTBF is 5.7 minutes. Is that realistic?

They're testing whether you understand your own data.

> "No — it's an artifact of the simulator's Markov transition probabilities, not plausible equipment behaviour. The pipeline computing it is correct; the input is synthetic. That's stated in the README and the architecture document, because a reliability figure that isn't caveated is misleading."

---

## 12. Trap questions

### Q: You used Snowflake?

> "No — a snowflake *schema*, which is a normalised dimensional design, implemented in PostgreSQL as a comparison against the star. Not Snowflake the cloud warehouse."

Worth pre-empting rather than being caught by. It's an easy misread of the README.

### Q: Your benchmark didn't support your design decision. Why keep the design?

> "Because query speed was never the strongest argument, and the benchmark measured the weakest one. The star was chosen for security cascade, engine fit and reviewability. Publishing the negative result is the point — a benchmark that only ever confirms the author's decision isn't a benchmark."

### Q: Isn't this over-engineered for six machines?

> "For six machines, yes — a flat table would report OEE fine. The design is sized for the failure modes rather than the row count, and those showed up anyway: the alert table hit 15 million rows at six machines. The modelling is what made that detectable."

### Q: How much of this did you write yourself?

Answer honestly. What you can always defend is the reasoning.

> "I built it end to end and used documentation and AI assistance along the way, like I would at work. What I can talk through is why each decision was made — why grain is enforced at the database layer, why the surrogate key falls back to `-1`, why the benchmark didn't support the design."

### Q: Show me the part you're least happy with.

> "SCD2 change detection. The `row_hash` column exists and the Airflow task reports drift, but nothing creates the new version — that's still manual. I stopped short deliberately, because a job that silently rewrites dimension history is worse than a manual step, but it's incomplete either way."

---

## 13. Questions to ask them

Have three ready. These signal you think about the same problems.

- "How do you handle late-arriving data in your streaming pipelines — what's your watermark policy?"
- "Where is grain enforced in your warehouse — convention, code review, or database constraints?"
- "What does your on-call look like for data pipelines, and what actually pages someone?"
- "How do you decide when something moves from a scheduled script to an orchestrated DAG?"

---

## Rapid-fire glossary

| Term | One-line answer |
|---|---|
| Grain | What one row of a fact table means |
| Conformed dimension | Shared across facts with identical keys and meaning |
| SCD Type 2 | New row on change, prior version closed; history preserved |
| Surrogate key | Warehouse-generated key; enables SCD2 and insulates from source changes |
| Degenerate dimension | Dimensional attribute stored on the fact, no dimension table |
| Junk dimension | Low-cardinality flags collapsed into one table |
| Idempotency | Reprocessing produces the same state |
| Watermark | Bound on how late data can arrive before state is discarded |
| Backpressure | Limiting input rate so the consumer isn't overwhelmed |
| Dead-letter queue | Quarantine for messages that fail the contract |
| Context transition | CALCULATE converting row context into filter context |
| BRIN | Block-range index; small, effective on naturally ordered data |
