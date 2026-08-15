# As-Built Dashboard Documentation

**File:** `IOT Dashboard.pbix`
**Connection:** PostgreSQL `localhost:5433` → database `iot_dashboard`
**Storage mode:** DirectQuery (all tables)
**Pages:** 3
**Data volume at time of documentation:** 212,108 rows in `sensor_readings`, latest reading `2026-08-08 15:52:59`

This document records the dashboard exactly as it exists today. The target-state design is in `POWERBI_SETUP.md`; the differences between the two are listed in section 6.

---

## 1. Data source

Eight PostgreSQL views in the `public` schema, connected via the PostgreSQL connector in DirectQuery mode. No Power Query transformations are applied — all shaping happens in the view definitions.

| View | Grain | Feeds |
|---|---|---|
| `vw_live_machine_status` | 1 row per machine (current state) | Live Machine Status table |
| `vw_avg_temperature` | 1 row (scalar) | Average Temperature card |
| `vw_failure_alerts` | 1 row per alert record | Failure Alerts table |
| `vw_machine_utilization` | 1 row per machine | Machine Utilization chart |
| `vw_power_consumption` | 1 row per machine per day | Power Consumption chart |
| `vw_downtime` | 1 row per machine | Downtime chart |
| `vw_predictive_maintenance` | 1 row per machine | Predictive Maintenance table |
| `vw_oee` | 1 row per machine | OEE cards and breakdown chart |

**Note on DirectQuery:** because no data is imported, the Refresh button re-queries Postgres live. The "Data" refresh option in the Refresh dropdown is greyed out by design — only "Schema" is available, and only needs running when view definitions change.

---

## 2. Page 1 — Executive Overview

### Visual inventory

| # | Visual type | Title | Field(s) | Aggregation | Source view |
|---|---|---|---|---|---|
| 1 | Card | Average Temperature (24h) | `avg_temp_last_24h` | **Average** | `vw_avg_temperature` |
| 2 | Card | Active Alerts | `alert_id` | Count | `vw_failure_alerts` |
| 3 | Card | Overall OEE % | `oee_pct` | **Average** | `vw_oee` |
| 4 | Clustered column | Average of availability_pct, performance_pct, quality_pct and oee_pct by machine_id | X: `machine_id`<br>Y: `availability_pct`, `performance_pct`, `quality_pct`, `oee_pct` | **Average** (all four) | `vw_oee` |
| 5 | Table | *(untitled)* | `machine_id`, `maintenance_risk`, `temp_1h`, `temp_drift`, `vib_1h`, `vib_drift` | Don't summarize | `vw_predictive_maintenance` |
| 6 | Table | *(untitled)* | `event_time`, `machine_id`, `machine_name`, `alert_type`, `severity`, `message`, `acknowledged` | Don't summarize | `vw_failure_alerts` |
| 7 | Table | *(untitled)* | `machine_id`, `machine_name`, `status`, `temperature_c` | Don't summarize | `vw_live_machine_status` |

### Filters

- **Page-level filter:** `acknowledged` **is False** — applied so the Active Alerts card counts only unacknowledged alerts
- **Visual-level filter:** `machine_name` is (All) on the Live Machine Status table

### Current values observed

| Metric | Value |
|---|---|
| Average Temperature (24h) | 60.51–60.53 °C |
| Active Alerts | 15M |
| Overall OEE % | 58.88% |
| Machines reporting | 5 (M-101 … M-105) |
| Predictive maintenance risk | HIGH_RISK on all 5 machines |

### Known defects on this page

1. **Active Alerts reads 15M.** This is not a Power BI problem — the upstream Spark job writes an alert row on every micro-batch while a machine remains in FAULT, rather than once per state transition. The card is faithfully counting bad data. Fix is in `sql/06_migration_backfill.sql` and `spark/streaming_job_star.py`.
2. **Three tables are untitled.** Power BI shows no header, so a reader cannot tell what they contain without inspecting fields.
3. **Chart title is auto-generated** — "Average of availability_pct, Average of performance_pct, Average of quality_pct and Average of oee_pct by machine_id" is the default string, not a written title.
4. **Predictive maintenance shows HIGH_RISK for every machine**, which makes the signal non-discriminating. Threshold tuning needed in the view.
5. **Page carries 7 visuals** — dense for an executive summary.

---

## 3. Page 2 — Production Performance

### Visual inventory

| # | Visual type | Title | Field(s) | Aggregation | Source view |
|---|---|---|---|---|---|
| 1 | Text box | Operations Dashboard | — | — | — |
| 2 | Line chart | Power Consumption (kWh) | X: `production_date`<br>Y: `total_kwh`<br>Legend: `machine_id` | Sum | `vw_power_consumption` |
| 3 | Clustered bar | Machine Utilization % | Y: `machine_id`<br>X: `utilization_pct` | **Average** | `vw_machine_utilization` |
| 4 | Clustered bar | Downtime (minutes) | Y: `machine_id`<br>X: `total_downtime_minutes` | Sum | `vw_downtime` |
| 5 | Clustered column | Average of availability_pct … by machine_id | X: `machine_id`<br>Y: 4 OEE components | **Average** | `vw_oee` |

### Current values observed

| Machine | Utilization % | Downtime (min) |
|---|---|---|
| M-101 | 80 | 28 |
| M-102 | 80 | 70 |
| M-103 | 80 | 81 |
| M-104 | 79 | 79 |
| M-105 | 79 | 79 |

Data labels are enabled on both bar charts.

### Known defects on this page

1. **Title text box still reads "Operations Dashboard"** while the tab is named "Production Performance" — inconsistent.
2. **The OEE breakdown chart is duplicated** from Page 1. It was copied rather than moved, so the same visual exists on two pages.
3. **Power Consumption spans only ~1 day** (Aug 07 12AM → Aug 08 12AM), so the trend line has little to show. Needs more days of accumulated data to be meaningful.
4. **Downtime arguably belongs on Page 3** with the other reliability metrics.

---

## 4. Page 3 — Maintenance & Reliability

**Current state: this page is a duplicate of Page 1.** It was created by duplicating the Executive Overview rather than adding a blank page, so it carries all 7 of Page 1's visuals unchanged.

### Intended contents (not yet implemented)

| Visual | Source view | Status |
|---|---|---|
| Failure Alerts table | `vw_failure_alerts` | Present (inherited from duplicate) |
| Predictive Maintenance table | `vw_predictive_maintenance` | Present (inherited from duplicate) |
| Downtime by machine | `vw_downtime` | Not yet moved from Page 2 |
| 3 cards + OEE chart + Live Status | — | **Should be deleted** |

### Remediation steps

1. Delete from this page: Average Temperature card, Active Alerts card, Overall OEE card, OEE breakdown chart, Live Machine Status table
2. Move the Downtime chart here from Page 2
3. Add a title text box reading "Maintenance & Reliability"
4. Result: 3 visuals, focused on a single question

---

## 5. Model configuration

| Setting | Current value |
|---|---|
| Storage mode | DirectQuery (all 8 views) |
| Relationships | **None defined** — each view is an isolated table |
| Date table | Not marked |
| Measures | **None** — all visuals use implicit aggregations on columns |
| Row-level security | Not configured |
| Theme | Default Power BI palette |
| Hidden columns | None |
| Sort-by columns | None configured |

**The absence of relationships is the most significant structural gap.** Because each view is standalone, a slicer on one visual cannot filter another, and cross-page filtering does not work. Every view independently repeats the machine list rather than sharing a conformed dimension.

**The absence of measures** means every number is an implicit aggregation. These are invisible in the field list, cannot be reused, cannot be documented, and are the reason several visuals defaulted to `Sum` when `Average` was required (a recurring correction during the build).

---

## 6. Gap summary vs production specification

| Dimension | As-built | Target (`POWERBI_SETUP.md`) |
|---|---|---|
| Pages | 3 (one a duplicate) | 5 (3 consumer, 1 drill-through, 1 tooltip) |
| Visuals | 12 across 3 pages | 24 across 5 pages |
| Model | 8 disconnected views | Star schema, 10 related tables |
| Measures | 0 explicit | 45+ in 9 display folders |
| Storage | Pure DirectQuery | Composite (Import agg + DQ atomic + Dual dims) |
| Titles | 4 of 12 visuals titled | All titled, none auto-generated |
| Theme | Default | Custom `theme.json` |
| RLS | None | Dynamic, 3 access tiers |
| Drill-through | None | Machine Detail page |
| Tooltips | Default | Custom report-page tooltip |
| Mobile layout | Not configured | Page 1 optimised |
| Accessibility | No alt text, no tab order | Both configured |

---

## 7. What works well in the current build

Worth stating plainly, because the gap table above is not the whole picture:

- **All 8 KPIs render correctly** against live data — the pipeline-to-visual path is proven end to end
- **DirectQuery is correctly configured** and reconnects cleanly after container restarts
- **Aggregations were corrected** from the Power BI default of `Sum` to `Average` wherever the underlying view was already one-row-per-machine — a subtle trap that silently produces wrong numbers
- **Page structure follows a defensible narrative** (overview → performance → maintenance), which is the right instinct
- **Data labels and formatting** were applied deliberately on the Operations page rather than left at defaults

The build demonstrates a working understanding of connection modes, aggregation semantics, and visual selection. The gaps are architectural rather than a matter of tool proficiency.
