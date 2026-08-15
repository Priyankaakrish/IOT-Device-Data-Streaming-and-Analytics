# Power BI — Semantic Model, Security & Dashboard Specification

Covers the star-schema model in `sql/04_star_schema.sql`. The legacy view-based
setup is preserved in section 9 so the existing report keeps working during the
migration.

---

## 1. Connect to PostgreSQL

1. **Get Data** → **More…** → **Database** → **PostgreSQL database**
   (first time only, accept the Npgsql driver prompt)
2. Server `localhost:5433`, Database `iot_dashboard`
3. Credentials `iot_user` / `iot_password`
4. In Navigator, select from the **`mart`** schema:

```
dim_date              fact_production_hourly
dim_time              fact_alert
dim_shift             fact_downtime
dim_machine           fact_sensor_reading
dim_machine_status    dim_user_scope
dim_alert_type
```

Do **not** load the `public.vw_*` views into the new model — they duplicate
logic that now lives in measures.

---

## 2. Storage mode — composite model

Neither pure Import nor pure DirectQuery is right. Set per table via
**Model view → table → Properties → Storage mode**.

| Table | Mode | Why |
|---|---|---|
| All `dim_*` | **Dual** | Serves both Import aggregates and DQ facts without a round trip |
| `fact_production_hourly` | **Import** | ~720 rows/machine/month. Compresses to nothing; sub-second visuals |
| `fact_alert` | **Import** | Small once transition-grained |
| `fact_downtime` | **Import** | Small |
| `fact_sensor_reading` | **DirectQuery** | Millions of rows. Only reached on drill-through |
| `dim_user_scope` | **Import** | Drives RLS |

**Why Dual rather than Import for dimensions:** a dimension joined to a
DirectQuery fact must be queryable in the source. An Import-only dimension
forces Power BI to send the whole dimension as a literal `IN (...)` list on
every DQ query, which fails above a few thousand values. Dual keeps a cached
copy for Import queries and a passthrough for DirectQuery ones.

### Aggregation table

Right-click `fact_production_hourly` → **Manage aggregations**:

| Agg column | Summarization | Detail table | Detail column |
|---|---|---|---|
| `units_produced` | Sum | fact_sensor_reading | units_produced |
| `good_units` | Sum | fact_sensor_reading | good_units |
| `reading_count` | Count table rows | fact_sensor_reading | — |
| `temp_sum` | Sum | fact_sensor_reading | temperature_c |
| `machine_key` | GroupBy | fact_sensor_reading | machine_key |
| `date_key` | GroupBy | fact_sensor_reading | date_key |

A query for monthly OEE now hits the imported aggregate in milliseconds; a
query for one machine's second-by-second trace falls through to DirectQuery
automatically. The user never knows which happened.

---

## 3. Relationships

All **one-to-many**, **single** cross-filter direction, dimension → fact.

| From (1) | To (many) | Column |
|---|---|---|
| `dim_date[date_key]` | all four facts | `date_key` |
| `dim_time[time_key]` | `fact_production_hourly` | `time_key` |
| `dim_machine[machine_key]` | all four facts | `machine_key` |
| `dim_shift[shift_key]` | all four facts | `shift_key` |
| `dim_machine_status[status_key]` | `fact_sensor_reading`, `fact_downtime` | `status_key` |
| `dim_alert_type[alert_type_key]` | `fact_alert` | `alert_type_key` |

**Rules:**

- **No bidirectional filters** except the RLS path. Bidirectional relationships
  create ambiguous filter paths and are the leading cause of "my totals don't
  match" bugs.
- **No relationships between fact tables.** That is what conformed dimensions
  are for.
- **Mark `dim_date` as the date table** on `full_date`
  (Table tools → Mark as date table). Without this, `DATESYTD` and siblings
  silently return wrong results at year boundaries.

---

## 4. Model hygiene

| Item | Action |
|---|---|
| Hide every `*_key` column on facts | Users must never see surrogate keys |
| Hide raw fact measure columns | Forces use of measures, not implicit `Sum of column` |
| Set `Summarize by = None` on all keys | Stops Power BI summing `machine_key` |
| Sort `month_name` by `month_number` | Otherwise months sort alphabetically |
| Sort `day_name` by `day_of_week` | Same reason |
| Format strings on every measure | Percentages `0.0%`, counts `#,0` |
| Descriptions on every measure | Surfaces as a tooltip in the field list |
| `_Measures` table | Empty table holding all measures |
| Apply `theme.json` | View → Themes → Browse for themes |

---

## 5. Row-Level Security

### 5.1 Why dynamic, not static

Static RLS means one role per line (`[production_line] = "LINE-A"`) and manual
membership management in the Service. It works but does not scale. We use
**dynamic RLS** driven by `mart.dim_user_scope`, which is also the source for
the Postgres-level policies in `sql/07_row_level_security.sql`.

### 5.2 Setup

**Step 1** — load `dim_user_scope` (Import mode).

**Step 2** — Modeling → **Manage roles** → new role `Dynamic Machine Access`,
filter applied to the **`dim_machine`** table:

```dax
VAR CurrentUser = LOWER ( USERPRINCIPALNAME () )
VAR UserScopes =
    FILTER (
        ALL ( dim_user_scope ),
        LOWER ( dim_user_scope[user_principal_name] ) = CurrentUser
            && dim_user_scope[is_active] = TRUE ()
    )
VAR HasGlobalAccess =
    CALCULATE (
        COUNTROWS ( UserScopes ),
        FILTER ( UserScopes, ISBLANK ( dim_user_scope[plant_code] ) )
    ) > 0
RETURN
    HasGlobalAccess
        || CONTAINS (
              SELECTCOLUMNS (
                  UserScopes,
                  "p", dim_user_scope[plant_code],
                  "l", dim_user_scope[production_line]
              ),
              [p], dim_machine[plant_code],
              [l], dim_machine[production_line]
           )
        || CALCULATE (
              COUNTROWS ( UserScopes ),
              FILTER (
                  UserScopes,
                  dim_user_scope[plant_code] = dim_machine[plant_code]
                      && ISBLANK ( dim_user_scope[production_line] )
              )
           ) > 0
```

One expression handles three access tiers: global (executive), plant-wide
(plant manager), line-specific (supervisor).

**Step 3** — secure the security table itself, same role, on `dim_user_scope`:

```dax
LOWER ( dim_user_scope[user_principal_name] ) = LOWER ( USERPRINCIPALNAME () )
```

Without this a curious user can read the whole access matrix, including who
holds executive rights.

**Step 4** — nothing else. Because `dim_machine` sits on the one-side of every
fact relationship, filtering it cascades to all four facts automatically.
**This is the payoff of the star schema: one RLS expression secures the entire
model.** In a snowflake the filter would have to traverse sub-dimensions; in a
flat design you would repeat the predicate on every table.

### 5.3 Testing

Modeling → **View as** → *Other user* → enter the UPN.

| Test user | Machines visible | Expected |
|---|---|---|
| `exec@contoso.com` | all | full totals |
| `plant.mgr@contoso.com` | all in PLANT-01 | full totals |
| `linea.sup@contoso.com` | LINE-A only | subset |
| `lineb.sup@contoso.com` | LINE-B only | subset |
| `nobody@contoso.com` | none | **blank, not everything** |

The unmapped-user case is the one that matters. A permissive default is a data
breach waiting to happen.

### 5.4 Service configuration

1. Publish, then Semantic model → **Security** → assign to the role
2. Assign **security groups**, never individuals
3. Workspace Admin/Member/Contributor roles **bypass RLS** — keep those lists
   minimal and give report consumers **Viewer** only
4. RLS is enforced in the model, not the gateway. The Postgres policies in
   `sql/07_row_level_security.sql` are the second layer, for anyone connecting
   with psql or DBeaver

---

## 6. Report pages

Five pages: three consumer-facing, one drill-through, one tooltip.
Canvas 1280×720, grid snap on, 8 px gutters.

### Page 1 — Executive Overview
*"Are we hitting target right now?"*

```
┌────────────────────────────────────────────────────────────────────┐
│  IoT Manufacturing Performance       [Date] [Plant] [Line]  [⟳]    │
├──────────────┬──────────────┬──────────────┬──────────────────────┤
│  OEE %       │ Availability │ Performance  │  Quality %           │
├──────────────┼──────────────┼──────────────┼──────────────────────┤
│ Active Alerts│ Avg Temp     │ Downtime     │  Energy / Unit       │
├──────────────┴──────────────┴──────────────┴──────────────────────┤
│  OEE Trend — Rolling 30 Days       │  Live Machine Status         │
└────────────────────────────────────┴──────────────────────────────┘
```

| Visual | Fields | Notes |
|---|---|---|
| Card ×4 | `[OEE %]`, `[Availability %]`, `[Performance %]`, `[Quality %]` | Callout colour bound to `[OEE Status Colour]`; subtitle `[OEE Trend Indicator]`. The last three multiply to the first — a reviewer will check |
| Card ×4 | `[Active Alerts]`, `[Avg Temperature (C)]`, `[Total Downtime (min)]`, `[Energy per Unit (kWh)]` | |
| Line chart | X `dim_date[full_date]`, Y `[OEE % (Visual)]` + `[OEE Target %]` | Target as dashed constant line, Y-axis fixed 0–1 |
| Table | `machine_name`, `status_name`, `[Avg Temperature (C)]`, `[OEE %]`, `[Active Alerts]` | Conditional format via `dim_machine_status[display_colour_hex]` |

Right-click a machine row → **Drill through → Machine Detail**.

### Page 2 — Production Performance
*"Where are we losing output?"*

| Visual | Fields | Notes |
|---|---|---|
| Text box | `[Dynamic Page Title]` | Reflects filter context |
| **Waterfall** | `[Availability Loss (min)]`, `[Performance Loss (min)]`, `[Quality Loss (min)]` | **The most valuable visual in the report** — shows where the OEE gap actually goes |
| Clustered column | X `machine_name`; Y the four OEE components | Sort descending by OEE |
| Line + column | X `full_date`; column `[Total Units Produced]`; line `[OEE %]` | Dual axis |
| Matrix | Rows `dim_shift[shift_name]`; Cols `machine_name`; Values `[OEE %]` | Heat map. Exposes shift variation invisible in daily aggregates |
| Line chart | X `full_date`; Y `[Total Energy (kWh)]`; Legend `machine_name` | |
| Bar | Y `machine_name`; X `[Energy per Unit (kWh)]` | Normalised — the fair comparison |

**Field parameter:** Modeling → New parameter → Fields → include `[OEE %]`,
`[Availability %]`, `[Performance %]`, `[Quality %]`, `[Scrap Rate %]`. Bind to
the column chart and matrix, add as a slicer. One visual now answers five
questions.

### Page 3 — Maintenance & Reliability
*"What needs attention, and how fast are we responding?"*

| Visual | Fields | Notes |
|---|---|---|
| Card ×4 | `[MTBF (hours)]`, `[MTTR (hours)]`, `[SLA Compliance %]`, `[Open Downtime Events]` | |
| Table | `raised_at`, `machine_name`, `alert_name`, `severity`, `[MTTA (min)]`, `breached_sla` | Sort `raised_at` desc; conditional format breaches red |
| Stacked column | X `full_date`; Y `[Total Alerts]`; Legend `severity` | Colours from `display_colour_hex` |
| Bar | Y `machine_name`; X `[Total Downtime (min)]`; Legend `is_planned` | Splitting planned vs unplanned is the point |
| Scatter | X `[MTBF (hours)]`, Y `[MTTR (hours)]`, Size `[Total Downtime (min)]`, Play axis month | Top-right quadrant = worst offenders |
| Decomposition tree | `[Total Downtime (min)]` by machine → shift → status | Root cause without building new visuals |

### Page 4 — Machine Detail (hidden, drill-through)

- Drill-through field `dim_machine[machine_name]`, Keep all filters **On**
- Add a **Back** button (Insert → Buttons → Back)
- Header card of machine attributes; full-resolution temperature and vibration
  traces from `fact_sensor_reading` via DirectQuery; complete alert history;
  downtime episode timeline

This page is the reason `fact_sensor_reading` stays in DirectQuery — atomic
data available exactly where it is needed, without importing millions of rows.

### Page 5 — Tooltip (hidden)

Page size **Tooltip** (320×240), Page information → *Allow use as tooltip* On.
Machine name, 24h OEE sparkline, status badge, open alert count. Assign on
other visuals via Format → Tooltip → Report page.

---

## 7. Formatting standards

| Element | Standard |
|---|---|
| Title | Segoe UI Semibold 14 pt `#252423` |
| Body | Segoe UI 10 pt `#605E5C` |
| Card callout | Segoe UI Bold 28 pt |
| Target met | `#2E7D32` |
| Warning | `#F9A825` |
| Breach | `#C62828` |
| Neutral series | `#118DFF` |
| Gridlines | `#E1DFDD`, horizontal only |
| Visual borders | Off — use whitespace, not boxes |
| Rounded corners | 8 px |
| Data labels | On for bar/column, off for line |
| Decimals | Fixed, never "Auto" |

**Before shipping:**

1. Every visual has a written title — "OEE by Machine", never
   "Average of oee_pct by machine_id"
2. No title containing "Sum of" or "Average of"
3. Alt text on every visual (Format → General → Alt text)
4. Tab order set (View → Selection → Tab order)
5. Slicers synced across pages (View → Sync slicers)
6. Tested at 100% zoom on 1920×1080 **and** in mobile layout
7. A "Data as at …" label on every page using `[_Last Refresh]`
8. Empty state handled via `[No Data Message]`

---

## 8. Performance targets

| Metric | Target | Tool |
|---|---|---|
| Executive Overview page load | < 2 s | Performance Analyzer |
| Any single visual | < 500 ms | Performance Analyzer |
| Worst DAX query | < 1 s | DAX Studio → Server Timings |
| Model size | < 1 GB | VertiPaq Analyzer |

**Incremental refresh** on `fact_production_hourly`: store 3 years, refresh
last 7 days, detect data changes on `created_at`. Requires `RangeStart` /
`RangeEnd` Date/Time parameters. Cuts daily refresh time by roughly two orders
of magnitude.

---

## 9. Legacy view-based setup (superseded)

The original report connects DirectQuery to eight `public.vw_*` views with no
relationships and no explicit measures. It still works and is documented in
`docs/DASHBOARD_AS_BUILT.md`.

Two limitations worth knowing while it remains in use:

- **No relationships** — each view is an isolated table, so cross-filtering
  between visuals does not work
- **No measures** — every number is an implicit aggregation, which is why
  fields kept defaulting to `Sum` when `Average` was required

Section B of `dax_measures.txt` keeps the view-based formulas so nothing breaks
before the migration completes.
