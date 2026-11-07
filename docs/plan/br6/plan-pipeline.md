# BR6 Plan: Route Finder Pipeline and Settings

Implements the run behind the Route Finder ([spec.md](../../spec/br6/spec.md) sections 3–4) and its settings ([spec-pipeline-config.md](../../spec/br6/spec-pipeline-config.md), C1–C14 except C11). It reads the star schema of [plan-data-model.md](plan-data-model.md), which [plan-data-import.md](plan-data-import.md) fills, and reuses that plan's run history, busy lock, shared job modules and app pages.

**Status:** implemented and tested end to end on 2026-10-24; figures in [result-pipeline.md](../../result/br6/result-pipeline.md). Decisions and readings confirmed; the spec was updated to match.

**As built, where it differs from below:**
- The reference files moved to a new pyspark-free module, `route_reference.py` (section 4).
- `docker-compose.yml` turns ClickHouse's `system.metric_log` off. Its merges took about 730 MiB of ClickHouse's 1 GB and made parallel inserts fail at random, which broke imports (result file, "ClickHouse memory").
- F6 measured: the grid saves about 25% of the matching CPU with every distance function, but no wall-clock time at this data size. The Python UDF doubles the matching executor time without making the grid visible. Pre-computed place statistics (F4) are the optimization with a clear effect.

This plan follows [BR5's pipeline plan](../br5/plan-pipeline.md) and says only where BR6 differs. Not covered: the Grafana dashboards and the Run History compare view (monitoring plan), and selectable table layouts (C11, Could). Testing here shows that the run is correct and that the settings take effect. The full benchmark matrix comes with the monitoring plan.

## 1. Decisions taken in this plan

| # | Decision | Why |
|---|---|---|
| F1 | **Each run is its own Spark application**, named "BR6 route run" (`spark-submit` per click, as BR5's A1). This settles open decision 7 of the spec. | Resource settings only apply when an application starts, and each run starts clean, so runs compare fairly. Cost: 15–30 s start-up per run. |
| F2 | **Spark does the data work; route geometry and scoring run in plain Python on the driver** (`route_rules.py`, shared by both engines). Spark reads the places and their statistics, applies the cleaning rules, and finds the places near the route. The driver gets back only the places near the route (at most a few thousand rows). | Same split as BR5 (A4). The scoring is easy to read and to unit-test without Spark, and both engines use the same code, so they can't drift apart. |
| F3 | **Distances are computed on a flat local map of the city:** longitude and latitude are turned into km east and north of the city centre (`dim_city.centre_*`). Route length, detour and position all use it. | One simple formula that native Spark expressions, a Python UDF, ClickHouse SQL and the unit tests all compute the same way, so the methods give the same numbers. Within 100 km of the centre the error is about 1% at most, which is small next to the straight-line simplification itself (spec 3.3). |
| F4 | **Place statistics per place: check-ins and different visitors, both as in the original data** (÷ multiplier). Two options (spec section 3): computed from the fact table at **every run** (baseline), or **at import** into a new table `br6.place_stats` (optimized; P11 of the import plan). | The statistics are per place, so they don't depend on the category grouping. A re-grouping takes effect on the next run in both modes, and "at import" never goes stale until the next import. The cleaning counts of R4 can be summed from the per-place figures, so both modes report the same counts. |
| F5 | **The grid method only skips places; the exact distance is still computed for every place it keeps.** It keeps the squares that can hold a place within the maximum detour, so both methods give exactly the same list (C9). | Correct by construction, and the unit tests check that the grid never loses a candidate. |
| F6 | **A new setting, "Distance function": native Spark expressions (default), `pandas_udf`, or a plain Python UDF (baseline).** | **The grid alone will barely show in the run time.** The largest city has 49,291 places with a location (Jakarta). Comparing all of them with a route of up to 4 legs means at most 200,000 distance calculations, which native Spark finishes in well under a second, next to 15–30 s of start-up. With a Python UDF, each of those calculations goes through Python, and then the grid's saving can be measured. It is also the native-vs-UDF comparison that CLAUDE.md asks every BR to show. |
| F7 | **An alternative engine: the same run as ClickHouse SQL** (`engine = clickhouse`, plain Python, no Spark), feeding the same `route_rules.py` (C10, Could). Built last; dropped if time runs short. | Same reasons as BR5's A5: the course asks for an alternative that is benchmarked, and it is a correctness reference, since both engines must give the same list. |
| F8 | **The run's choices are stored by natural ids:** city code, `venue_id` of each route point, `user_id` of the visitor. The job looks up the keys. | Place and visitor keys depend on which cities were imported (keys are numbered per city with an offset), so a run repeated after a re-import with other cities still means the same route. |
| F9 | **Every run reloads `data/br6/category_place_type.csv`** into `br6.category_place_type` when the file differs from the table, then records the file's hash in the settings snapshot (D13, P9). The Route Finder page does the same before it builds its lists. The reload builds a new table and swaps it in with `EXCHANGE TABLES`. | Spark, ClickHouse SQL, the app's lists and Grafana all read the same grouping table, and the run history shows which grouping a run used. |
| F10 | **Settings, presets, the Stop button, the busy lock and the settings check that blocks a run that can never start** work as in BR5 (A6–A8, A10), using the shared `spark_settings.Catalog`, `run_store` and `job_launch` built in the import stage. | Already built and tested. BR6 only adds its own settings (section 5). |

**How this plan reads the spec** (confirmed and written into the spec):

| # | Spec point | Reading |
|---|---|---|
| S1 | Cleaning (3.5) | A check-in is excluded for the first reason that applies to **its place**, in this order: no location; more than the far-from-centre distance from the city centre; main category is *Not offered*. So read = used + Σ excluded holds exactly. **Used** check-ins are the ones popularity and visitor habits are computed from |
| S2 | Place type of a place | The type of its **main category** (data model 3.1), for both suggestions and visitor habits |
| S3 | Places near the route (R1) | **Found** = places with a location, not too far from the centre, within the maximum detour, of any type, except the route's own points. **After the filters** = found, of a chosen place type, and with at least the minimum of different visitors |
| S4 | Minimum check-ins of a visitor (3.5) | Counted on the visitor's **used** check-ins (offered place types only). A visitor with 10 check-ins, all at offices and stations, has no habits to fit, so they are not offered. On this data: 10,190 eligible visitors in Istanbul, 19 in Beijing |
| S5 | Fit (3.3) | **Fit** = the visitor's share of used check-ins in the place's type, divided by their share in their favourite type, so the favourite type scores 1. Example: 50% Food & drink, 30% Shopping, 20% Sights & culture → fit 1.0, 0.6 and 0.4. **No bonus for a place already visited:** the card still shows the number of visits, but they don't change the score. The card shows the raw share ("35% of your check-ins are Food & drink") |
| S6 | General visitor | No fit part. It uses its own weights, **70% popularity / 30% closeness** by default, editable like the other weights |
| S7 | Popularity (3.3) | Different visitors ÷ the most of any place after the filters on this route. Order of the list: score (rounded to 6 decimals), then different visitors, then check-ins, then place id. So check-ins break ties in popularity, and the place id breaks the rest, as the spec asks |
| S8 | Route points | The start, the end and the stops are never suggested |
| S9 | Place search (Q2) | Lists places with a location within the far-from-centre distance, of any type (a hotel or a station is a fair start), except the category *Home (private)* |
| S10 | Visitor with the multiplier | The chosen visitor's habits are read from copy 0, which is exactly the original visitor |

## 2. Flow of a run

```
Route Finder "Find stops"  /  make br6-run CITY=.. START=.. END=.. [STOPS=a,b] [TYPES=..] [VISITOR=..] [PRESET=..]
  └─ route_launch.py run (inside br5-app): data loaded? busy (BR5 or BR6)? resolve settings → validate
       ├─ engine spark:      spark-submit route_run.py --city C --start V --end V [--stops ..] [--types ..] [--visitor U] ..
       └─ engine clickhouse: python3 route_run.py ...           (no Spark application)
            1. register   run row (run_type 'run', criteria, settings snapshot, import_id); reload the grouping (F9);
                          look up the keys of the route points and the visitor; build the route (legs, km) on the driver
            2. read       places of the city with their type and statistics:
                            stats at import → br6.place_stats (one row per place)
                            stats every run → the city's fact rows (place, visitor, copy), aggregated per place;
                                              with caching on, the rows are cached here for step 5
            3. clean      per-place reason (S1) → check-ins read, used, excluded per reason
            4. match      "find places near the route": grid or every place → exact detour, leg and position
                          (native / pandas_udf / Python UDF) → places within the maximum detour → driver
            5. visitor    (chosen visitor only) used check-ins per place type, visits per place near the route
            6. score      route_rules.py: filters (S3), popularity, fit, closeness, score, order, top N
            7. write      run_point and run_place rows; finish run (counts, result summary, step durations, figures)
          on failure: run marked failed with the error; no partial result is shown
```

- **Step 2 is where the two statistics modes differ.** At import, the run reads one small row per place (up to 85k for the largest city, including places without a location). Every run, it reads the city's whole fact partition: up to 544,471 rows × multiplier for Istanbul, with `countDistinct(visitor, copy)` per place. The busiest places (shopping malls) are a real skew case for that aggregation.
- **Reading the fact** (every-run mode) uses the reader setting: **parallel**, where each Spark task reads its own slice from ClickHouse as Arrow (`source_row % N`, inside `city_key = k`, so ClickHouse reads one partition only), or **driver**, one query turned into a DataFrame. This is BR5's reader, moved to `jobs/common/clickhouse_read.py`. BR5's copy is not touched (P3).
- **Joins:** places ⋈ statistics (up to 85k rows each side) is broadcast or sort-merge depending on the broadcast threshold, so the baseline preset shows a sort-merge join in `explain()`. The route's squares and legs are tiny and always broadcast.
- **The driver only ever collects the places near the route.** A 15 km route through central Jakarta, with a 1 km corridor, holds a few thousand places.

## 3. Route geometry and scoring (`route_rules.py`)

**Flat map (F3).** For a city with centre (lat₀, lon₀) and K = 111.195 km per degree (π × 6371.009 / 180):
- x = (lon − lon₀) × cos(lat₀) × K and y = (lat − lat₀) × K
- cos(lat₀) is computed once on the driver and passed to Spark and ClickHouse as a number, so every engine computes x and y the same way.

**Route.** The points are start, then stops in order, then end, which gives up to 4 legs. Two consecutive points must be different places; if two places share coordinates, the leg has length 0 and is handled.

**Detour, leg and position of a place Q.** For each leg from A to B with length L:
- t = clamp(((Q − A) · (B − A)) / L², 0, 1), with t = 0 when L = 0
- the distance from Q to the leg is |Q − (A + t (B − A))|

Detour = the smallest distance over the legs. When two legs are equally close, the earliest one counts. Position = the length of the legs before it + t × L. The card text says "3.2 km from the start, between the start and stop 1".

**Grid (F5).**
- Square of a place = (⌊x / s⌋, ⌊y / s⌋), where s is the square size.
- The route's squares are computed on the driver. For each leg, every square in the leg's bounding box widened by the maximum detour D is kept when its centre is within D + s/√2 (half the square's diagonal) of the leg.
- Every place within D of the route lies in a kept square, so no candidate is lost. The unit tests check this on random routes.

**Score** (defaults: weights 50 / 30 / 20, general visitor 70 / 30, D = 1 km):
- popularity = visitors ÷ the most visitors of any place after the filters
- fit = the visitor's share in the place's type ÷ their share in their favourite type (S5)
- closeness = 1 − detour ÷ D
- chosen visitor: score = w_pop × popularity + w_fit × fit + w_close × closeness
- general visitor: score = g_pop × popularity + g_close × closeness (S6)
- Order and ties as in S7. The top N (default 10) are suggested

**Messages instead of an empty page:**
- No place within the detour: "No places within the allowed detour for these choices." The page suggests widening the place types and says how many places were found of other types.
- Places found, but none passes the minimum visitors: the same message, with the count of places that have fewer visitors.

## 4. Files

| File | New/changed | Content |
|---|---|---|
| `jobs/br6_route/route_rules.py` | new | Pure Python: flat map, legs, detour and position, the route's squares, filters, fit, score, ordering, card texts, result summary |
| `jobs/br6_route/route_reference.py` | new (*built*) | The reference files (cities, category grouping, place types) moved out of `route_star.py`, which imports pyspark, so the app and the ClickHouse engine can read them. `route_star.py` re-exports the names |
| `jobs/br6_route/route_run.py` | new | Run entry point (arguments: `--city`, `--start`, `--end`, `--stops`, `--types`, `--visitor`, `--run-id`, `--settings-json`, `--settings-label`). Steps as in section 2; picks the engine |
| `jobs/br6_route/route_spark.py` | new | Spark engine: read (place statistics or fact rows), clean, match (grid or every place; three distance functions), visitor habits. Logs `explain()` of the statistics and the matching |
| `jobs/br6_route/route_sql.py` | new | ClickHouse engine (F7): the same outputs from SQL, every place compared, the same flat-map formula |
| `jobs/br6_route/route_compare.py` | new | Checks that two runs gave the same list and figures (rounded). Used for verification rows 3–6 and in the restart guide |
| `jobs/br6_route/route_settings.py` | additive | BR6 settings and analysis rules (section 5), presets, checks (weights add up to 100) |
| `jobs/br6_route/route_runs.py` | additive | Grouping reload (F9), key lookup by natural id, the latest run and its results, `place_stats` in `DATA_TABLES` |
| `jobs/br6_route/route_launch.py` | additive | `run_args(...)` and the `run` CLI subcommand; the ClickHouse engine starts `python3` instead of `spark-submit` |
| `jobs/br6_route/job.py` | small | New import step `place_stats` (section 7) |
| `jobs/common/clickhouse_read.py` | new (shared) | BR5's parallel Arrow reader and driver reader, with the table, columns and filter as parameters. BR5's copy is not touched (P3) |
| `sql/clickhouse/ddl_br6.sql` | additive | Tables `place_stats`, `run_point`, `run_place` (section 6) |
| `apps/br5/route_backend.py` | additive | City list with suggestible place counts, place search, visitor list, latest run and its results, settings and presets |
| `apps/br5/views/route_finder.py` | rewritten | Route Finder screen (section 8) |
| `apps/br5/views/br6_settings.py` | rewritten | Advanced Settings screen for BR6 (section 9) |
| `apps/br5/views/br6_data.py` | small | Place statistics status line |
| `apps/br5/views/br6_run_history.py` | small | Lists runs as well as imports until the monitoring plan builds the full screen |
| `scripts/br6.py`, `Makefile` | additive | `run` subcommand and `br6-run` (variables `CITY`, `START`, `END`, `STOPS`, `TYPES`, `VISITOR`, `PRESET`); `PRESET` for `br6-import` |
| `docker-compose.yml` | small (*built*) | Config `clickhouse-system-logs-xml` turns `system.metric_log` off (see the status note) |
| `tests/br6/test_rules.py` | new | Geometry, grid (never loses a candidate), filters, fit, score, ties, general visitor, messages |
| `tests/br6/test_import.py` | additive | Settings: new choices, weights check, presets valid |
| `docs/guide/br6/config-guide.md` | new | Restart guide (section 10) |
| `docs/spec/br6/*.md` | done | Updated with F1 (open decision 7), F6, and the readings S1–S10 |

BR5 files are not changed, apart from `app.py`, which already lists the BR6 pages.

## 5. Settings

**Spark settings:** the shared catalog, as for the import, with the same defaults.

**BR6 settings:**

| Group | Setting | Default | Choices / range | Notes |
|---|---|---|---|---|
| Finding places near the route | `pipeline.matching` | grid | grid / all | All = every place of the city is compared with the route (baseline) |
| | `pipeline.cell_km` | 1.0 | 0.1–20 km | Square size for the grid |
| | `pipeline.distance` *(F6)* | native | native / pandas_udf / python_udf | How each distance is computed |
| Place statistics | `pipeline.place_stats` | import | import / run | Import needs a re-import after this plan is built; until then a run in this mode fails with "Place statistics are missing: re-import, or set Place statistics to every run" |
| Caching | `pipeline.cache`, `pipeline.storage_level` | on, MEMORY_AND_DISK | as BR5 | Caches the fact rows read in every-run mode, which steps 3 and 5 both use. No effect in import mode (the help text says so) |
| Reading and writing | `pipeline.engine` | spark | spark / clickhouse | F7 |
| | `pipeline.reader`, `pipeline.read_partitions` | parallel, 2 | parallel / driver; 1–64 | Fact read, every-run mode only |
| | `pipeline.writer`, `pipeline.write_partitions` | partitions, 2 | as built | Import only |
| | `clickhouse.max_threads`, `clickhouse.max_memory_usage` | 0, 0 (server default) | as BR5 | Sent with every query |
| Analysis rules (3b) | `rules.max_detour_km` | 1.0 | 0.1–10 | |
| | `rules.top_n` | 10 | 1–50 | |
| | `rules.weight_popularity` / `_fit` / `_closeness` | 50 / 30 / 20 | 0–100 each | Chosen visitor. Error unless they add up to 100 |
| | `rules.general_weight_popularity` / `_closeness` | 70 / 30 | 0–100 each | General visitor (S6). Error unless they add up to 100 |
| | `rules.min_place_visitors` | 3 | 1–1000 | Compared after dividing by the multiplier |
| | `rules.min_visitor_checkins` | 10 | 1–1000 | S4 |
| | `rules.max_centre_km` | 100 | 1–1000 | |
| | Category → place type table | the file | read-only on the screen | Shows the path, the hash and the categories per type; edited in the repository (F9) |

**Built-in presets** (read-only):
- **default**: the table above.
- **baseline**: every place compared, Python UDF, statistics every run, driver reader, no cache, broadcast off, AQE off, pandas writer.

The snapshot (C6) is the same as BR5's: every resolved value, where the settings came from, Spark's effective values and the cluster seen at the start. BR6 adds `grouping.sha1` and the categories per type (F9).

## 6. Tables (additive)

**`br6.place_stats`** (data table, built by the import, swapped with the others, dropped by fresh mode):
- `city_key, place_key, checkins UInt32, visitors UInt32`, both counted in the original data.
- `MergeTree ORDER BY (city_key, place_key)`: one row per place, including places without a location.

**`br6.pipeline_runs`**, used as it is:
- `criteria`: city, start, end, stops (as `venue_id`s, F8), place types, visitor (`user_id` or empty).
- `rows_read`: the city's check-ins (original data).
- `rows_used` and `rows_excluded` (S1).
- `result_count`: the number suggested.
- `result_summary` (JSON): route km, places found and remaining per filter (S3), suggestions per place type, the message if any, the visitor's used check-ins and shares per type.

**`br6.run_point`** (`MergeTree ORDER BY (run_id, seq)`):
- `run_id, seq (0 = start), role (start / stop / end), place_key, venue_id, name, latitude, longitude, km_from_start`.

**`br6.run_place`** (`MergeTree ORDER BY (run_id, place_key)`): one row per place found near the route (S3).
- Place: `place_key, venue_id, name, address, category_name, place_type`.
- Route: `detour_km, position_km, leg`.
- Statistics: `visitors, checkins, visitor_visits, type_share`.
- Score: `popularity, fit, closeness, score, rank` (0 = not suggested).
- `status`: suggested / below the top N / few visitors / type not chosen / not offered.

Results are kept for every run (a few thousand rows at most). The monitoring plan will chart them.

## 7. Import change (P11)

New step **`place_stats`** between `verify` and `swap`:
- It groups the cached original rows (×1, before the copies) by (city, place): check-ins and different visitors. It writes them to `br6_staging.place_stats` with the import's writer.
- Checks: one row per `dim_place` row; Σ check-ins = rows read, also per city.
- The swap includes the table. The Data screen shows "Place statistics: 338,660 places", or "missing: re-import".

The import's recorded figures gain the step's duration, so the report can weigh the extra import time against the time saved per run.

## 8. Route Finder screen (spec 3.1–3.2)

**Choices:**
- **City** (Q1): "Jakarta (Indonesia) · 6,628 places to suggest". The count is the places with a location, near the centre, of an offered type and with the minimum of visitors. It comes from one ClickHouse query, cached per import and settings.
- **Start, end and stops** (Q2–Q4):
  - Each has a search box and a drop-down of the matches (S9). The matches are the 50 most visited places whose name contains the text, shown as "Name · Category · Address".
  - **Add a stop** adds up to 3 stops, in order, each with a remove button.
  - Two consecutive points must be different places, and the end must differ from the start.
- **Place types** (Q5): a multiselect of the five types; all are chosen by default.
- **Visitor** (Q6):
  - A drop-down with **General visitor** first, then the eligible visitors (S4) sorted by check-ins, shown as "Visitor 18342 · 214 check-ins · mostly Food & drink, Shopping".
  - Istanbul has about 10,000 eligible visitors. Streamlit's drop-down filters as you type; if it is slow with that many, the list shows the 2,000 most active visitors and says so.
- No data loaded: the "No data yet" message with a link to the BR6 Data page.

**Find stops:**
- The button starts a run and is disabled while BR5 or BR6 is busy (G7).
- Progress shows the current step in plain words every 2 s, with **Stop** for a run started from this app.

**Result** (the latest succeeded run, headed with its choices; a result computed on an older import is labelled as such):
- **R1:** start → stops → end, the route length in km, places found and places left after the filters. The note "Distances are straight lines; the app knows no roads" is always shown.
- **R2:** one card per suggestion, with the content of spec 3.2: place, detour, position, popular because, fits you because (chosen visitor only) and score with its three parts.
- **R3** (expander): the table of every place found, with its status and scores, and a bar chart of suggestions per place type.
- **R4** (expander):
  - run time and step durations
  - engine, preset and the settings that differ from the default
  - check-ins read, used and excluded per reason
- **R5:** CSV download of the suggestions.
- The no-result message of section 3 when nothing is suggested.

## 9. Advanced Settings screen (C1–C7, C12)

Built like BR5's screen ([BR5 plan](../br5/plan-pipeline.md), section 6), in `views/br6_settings.py`, on BR6's own `__current__` row and presets in `br6.settings`:
- one section per group, with explanation, default, current value, a mark when changed, and a per-setting Reset; Reset all at the top
- edits are saved at once; wrong values are rejected with a message
- errors from the cluster check and the weights check block Run and Import; warnings are shown next to the setting
- presets: Load, Save as, Delete (user presets only)
- the read-only "Needs restart" section and a link to the restart guide
- Analysis rules have their own group, labelled "for the team", including the read-only category table (section 5)

## 10. Restart guide (`docs/guide/br6/config-guide.md`, C13–C14)

- Same entries as [BR5's guide](../../guide/br5/config-guide.md): worker memory and cores, container limits, `SPARK_DAEMON_MEMORY`, ClickHouse server settings, Prometheus scrape interval. Each entry gives what it does, the file and line, a sample value, the exact restart command, and how to confirm the change.
- **What is shared:** a restart of Spark, ClickHouse or the app affects BR5 and BR6 together. Restarting `br5-app` closes both BRs' pages and stops any job started from it.
- **BR5 and BR6 must not run at the same time while measuring (G7).** BR6 waits for BR5, but BR5 doesn't check BR6.
- **Fair comparison (C14):** change one thing at a time; same city, route, visitor and rules; discard the first run; repeat 3 times; compare medians. `route_compare.py` confirms that two runs gave the same list.

## 11. Order of work

| Step | Work | Done when |
|---|---|---|
| 1 | `route_rules.py` + `tests/br6/test_rules.py` | Unit tests pass, including the grid check on random routes |
| 2 | Settings, DDL additions, `route_runs.py` additions | Unit tests pass; the DDL re-applies on the existing `br6` without errors |
| 3 | `place_stats` import step | Re-import ×1 passes the new checks |
| 4 | `clickhouse_read.py`, `route_spark.py`, `route_run.py` (Spark engine, all options) | A manual run writes results; the counts add up; the options give the same list |
| 5 | `route_launch.py run`, `scripts/br6.py run`, Makefile | `make br6-run` works with each preset |
| 6 | Advanced Settings screen | Edit, reset, presets, checks and read-only section work |
| 7 | Route Finder screen, Data and Run History changes | Section 12 rows 1–9 checked in the browser |
| 8 | `route_sql.py` (ClickHouse engine, F7) | Same list as the Spark engine |
| 9 | Restart guide; spec and plan updates; result file | Docs match the code; `result-pipeline.md` written |

## 12. Verification (kept small on purpose)

Routes used below: **Jakarta**, a route of about 15 km through the centre (the largest city by places). **Istanbul**, with one stop (the largest by check-ins). **Beijing** (the thinnest data). The exact start and end are picked in step 4 and written into the result file.

| # | Check | Expected | Covers |
|---|---|---|---|
| 1 | Unit tests: detour and position on hand-made routes, leg ties, zero-length leg, grid never loses a place (random routes), filters, fit (a visit doesn't change it), general-visitor weights, order and ties, messages; settings choices, weights check, presets | All pass | Rules, C4 |
| 2 | Jakarta, general visitor, default settings | Succeeds; read = used + Σ excluded; read = the city's check-ins; up to 10 suggestions with detour ≤ 1 km | R1–R4, correctness |
| 3 | Row 2 with matching all vs grid, and native vs pandas_udf vs Python UDF | Same list (`route_compare.py`) | C9, F6 |
| 4 | Row 2 with statistics at import vs every run | Same list and the same R4 counts | F4 |
| 5 | Same run twice; then re-import at ×2 and run again | Identical lists | Determinism, multiplier |
| 6 | Spark vs ClickHouse engine on row 2 | Same list | C10 |
| 7 | Istanbul with a stop and a chosen visitor, then general visitor | Different rankings; the fit lines match a hand query on the visitor's check-ins | Acceptance 3, S5 |
| 8 | Beijing with a short route; a route outside the places of the chosen types | Clear message, never an empty page | Acceptance 4 |
| 9 | Preset `baseline` | Snapshot shows its values and Spark's effective values match; `explain()` shows a sort-merge join and a Python UDF | C2, C6 |
| 10 | Weights 50/30/30; general-visitor weights 70/40; executor memory 4g | Both block Run with a clear error | C4 |
| 11 | Start a run during a BR5 run; press Stop on a run | Refused as busy; "Stopped by the user", and the lock clears | G7, F10 |
| 12 | Edit the category file (move one category), run again | The grouping is reloaded; the snapshot hash changes; the list follows | F9, D13 |
| 13 | BR5: one analysis run; BR3/BR4 `spark-submit` commands | Unchanged | Additive changes |

The result file records a few figures only:
- run time per step for row 2 (and Istanbul) with default vs baseline settings, at ×1 and ×5
- each option on its own: matching method, distance function, statistics mode, reader
- the Spark engine vs the ClickHouse engine
- the `place_stats` import step's duration against the time it saves per run
- exclusion counts per reason, and places found vs suggested

## 13. Risks and notes

- **Spark start-up will dominate most runs.** At import-time statistics, the Spark work itself is a second or two, so the ClickHouse engine will likely win by the start-up alone, as in BR5. The every-run statistics at ×5 on Istanbul (2.7 million rows) are where Spark has real work to do. That contrast is a finding for the report.
- **The grid helps little with native distances (F6).** Without the Python UDF setting, this would be reported as "no measurable difference at 49k places".
- **Floating-point results** can differ in the last digits between engines and distance functions. Distances are rounded to 1 m and scores to 6 decimals before ordering, storing and comparing.
- **The visitor drop-down** may be slow with 10,000 entries (section 8 has the fallback).
- **The flat map** is about 1% off at 100 km from the centre. This is acceptable next to straight-line routes, and every engine uses the same map, so it doesn't affect the comparisons.
- **Reloading the grouping from the app** (F9) writes to a shared table while no job runs. A run that starts at the same moment reads either the old or the new table, never a mix, because the swap is one `EXCHANGE TABLES`.
