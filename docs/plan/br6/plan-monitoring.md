# BR6 Plan: Monitoring and Run History

Implements [spec-monitoring.md](../../spec/br6/spec-monitoring.md): M1–M10, the two-run comparison and the links (main spec G4–G6). It builds on what [plan-data-import.md](plan-data-import.md) and [plan-pipeline.md](plan-pipeline.md) already record in `br6.pipeline_runs`, and copies the design of [BR5's monitoring plan](../br5/plan-monitoring.md).

**Status:** implemented and tested on 2026-10-24. Figures and checks: [result-monitoring.md](../../result/br6/result-monitoring.md). How to use it: [monitoring-guide.md](../../guide/br6/monitoring-guide.md).

Not covered: the benchmark (repeated runs for medians, routes B and C per option, the baseline at ×5 with the parallel reader). It gets its own plan, as in BR5, and uses what this plan builds.

## 1. Where we start

Most of the monitoring already works for BR6, because the earlier BR6 stages used the shared BR5 tools:

| Spec | Already in place |
|---|---|
| M1–M5 | Every import and run has a row in `br6.pipeline_runs`: id, type, times, status and error, step durations, choices, settings snapshot (with the grouping hash), check-ins read, used and excluded per reason, result count and summary (found, after filters, per status, route km), multiplier |
| Exact per-run figures | `run_figures.record` stores Spark figures (totals and per step, from the job groups) and ClickHouse figures (`log_comment = run_id` for runs, the staging inserts for imports) in `resources`. `spark_app_id` is recorded |
| M10 | Spark applications are named **"BR6 import"** and **"BR6 route run"**. The event log is on by default, so they appear on the shared Spark history server. BR6 Run History already links each run to its page there |
| Container time series | `docker-stats`, `spark-history` and the Prometheus job (5 s, 90 days) are in profile `br6` too and are started by `scripts/br6.py`. The containers are the same as BR5's: `spark-worker`, `br5-app`, `clickhouse` |
| Sidebar | `route_backend.DASHBOARDS` is empty, so BR6 pages show only *Spark history* |

What is missing:
- the BR6 Grafana folder and its three dashboards (M6–M8)
- the Run History comparison
- the dashboard links in the sidebar and back to the app

## 2. Decisions

| # | Decision | Why |
|---|---|---|
| H1 | **No new service, exporter or job code.** BR5's monitoring stack and the shared `run_figures` and `spark_settings` modules are reused as they are. | They already cover BR6 (section 1). The spec's note asks the plan to confirm the reuse; the only new work is the views on top. |
| H2 | **A view `br6.run_history`** gives one flat row per import and run: `duration_s`, `outside_steps_s`, `settings_from` (preset), `engine`, `city`, `route` (start → stops → end), `visitor` ("general" or the user id), `types`, `variant` (preset · engine) and, from `result_summary`: `route_km`, `found`, `after_filters`, `suggested`, and the counts per status. Dashboards and the app read it. | Same as `br5.run_history`. JSON parsing is written once, not in every panel. |
| H3 | **Three dashboards in a new BR6 folder**, from `grafana/dashboards-br6/` with its own provider `BR6`. They are adapted from BR5's JSON (uids `br6-run-performance`, `br6-resource-use`, `br6-data-result`). | M9. A provider reads its folder recursively, so BR6 needs its own directory. BR5's folder and files stay unchanged. |
| H4 | **The dashboards filter by city and by visitor kind (chosen or general)**, as well as BR5's run type, status, engine, preset, multiplier and *Compare by a setting*. Every settings key is offered there, for example `pipeline.matching`, `pipeline.distance`, `pipeline.place_stats`, `pipeline.reader`, AQE and shuffle partitions. | M6 asks to filter by city, settings and multiplier. Matching a chosen visitor adds a step, so both kinds should not be mixed in one comparison. |
| H5 | **Run History in the app gets BR5's compare section**, with BR6 content: choices (city, points, types, visitor), step durations, settings that differ, resource figures, and **Same result?** from `route_compare.differences`. `route_compare.py` gets its own `settings_diff`, a copy of BR5's 9 lines. The page accepts `?run=<id>` (as today) and `&a=<id>` to open a comparison, the same parameters as BR5. | Spec section 1 (Should) and G5: BR6 content imports no BR5 job module, and only BR6 runs are offered. |
| H6 | **Links:** `route_backend.DASHBOARDS` gets the three uids, so BR6 pages and the selection pages show them (G6, nothing else to change). Every dashboard links to the app's Route Finder and BR6 Run History. Its run tables link each row to `/route_run_history?run=<id>` and to the Spark history page. | Spec section 2, G4. |
| H7 | **`tests/br5/check_dashboards.py` gets an optional `--br 6`.** Without it, it checks BR5's dashboards as today. | Reuse, additive. |
| H8 | **Very short runs in M7:** a ClickHouse engine run (about 2 s) usually falls between two 5 s scrapes, so its time series show little or nothing. It is still marked on the panels. Its exact figures come from `resources`, and the Resource use dashboard has a table of them. The scrape interval stays 5 s. | A shorter interval would change BR5's monitoring for a gain only on 2 s runs. The exact figures are what the report uses. |

## 3. Dashboards (BR6 folder)

Variables shared by all three: time range, run type (import / run), status, engine, preset, multiplier, city, visitor kind. Every dashboard links to Route Finder, BR6 Run History and the other two dashboards.

**M6 Run performance** (ClickHouse, `br6.run_history`)
- Stats: runs, median, fastest and slowest run time.
- Run time over time: one point per run, coloured by variant.
- **Compare by a setting**: a median bar chart, and a table of runs with median, min and max per value of the chosen key.
- Steps:
  - stacked bars of the run steps (read, clean, match, visitor, score, write) for the latest 15 runs
  - stacked bars of the import steps for the latest 8 imports
  - Spark task time per step
- Runs table: time, type, status, preset, engine, city, route, visitor, types, multiplier, duration, check-ins read, places near the route, suggested. Links to Run History and to the Spark page.
- Resource figures per run: Spark CPU, GC, shuffle, spill, peak heap; ClickHouse rows read, CPU and peak memory.

**M7 Resource use** (Prometheus 5 s, plus run marks from ClickHouse)
- CPU and memory of `spark-worker`, `br5-app` and `clickhouse` against their limits. The `br5-app` panels say that this container also runs BR5 (spec note).
- Spark worker cores and memory given to runs; ClickHouse running queries and tracked memory.
- CPU seconds and peak memory per container in the time range.
- Every panel shows BR6 runs and imports as shaded regions (type, preset, status). A table lists the runs in the range, with their exact figures (H8).

**M8 Data and result** (ClickHouse)
- Latest import: status, time, cities, check-ins stored, multiplier, duration. Import history table.
- **Check-ins stored per city** (bar chart, from `fact_checkin` and `dim_city`) and places with a location per city.
- Latest run:
  - its choices and route km
  - check-ins read, used and excluded per reason (no location, far from the city, not offered)
  - places found near the route, after the filters, and per status
  - the suggestions per place type
  - the suggested places (name, type, detour, score)
- Per run over time: places near the route and places suggested.

## 4. Run History page (app)

`views/br6_run_history.py` is rebuilt on the pattern of BR5's `run_history.py`:
- **Table**, newest first, with filters for type, status, engine, preset and city. Columns: time, type, status, city and route, types, visitor, preset, engine, multiplier, duration, check-ins read and used, places near the route, suggested, and links to the result and the Spark page.
- **Compare two runs:** A and B (the latest two by default, or `a` and `run` from the address).
  - Summary side by side: status, duration, choices, data (import and multiplier), counts.
  - Step durations as grouped bars.
  - Settings that differ, including Spark's effective values and the grouping hash. The other settings are hidden.
  - Resource figures side by side (the BR5 page's list).
  - **Same result?** For two succeeded runs with the same choices: "yes", or the first differences. Otherwise it says why it can't compare.
  - Warnings when the runs read different imports or choices, when one did not succeed, or when the grouping hash differs.
- An import can be compared with another import: summary, steps, settings and figures, without *Same result?*.

## 5. Files

| File | New/changed | Content |
|---|---|---|
| `sql/clickhouse/ddl_br6.sql` | additive | View `br6.run_history` (H2) |
| `jobs/br6_route/route_runs.py` | additive | `run_history(client, limit, run_type)` reads the view. *As built:* no `run_pair`; the page takes both runs from the history it has already read |
| `jobs/br6_route/route_compare.py` | additive | `settings_diff(a, b)` |
| `grafana/dashboards-br6/br6-run-performance.json`, `br6-resource-use.json`, `br6-data-result.json` | new | Section 3. *As built:* adapted from BR5's JSON (same layout and panel styles). The *Import status* and *Imported at* stats read every field (`reduceOptions.fields = "/.*/"`): with BR5's setting they show "No data", because a stat panel reads only numbers by default. BR5's own dashboard had the same problem from the start: with the ClickHouse plugin of mid-October (4.22.1) it showed "No data" too, so it wasn't caused by a plugin update. It was never noticed because no BR5 screenshot shows that dashboard and the dashboard check counts rows only. Fixed in BR5's file the same way |
| `docker-compose.yml` | additive | Grafana provider `BR6` (folder BR6) and the mount of `grafana/dashboards-br6` |
| `apps/br5/route_backend.py` | small | `DASHBOARDS` and `dashboard_url(name, run)` (the run's time window); history through the view; `same_result`, `settings_diff`. *As built:* container CPU and memory per run reuse BR5's app helper `backend.container_figures` (same containers, same Prometheus) |
| `apps/br5/views/br6_run_history.py` | rebuilt | Section 4 |
| `apps/br5/views/route_finder.py` | small | A result links to Run History ("Compare with another run") and to Resource use for its time window |
| `tests/br5/check_dashboards.py` | additive | `--br 6` (H7) |
| `tests/br6/test_compare.py` | new | `settings_diff`. *As built:* the view's parsing needs ClickHouse, so it was checked by query instead (result file, row 1) |
| `scripts/br6.py`, `Makefile` | small | `status` (and `up`) print the app, Run History and BR6 dashboard URLs; `make br6-check-dashboards` |
| `docs/guide/br6/monitoring-guide.md` | new | URLs, what each dashboard shows, comparing by a setting, the Spark history page, the H8 note |
| `docs/plan/br6/plan-shared-app.md` | small | Section 3 and 4 status lines |

## 6. Order of work

| Step | Work | Done when |
|---|---|---|
| 1 | View, `route_runs` reads, `settings_diff`, unit tests | The view returns every stored run with its parsed counts; tests pass |
| 2 | Provider and the three dashboards; `check_dashboards --br 6` | All panel queries run without errors (row 2) |
| 3 | Run History page and links | Rows 5–7 checked in the browser |
| 4 | Guide, status lines, restart check, result file | Rows 8–9; `result-monitoring.md` written |

## 7. Verification (kept small, as in BR5)

| # | Check | Expected | Covers |
|---|---|---|---|
| 1 | Unit tests: `settings_diff`, summary parsing; the existing 25 | All pass | H2, H5 |
| 2 | `check_dashboards.py --br 6` after a few runs; the same script without `--br` | The BR6 folder has 3 dashboards and every query runs; the panels that need runs have data. BR5's check still passes | M6–M9 |
| 3 | One run each on route A: Spark default, Spark baseline, ClickHouse engine; and one import | Spark runs: `spark_app_id` set, CPU time > 0, per-step figures sum to no more than the totals, baseline shuffles more than the default. The ClickHouse run has ClickHouse figures only. The import has Spark and ClickHouse figures | M2, M7 |
| 4 | M7 during a default Spark run | `spark-worker` CPU rises inside that run's shaded region; memory stays under the limit | M7 |
| 5 | M6 *Compare by* `pipeline.matching` and `pipeline.distance`, and filtered by city | Run counts per value equal a direct query on `pipeline_runs` | M6 |
| 6 | M8 against the app | Excluded per reason, places found and the top suggestions match the Route Finder's result for the same run | M8 |
| 7 | Run History: default vs baseline, two identical runs, Spark vs ClickHouse engine | The settings that differ are exactly the baseline's keys (and their effective values). Same result: yes in all three cases. The identical runs show no setting differences | Spec section 1 |
| 8 | Links | App sidebar → the three dashboards and Spark history (search "BR6"); dashboard rows → BR6 Run History and the Spark page; BR5's sidebar and links unchanged | M9, M10, G4, G6 |
| 9 | `docker compose … down`, then `up` | Run history, dashboards, Spark history pages and the earlier M7 series are still there | Spec note 4 |

The result file records the three runs' figures from row 3, and the peak memory per container during them.

## 8. Risks

- **Shared containers:** `br5-app`, `spark-worker` and `clickhouse` serve both BRs. A BR5 run during a BR6 run would mix their figures, but the busy lock (G7) prevents that. The dashboards mark BR6 runs only.
- **The `docker-stats` and Spark history setup is BR5's.** BR6 changes nothing in it, so BR5's monitoring result stays valid.
## 9. Decided with the user (2026-10-24)

1. **Start dashboard (M9, Could): not built.** Grafana's folder list (CO5173, BR5, BR6) already works as the BR picker. A start dashboard would sit in the shared CO5173 folder, or would become Grafana's home page, which changes what BR3/BR4 users see.
2. **The benchmark gets its own plan** (`plan-benchmark.md`, as in BR5) after this one, rather than being part of this plan.
3. **H8:** short ClickHouse-engine runs are not caught by the 5 s scrape. They are covered by their exact figures, and the shared scrape interval stays as it is.

## 10. Notes

- **Spark history search:** the history server has no address parameter for its search box, so the dashboards link to its start page and the guide says to type "BR6".
- **Python work isn't in Spark's CPU figures** (BR5 plan, section 9). This matters more for BR6: the parallel reader and the UDF variants of the match step run in Python workers. The container CPU in M7 includes them, and the guide says so.
