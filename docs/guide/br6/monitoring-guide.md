# BR6 Guide: Monitoring and Run History

How to read what the Route Finder pipeline did and what it cost. It covers [plan-monitoring.md](../../plan/br6/plan-monitoring.md). For changing the settings behind a run, see [config-guide.md](config-guide.md). BR6 uses the same monitoring stack as BR5, so [BR5's monitoring guide](../br5/monitoring-guide.md) explains the parts they share in more detail.

## Where to look

| What | Where | Use it for |
|---|---|---|
| Run History (app) | http://localhost:8501/route_run_history | Every BR6 import and run; comparing **two runs** side by side |
| Grafana, BR6 folder | http://localhost:3000/dashboards?tag=br6 (admin / admin) | Trends over many runs, CPU and memory over time |
| Spark history | http://localhost:18080 | One Spark run in detail: jobs, stages, tasks, the plans. BR6 applications are named "BR6 import" and "BR6 route run": type "BR6" in the search box |
| Live Spark page | http://localhost:4040 | The Spark run in progress (the Route Finder links to it while a run is going) |
| Prometheus | http://localhost:9090 | Raw metrics, for queries the dashboards don't have |

Every BR6 page of the app has these links in the sidebar under **Monitoring**. The selection pages (Advanced Settings, Data, Run History) show BR5's and BR6's links in two groups. `make br6-status` prints the URLs.

## What is recorded for each run

Each import and Route Finder run is one row of `br6.pipeline_runs`. The view `br6.run_history` flattens it for the dashboards and the app:

| Column | Meaning |
|---|---|
| `settings_from`, `engine`, `variant` | "current settings" or "preset NAME"; `spark` or `clickhouse`; the two together |
| `city`, `route`, `types`, `visitor`, `visitor_kind` | The choices: city code, start → stops → end (place names once the run has found them), place types, the visitor's user id or "general". For imports, `city` and `visitor_kind` are "(import)" |
| `duration_s`, `outside_steps_s`, `step_durations` | Run time, time outside the steps (mostly Spark start-up) and each step: `read`, `clean`, `match`, `visitor`, `score`, `write`; for imports `read` … `place_stats`, `swap` |
| `rows_read`, `rows_used`, `rows_excluded` | Check-ins of the city read, used, and excluded per reason (`no_location`, `far_from_city`, `not_offered`) |
| `found`, `after_filters`, `suggested`, `below_top_n`, `few_visitors`, `type_not_chosen`, `not_offered` | Places within the detour, places left after the filters, and places per status |
| `resources` | Exact Spark and ClickHouse figures, the same names as BR5 (BR5 guide, "What is recorded"). Spark figures per step are `spark.<figure>@read`, `@clean`, `@match`, `@visitor`; `score` and `write` run in the driver without Spark jobs. A ClickHouse-engine run has ClickHouse figures only |

Container CPU and memory are not stored with the run. They come from Prometheus every 5 s and are kept for 90 days. Run History computes them for the run's time window.

## Dashboards

All three have links to the Route Finder, the BR6 Run History, Spark history and each other at the top.

**BR6 Run performance**:
- Filters at the top: type (runs by default), status, engine, "settings from", multiplier, city, visitor (general or chosen).
- **Compare by setting:** pick any key of the settings snapshot, for example `pipeline.matching` (grid or every place), `pipeline.distance` (native, pandas_udf, python_udf), `pipeline.place_stats` (import or run), `pipeline.reader` or `spark.sql.adaptive.enabled`. The bar chart and table give the runs, median, min and max per value of that key.
  - For a fair comparison, fix the city, the visitor and the multiplier first. The route itself is not a filter: check the Runs table for the routes behind the figures.
- Below that: the steps of the latest runs and imports (grey = start-up and other time), Spark task time per step, the runs table and the resource figures per run.
- In the runs table, `run_id` opens the run in the app's Run History, and `spark_app_id` opens its Spark page.

**BR6 Resource use**:
- CPU (cores in use) and memory (working set, dashed = the limit) for `spark-worker` (the executors), `br5-app` and `clickhouse`. `br5-app` is the shared app container: it runs the Spark driver of both BR5 and BR6, and the ClickHouse engine's Python code.
- Each BR6 import and run is a shaded region; hover over its edge for its details.
- **To see what one run cost:** drag across its shaded region to zoom in. "CPU seconds used" and "Peak memory" then cover that run only.
- The table at the bottom lists the runs in the time range with their exact figures. A ClickHouse-engine run (about 2 s) usually falls between two 5 s samples and barely shows on the charts; its row in this table still has its figures.

**BR6 Data and result**:
- The latest import (status, time, check-ins stored, multiplier, duration), the stored tables and the import history.
- **Check-ins stored per city**, and per city: check-ins, check-ins without a location, places, places with a location, visitors.
- For the latest succeeded Route Finder run:
  - its choices and route length
  - check-ins read, used and excluded per reason
  - places near the route, per status
  - the suggested places, and the suggestions per place type
- At the bottom, per run over time: places near the route, after the filters, and suggested.

## Comparing two runs (app)

On **Run History** (Route Finder panel), pick run A and run B. Every Route Finder result also has a **Compare with another run** link, and Grafana's tables open a run directly (`/route_run_history?run=<id>`; add `&a=<id>` to choose run A as well). The page shows:
- the choices, the data, run time and the counts side by side, and the step durations as bars
- **settings that differ**: the values used, what Spark reports, the cluster, and the category grouping (its file hash and the categories per place type). Identical settings show none
- resource figures side by side, including container CPU and peak memory from Prometheus
- **Same result?** for two succeeded runs with the same choices: the places near the route, their statuses, ranks, detours, positions and scores, and the cleaning counts (`route_compare.py`)

The page warns when the comparison isn't fair: different data (another import or multiplier), different choices, a run that didn't succeed, or a changed category grouping.

## Checks

```bash
make br6-test
```
Runs the unit tests, including the settings diff.

```bash
make br6-check-dashboards
```
Runs every panel query of the three BR6 dashboards through Grafana's API. It reports errors, and panels with no data in the dashboard's default time range.

## Notes

- **Spark's CPU figures leave out Python.** `spark.cpu_ms` counts the JVM task threads. The parallel reader, and the Python and pandas UDF variants of the match step, run in Python workers, so their cost shows in the container CPU of `spark-worker` but not in `spark.cpu_ms`.
- **Shared containers:** the busy lock keeps a BR5 job and a BR6 job from running at the same time, so a run's container figures are its own.
- **Event log:** the setting **Spark event log** (Advanced Settings, Monitoring) is on by default. With it off, a run has no Spark history page.
