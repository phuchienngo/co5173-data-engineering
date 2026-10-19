# BR5 Plan: Monitoring and Run History

Implements [spec-monitoring.md](../../spec/br5/spec-monitoring.md) (M1–M8, the run history view, the links). It builds on the run history that [plan-data-import.md](plan-data-import.md) and [plan-pipeline.md](plan-pipeline.md) already record in `br5.pipeline_runs`.

Not covered: the detailed benchmark matrix (×50, reader, cache and skew variants, repeated runs). It gets its own test plan later and uses what this plan builds. Testing here is only enough to show the monitoring works and its figures are correct.

## 1. Where we start

M1–M5 are already recorded for every import and run: id, type, times, status and error, step durations, criteria, settings snapshot, rows read / used / excluded per reason, multiplier. What is missing:

- **Resource figures.** Prometheus scrapes the ClickHouse server and the Spark master and worker processes every 15 s. It sees nothing of a run itself. Each run is its own short Spark application (about 30 s), whose driver lives in `br5-app` and whose executor lives in `spark-worker`.
- **Step-level detail after a run.** The Spark web page of a run (port 4040 of the driver) disappears when the run ends, and the port isn't published.
- **The dashboards, the history screen and the links.**

## 2. Decisions taken in this plan

| # | Decision | Why |
|---|---|---|
| B1 | **Time series come from the containers, not from the Spark applications.** A new exporter service exports CPU and memory per container (as built: `docker-stats`, see the fallback below). Prometheus scrapes it every 5 s, and only this new job gets the shorter interval. Spark's executors run inside `spark-worker`, the driver inside `br5-app`, so the three containers `spark-worker`, `br5-app` and `clickhouse` cover a run completely, whatever engine it uses. | This settles the spec's open point. Scraping each run's own driver (`:4040/metrics`) gives a target that exists for 30 s and is down between runs. With a 15 s interval that is 2 points per run, and nothing at all for the ClickHouse engine. Container metrics don't depend on the run's lifetime, and they include the Python workers that Spark's own CPU figures leave out. |
| B2 | **Exact per-run figures are stored with the run** in a new `pipeline_runs.resources` map, collected by the job at its end. **Spark** figures come from the driver's REST API (`/api/v1/applications/<id>/…`): executor CPU and run time, GC time, input, shuffle read and write, spill, tasks, peak JVM heap. They are also given **per step**: each step sets its Spark job group to the step name. **ClickHouse** figures come from `system.query_log`: queries, duration, CPU time, peak memory, rows and bytes read and written. Analysis queries are tagged with `log_comment = run_id`; for imports, it is the inserts into the BR5 databases during the run. | These survive restarts and need no time window, so they are the evidence for the report (spec note 2). They are exact even for a 2 s ClickHouse engine run that no scrape catches. Collected after the run is marked finished and before Spark stops, so they don't add to the run time. |
| B3 | **A Spark history server** (new `spark-history` service, port 18080, profile `br5`) reads event logs written to `logs/spark-events`. Event logging is a new setting (`spark.eventLog.enabled`, default on). Every run records its `spark_app_id`. The app and Grafana link each Spark run to its page there. While a run is going, the app links to the driver's live page (port 4040, newly published on `br5-app`). | The spec asks for a link to the Spark web page for step-level detail. Without a history server, that page only exists while the run is going. As a setting, event logging can be switched off to measure its overhead. |
| B4 | **Grafana reads the run history straight from ClickHouse** (the datasource is already provisioned) for M6 and M8, and for the run start/end marks on M7 (annotations from `pipeline_runs`). Prometheus is used for the M7 time series only. | The history is already in ClickHouse and survives restarts. No new exporter is needed for run figures. |
| B5 | **Three dashboards in a new BR5 folder**, from a new `grafana/dashboards-br5/` directory with its own provider. The existing provider and folder stay as they are. | Grafana's file provider reads its folder recursively, so a `br5/` subfolder of `grafana/dashboards/` would be loaded twice. A separate directory keeps BR3/BR4 untouched. |
| B6 | **Prometheus keeps 90 days** instead of its default 15 (`--storage.tsdb.retention.time=90d`; the data is already in a volume). | Container time series for a run are then still there when the report is written. The exact per-run figures (B2) don't depend on it. |
| B7 | **A Run History page in the app** lists imports and runs, and compares two of them side by side: summary, step durations, settings that differ, resource figures, and "same result?" (the check of `compare_runs.py`, for runs with the same choices). It warns when two runs read different data or choices. | Spec section 1 (Should). Grafana is good for trends, but picking two runs and diffing their settings is easier in the app. |
| B8 | **New services go under profile `br5`**: `docker-stats` and `spark-history` (also in the service list of `scripts/br5.py`, which starts named services). The only changes to shared services: Prometheus gets the new scrape job and the longer retention, Grafana gets the new provider and mount, and `br5-app` publishes port 4040. | Keeps the BR3/BR4 demo stack the same size. BR3/BR4 jobs don't get event logging (it is set by BR5's settings only). |

**Fallback for B1, which was needed.** cAdvisor must work on this Docker host (Rancher Desktop on WSL2, cgroup v2), with container names as labels. It doesn't: Docker here uses the containerd image store, and cAdvisor fails with "failed to identify the read-write layer ID", so it only reports the whole machine.

What was built instead is `docker-stats` (`docker/docker-stats/exporter.py`):
- A small exporter of our own, Python standard library only.
- It reads the Docker API's stats through the socket, for the containers of this Compose project.
- It exports cAdvisor's metric names, so the dashboards would work with either: `container_cpu_usage_seconds_total`, `container_memory_working_set_bytes` (usage minus `inactive_file`), `container_spec_memory_limit_bytes`.
- It runs on the existing Spark image, so there is nothing new to pull.

## 3. Flow of a run, with monitoring

```
launch.py → spark-submit analyze.py (event log on → logs/spark-events/<app_id>)
  register run (+ spark_app_id)
  each step: run.step(name) → Spark job group = name; ClickHouse queries carry log_comment = run_id
  finish → status succeeded/failed (lock clears, duration fixed)
  collect figures (best effort, ≤ 5 s): Spark REST (totals + per step), SYSTEM FLUSH LOGS + query_log
  update run row with resources → spark.stop()

meanwhile: docker-stats ──5 s──▶ Prometheus (container CPU / memory)    ┐
           pipeline_runs (ClickHouse) ─────────────────────────────────┼─▶ Grafana M6 / M7 / M8
           spark-history ◀── event logs                                 ┘    app Run History
```

A stopped or failed run still gets its figures if Spark is alive at that point. If collection fails, the run keeps its status and the log says why.

As built:
- **The driver page's address and the app id are read once, when the session starts.** After a Stop interrupts a call to the JVM, the next py4j call can return a stale answer. The REST API is plain HTTP and still answers.
- **ClickHouse figures count each async insert once.** ClickHouse logs an async insert twice: the client's `Insert` and the server's `AsyncInsertFlush`. Rows and queries come from the client entries; CPU and memory include the flushes.

## 4. Files

| File | New/changed | Content |
|---|---|---|
| `jobs/br5_promotion/run_metrics.py` | new | `spark_figures(app_url, app_id)`: reads `allexecutors`, `jobs` and `stages`, and sums totals and per job group. The parsing is pure functions over the JSON, for tests. `clickhouse_figures(client, run)`: flushes the logs and reads `query_log`. Both return `{name: number}` |
| `jobs/br5_promotion/runs.py` | additive | `Run` fields `spark_app_id`, `resources`; an optional step hook (called with the step name); `run_history(client, filters)`, `run_pair(client, id1, id2)` |
| `jobs/br5_promotion/analyze.py`, `job.py` | small | Set the step hook to `setJobGroup`, add `log_comment` to the query settings (analysis), record the app id, collect figures after `finish` |
| `jobs/br5_promotion/compare_runs.py` | small | `differences()` reused by the app (already a function); new `settings_diff(a, b)` |
| `jobs/br5_promotion/run_settings.py` | additive | Setting `spark.eventLog.enabled` (group "Monitoring", default on), with `spark.eventLog.dir` fixed to `/opt/logs/spark-events` |
| `jobs/br5_promotion/run_metrics.py` (as built) | — | `prepare_event_log(conf)` creates `logs/spark-events` if missing (Spark refuses to start otherwise), called by `analyze.py` and `job.py` rather than `launch.py`, so `make demo-br BR=5` gets it too |
| `sql/clickhouse/ddl_br5.sql` | additive | `ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS spark_app_id String, resources Map(String, Float64)`; view `br5.run_history` (one flattened row per run: `settings_from`, `engine`, `choice`, `variant`, `outside_steps_s`), used by the dashboards and the app |
| `docker/docker-stats/exporter.py` | new | The container exporter (B1 fallback) |
| `docker-compose.yml` | additive | Services `docker-stats`, `spark-history` (profile `br5`); Prometheus scrape job `docker-stats` (5 s) and retention flag; Grafana provider `BR5` and mount of `grafana/dashboards-br5`; `br5-app` port 4040 |
| `grafana/dashboards-br5/br5-run-performance.json`, `br5-resource-use.json`, `br5-data-result.json` | new | Section 5 |
| `apps/br5/views/run_history.py` | new | Section 6 |
| `apps/br5/app.py`, `backend.py`, `views/promotion_finder.py`, `views/data.py` | small | New page; links to Grafana, Spark history and the live Spark page |
| `scripts/br5.py`, `Makefile` | small | `status` prints the monitoring URLs; the two new services are started with the others; `br5-check-dashboards` |
| `tests/br5/test_metrics.py` | new | Section 8 row 1 |
| `tests/br5/check_dashboards.py` | new | Runs every panel query of the BR5 dashboards through Grafana's API (filling in the variables, over each dashboard's own time range) and reports errors and empty results |
| `docs/guide/br5/monitoring-guide.md` | new | URLs, what each dashboard shows, how to compare by a setting, the Spark history page. The config guide's scrape-interval entry is updated |

## 5. Dashboards (BR5 folder)

Every dashboard has links to the app (Promotion Finder, Run History) and to the other two dashboards. Shared variables: time range, run type (import / analysis), status, engine, preset (`launch.settings_from`), multiplier.

**M6 Run performance**
- Run duration over time: one point per run, coloured by preset and engine.
- Duration per step: stacked bars for the last runs in the range.
- **Compare by a setting:** a variable lists every key in the settings snapshots (for example `spark.sql.adaptive.enabled`, `spark.sql.shuffle.partitions`). A bar chart and table show runs, median, min and max duration per value of that key. The other filters still apply, so a fair comparison is "same criteria, same multiplier, one key".
- Runs table: time, type, status, preset, engine, criteria, multiplier, duration, rows, and links to the app's Run History and to the Spark history page.
- Spark and ClickHouse figures per run (B2): CPU time, GC, shuffle, spill, ClickHouse rows read and peak memory.

**M7 Resource use** (Prometheus, 5 s)
- CPU (cores used) and memory (working set against the container limit) of `spark-worker`, `br5-app` and `clickhouse`.
- Spark worker cores and memory in use (existing worker metrics), and ClickHouse running queries and tracked memory (existing ClickHouse metrics).
- Every panel shows the runs as shaded regions from start to end, labelled with type, preset and status (annotation from `pipeline_runs`).

**M8 Data and result** (ClickHouse)
- Latest import: status, time, rows imported, multiplier, duration. Import history table.
- Rows excluded per reason, from the latest succeeded analysis.
- Latest recommendation: its choices, low-season text, and the cards (target, when, potential, discount).

## 6. Run History page (app)

- **Table** of imports and runs, newest first, with filters for type, status, engine and preset. Columns: time, type, status, choices, preset, engine, multiplier, duration, rows read / used, and links to the Spark history page and the run's result.
- **Compare two runs:** pick run A and run B (the latest two by default).
  - Summary side by side: status, duration, rows, choices, data (import and multiplier).
  - Step durations as grouped bars.
  - Settings that differ: resolved values, Spark's effective values and the cluster, with the other settings hidden.
  - Resource figures (B2) side by side.
  - **Same result?** For two analysis runs with the same choices: yes, or the first differences. Otherwise it says why it can't compare.
  - A warning when the runs read different imports or choices, or one of them didn't succeed.
- **Links** in the sidebar of every page: Grafana BR5 dashboards and the Spark history server. The Promotion Finder's progress area links to the live Spark page while a Spark run is going.

## 7. Order of work

| Step | Work | Done when |
|---|---|---|
| 1 | Container exporter (cAdvisor tried first; `docker-stats` built) + Prometheus job and retention; `spark-history`; event-log setting; port 4040 | Prometheus target `docker-stats` is up with series for `spark-worker`, `br5-app`, `clickhouse` by name; a run appears on the history server |
| 2 | DDL, `run_metrics.py`, step hook, `log_comment`, collection in `analyze.py` and `job.py`; unit tests | A Spark run, a ClickHouse engine run and an import each store their figures (row 3) |
| 3 | Grafana provider and the three dashboards; `check_dashboards.py` | All panel queries run without errors (row 2) |
| 4 | Run History page and links | Rows 6–7 checked in the browser |
| 5 | Guide, spec and plan updates, restart check, result file | Rows 8–10; `result-monitoring.md` written |

## 8. Verification (kept small on purpose)

| # | Check | Expected | Covers |
|---|---|---|---|
| 1 | Unit tests: figure parsing from sample REST JSON (totals, per step, missing fields, a run without Spark), `settings_diff`; the existing 43 tests | All pass | B2, B7 |
| 2 | `check_dashboards.py` after a few runs | The BR5 folder has 3 dashboards; every panel query returns without error, and the panels that need runs have data | M6–M8 |
| 3 | One run each: Spark default, Spark `baseline`, ClickHouse engine (All / All / All, ×1), and one import | Spark runs: `spark_app_id` is set and CPU time > 0. The per-step figures sum to no more than the totals. The baseline has more shuffle than the default. The ClickHouse engine has ClickHouse figures only. The default run's ClickHouse rows read include 2 × 119,390 fact rows (2 read slices, each scanning the table). The Spark totals match the history server's pages for the same app | B2, B3 |
| 4 | M7 during the default run | `spark-worker` CPU rises inside the shaded region of that run; memory stays below the limit; `br5-app` memory rises with the driver | B1, M7 |
| 5 | M6 "compare by" `spark.sql.adaptive.enabled` | Two values, with run counts equal to a direct query on `pipeline_runs` | M6 |
| 6 | M8 against the app | Excluded per reason and the top card match the Promotion Finder's R4 and first card | M8 |
| 7 | Run History: compare default vs baseline, and two identical runs | The settings that differ are exactly the baseline's keys (plus their effective values). Same result: yes. The identical runs show no setting differences | B7 |
| 8 | Links | App → Grafana and → Spark history (job descriptions are the step names); Grafana → app; the live link works during a run | Spec 2 |
| 9 | `docker compose … down` then `up` | Run history, M6, M8, Spark history pages and the earlier M7 series are all still there | Spec note 2, B6 |
| 10 | A stopped run; the BR3/BR4 dashboard | The stopped run is listed and marked in M7. The CO5173 folder and its dashboard are unchanged | A10, BR3/BR4 |

The result file records a few figures only:
- the figures of the three runs in row 3 (CPU time, shuffle, ClickHouse rows read, peak memory per container)
- the run time with event logging on and off (one pair, as a first look at its cost)
- the exporter's and the history server's own CPU and memory use

## 9. Risks and notes

- **cAdvisor on Rancher Desktop** may not name containers or may miss cgroup v2 stats. Step 1 checks this first; the fallback is in B1. *Happened: the fallback is what was built.*
- **Spark's CPU time leaves out Python.** `executorCpuTime` measures the JVM task threads only. The parallel reader's Arrow work runs in Python workers, so the read step looks cheap in Spark's figures. The container CPU (B1) includes it; the guide says so.
- **Peak JVM heap** comes from executor heartbeats (every 10 s), so it is approximate for short runs. The container memory peak is the cross-check.
- **Short runs at a 5 s interval** still give only a few points. That is fine for spotting where the time and memory go; exact totals come from B2.
- **The parallel reader scans the fact table once per slice** (`source_row % N`). The ClickHouse figures will show this. It is a finding for the benchmark, not a bug to fix here.
- **Event logs grow** with every run. The history server cleans logs older than 30 days.
- **Monitoring has its own cost** (the exporter's CPU, the history server, the event log). Both are measured once in the result file. A setting turns the event log off for benchmarks that need it.
