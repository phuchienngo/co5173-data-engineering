# BR5 Guide: Monitoring and Run History

How to read what the BR5 pipeline did and what it cost. It covers [plan-monitoring.md](../../plan/br5/plan-monitoring.md). For changing the settings behind a run, see [config-guide.md](config-guide.md).

## Where to look

| What | Where | Use it for |
|---|---|---|
| Run History (app) | http://localhost:8501/run_history | Every import and run; comparing **two runs** side by side |
| Grafana, BR5 folder | http://localhost:3000/dashboards?tag=br5 (admin / admin) | Trends over many runs, CPU and memory over time |
| Spark history | http://localhost:18080 | One Spark run in detail: jobs, stages, tasks, the plans |
| Live Spark page | http://localhost:4040 | The Spark run in progress (the Promotion Finder links to it while a run is going) |
| Prometheus | http://localhost:9090 | Raw metrics, for queries the dashboards don't have |

Every app page has these links in the sidebar under **Monitoring**. `make br5-status` prints them.

## What is recorded for each run

Each import and analysis run is one row of `br5.pipeline_runs`. The view `br5.run_history` flattens it for queries. It holds:
- the type, status, error, times, and duration per step
- the choices, and the settings snapshot with where it came from (`settings_from`: "current settings" or "preset NAME")
- rows read, used, excluded per reason and written
- **resource figures** (`resources`), collected when the run ends:

| Figure | Meaning |
|---|---|
| `spark.cpu_ms` | CPU time of the executors' task threads. **The JVM only**: the parallel reader's Arrow work runs in Python workers and isn't counted. Container CPU (below) includes it |
| `spark.run_ms` | Task time summed over all tasks. More than the wall time when tasks run in parallel |
| `spark.gc_ms`, `spark.tasks`, `spark.stages`, `spark.jobs` | Garbage collection time and the work done |
| `spark.input_bytes`, `spark.shuffle_read_bytes`, `spark.shuffle_write_bytes`, `spark.spill_memory_bytes`, `spark.spill_disk_bytes` | Data moved. Reading cached data counts as input |
| `spark.executor_peak_heap_bytes`, `spark.driver_peak_heap_bytes` | Peak JVM heap, sampled at executor heartbeats (every 10 s), so approximate for short runs |
| `spark.<figure>@<step>` | The same per step (`read`, `clean`, `aggregate`, `profile`; for imports `read` … `write_fact`). `@setup` is work before the first step |
| `clickhouse.queries`, `query_ms`, `cpu_ms`, `peak_memory_bytes` | The run's ClickHouse queries, from `system.query_log`: analysis queries are tagged with the run id, and an import's queries are those on `br5_staging` |
| `clickhouse.read_rows`, `read_bytes`, `written_rows`, `written_bytes` | Rows and bytes ClickHouse read and wrote for the run |

Container CPU and memory are not stored with the run. They come from Prometheus every 5 s and are kept for 90 days. Run History computes them for the run's time window.

## Dashboards

All three have links to the app and to each other at the top.

**BR5 Run performance**:
- Filters at the top: type, status, engine, "settings from", multiplier, choice.
- **Compare by setting:** pick any key of the settings snapshot, for example `spark.sql.adaptive.enabled` or `pipeline.reader`. The bar chart and table give the runs, median, min and max per value of that key.
  - For a fair comparison, fix the choice and the multiplier in the filters first, so that only the chosen setting differs.
- Below that: the steps per run (grey = Spark start-up and other time outside the steps), Spark task time per step, the runs table and the resource figures per run.
- In the runs table, `run_id` opens the run in the app's Run History, and `spark_app_id` opens its Spark page.

**BR5 Resource use**:
- CPU (cores in use) and memory (working set, dashed = the limit) for `spark-worker` (where the executors run), `br5-app` (the Spark driver, and the app) and `clickhouse`.
- Each BR5 run is a shaded region; hover over its edge for the run's details.
- **To see what one run cost:** drag across its shaded region to zoom in. "CPU seconds used" and "Peak memory" then cover that run only.
- The Spark worker panel shows what the worker *gave* to executors, not what they used.

**BR5 Data and result**:
- The latest import (status, time, rows, multiplier) and the stored tables.
- For the latest succeeded analysis run: rows read, used and excluded per reason, its low season and its cards.

## Comparing two runs (app)

On **Run History**, pick run A and run B. Every Promotion Finder result also has a "Compare with another run" link. The page shows:
- run time and step durations side by side
- **settings that differ** (the values used, what Spark reports, the cluster); identical settings show none
- resource figures side by side, including container CPU and peak memory from Prometheus
- **Same result?** for two analysis runs with the same choices: the cards and monthly figures are compared to 2 decimals

The page warns when the comparison isn't fair: different data (another import or multiplier), different choices, or a run that didn't succeed.

## Spark history

- Every Spark run (analysis or import) writes an event log to `logs/spark-events` while the setting **Spark event log** is on (the default).
- The history server shows the run about 5 s after it ends. Spark jobs are named after the pipeline step that ran them.
- With the setting off, the run has no history page. One pair of runs showed no measurable difference in run time; each log is about 0.4 MB.
- Logs older than 30 days are removed by the history server.

## Checks

```bash
make br5-test
```
Runs the unit tests, including the figure parsing and the settings diff.

```bash
make br5-check-dashboards
```
Runs every panel query of the three dashboards through Grafana's API. It reports errors, and panels with no data in the dashboard's default time range.

## Notes

- **Where container figures come from:** `docker-stats` (`docker/docker-stats/exporter.py`) reads `docker stats` from the Docker socket. cAdvisor, the usual tool, can't identify containers on this Docker host (containerd image store). The metric names are cAdvisor's, so the dashboards would work with it.
- **Short runs:** a 2 s ClickHouse engine run gets at most one 5 s sample, so its container figures may be empty. Its stored ClickHouse figures are exact.
- **Monitoring's own cost:**
  - `docker-stats`: about 0.005 cores and 16 MB.
  - `spark-history`: about 0.01 cores and 240 MB.
  - Event logs: about 0.4 MB per run.
