# BR6 Guide: Settings That Need a Restart

Implements section 4 of [spec-pipeline-config.md](../../spec/br6/spec-pipeline-config.md) (C13, C14).

Most settings change live. Edit them on the app's **Advanced Settings** page: open http://localhost:8501/choose_settings and pick **Route Finder**. The next run or import uses them. This guide covers the settings that are fixed when a container starts; each changes only after that container is restarted.

**The stack is shared with BR5** (and the other BRs). Every restart below affects BR5 too, and the same settings appear in [BR5's guide](../br5/config-guide.md).

## Before you restart anything

- **Check that nothing is running, in BR5 or BR6.** Look at the app's Data page (either panel), or run `make br6-status` and `make br5-status`.
  - Restarting Spark or ClickHouse during a run or an import kills it. A killed import leaves the stored data untouched; re-run it afterwards.
  - **Restarting `br5-app` closes both BRs' pages** and stops any job started from the app.
- **Run every command from the repository root.** The command below recreates one service with its new settings and leaves the others running:

  ```bash
  docker compose --profile full --profile monitoring --profile br6 up -d --force-recreate <service>
  ```

  `<service>` is the name in the table below. Data in the Docker volumes (ClickHouse tables, run history, Grafana) is kept.
- **Don't run BR5 and BR6 at the same time while measuring.** They share the Spark worker. A BR6 job refuses to start while a BR5 job runs, but BR5 doesn't check BR6, so a BR5 run started during a BR6 run waits for cores and spoils both timings.

## Settings

| Setting | File and line | Restart |
|---|---|---|
| [Spark worker memory and cores](#spark-worker-memory-and-cores) | `docker-compose.yml` lines 235–236 (cores: also `BR5_WORKER_CORES`) | `spark-worker` |
| [Container memory and CPU limits](#container-memory-and-cpu-limits) | `docker-compose.yml`, `mem_limit` / `cpus` of each service | the changed service |
| [Spark background memory](#spark-background-memory-spark_daemon_memory) | `docker-compose.yml` lines 208 and 237 | `spark-master`, `spark-worker` |
| [ClickHouse server settings](#clickhouse-server-settings) | `docker-compose.yml` lines 27–32 and 149–150 | `clickhouse` |
| [Prometheus scrape interval](#prometheus-scrape-interval) | `docker-compose.yml` lines 43 and 68 | `prometheus` |

Line numbers are those of the version this guide was written with. If they have moved, search the file for the setting name.

### Spark worker memory and cores

- **What it does:** the total memory and cores the Spark worker offers to runs and imports. It is the ceiling for the per-run settings "Executor memory", "Cores per executor" and "Total cores". The Advanced Settings page blocks a job whose executor can never fit in it.
- **Where:** `docker-compose.yml`, service `spark-worker`:
  ```yaml
  - SPARK_WORKER_MEMORY=2G                         # line 235
  - SPARK_WORKER_CORES=${BR5_WORKER_CORES:-2}     # line 236
  ```
  Keep the container limit above the worker memory: the same service's `mem_limit` (line 226, `2500m`) must cover the worker memory plus about 300 MB for the worker process. `cpus` (line 228) follows the cores. The variable `BR5_WORKER_CORES` (its name comes from BR5) sets both cores and `cpus` without editing the file:
  ```bash
  BR5_WORKER_CORES=4 docker compose --profile full up -d --no-deps spark-worker
  ```
- **Sample value:** `SPARK_WORKER_MEMORY=3G`, `SPARK_WORKER_CORES=3`, with `mem_limit: 3500m`. The laptop must have that much free memory.
- **Restart:** `spark-worker`.
- **Confirm:** the "Needs restart" section at the bottom of the Route Finder's Advanced Settings shows the new worker memory and cores, or the Spark master page (http://localhost:8082) lists the worker with them.

### Container memory and CPU limits

- **What it does:** caps how much memory and CPU each container may use. A container that goes over its memory limit is killed. In ClickHouse, a query fails instead, because ClickHouse keeps itself under 90% of its limit.
- **Where:** `docker-compose.yml`, `mem_limit` and `cpus` of each service:

  | Service | Lines | Current |
  |---|---|---|
  | `clickhouse` | 130–131 | 1g, 1.0 |
  | `spark-master` | 203–204 | 512m, 0.5 |
  | `spark-worker` | 226 and 228 | 2500m, `BR5_WORKER_CORES` (default 2) |
  | `br5-app` (the shared app; runs the Spark driver of every BR5 and BR6 job) | 287–288 | 2g, 1.0 |
  | `spark-history` | 331–332 | 512m, 0.5 |
  | `docker-stats` | 361–362 | 128m, 0.25 |
  | `kafka` (not used by BR6) | 165–166 | 1g, 1.0 |

- **ClickHouse's 1 GB is tight for BR6 imports.** ClickHouse keeps its tracked memory under 90% of the limit (about 920 MiB). When an insert would pass it, the insert fails, Spark retries the task, and the retried task inserts its rows again. The import's verify step catches the extra rows and fails the import, so the stored data stays as it was.
  - The big consumer was ClickHouse's own `system.metric_log` (about 2,100 columns): each of its background merges peaked at about 730 MiB. `docker-compose.yml` now turns it off (config `clickhouse-system-logs-xml`, mounted as `config.d/system-logs.xml`). Nothing in the project reads it; Grafana uses the Prometheus endpoint. See [result-pipeline.md](../../result/br6/result-pipeline.md).
  - To turn it back on, remove that config and its mount, then restart `clickhouse`. If imports fail with "memory limit exceeded" again, raise ClickHouse's `mem_limit`.
- **Sample value:** `clickhouse` with `mem_limit: 2g` and `cpus: 2.0`.
- **Restart:** the changed service. If you change `br5-app`, the app reloads for both BRs; a job it started is stopped.
- **Confirm:** run `docker stats --no-stream` and check the MEM LIMIT column. For the CPU limit, `docker inspect --format "{{.HostConfig.NanoCpus}}" clickhouse` prints the CPU count × 10⁹. For ClickHouse, the "Needs restart" section also shows its container memory and CPU limit.

### Spark background memory (`SPARK_DAEMON_MEMORY`)

- **What it does:** the memory of the Spark master and worker processes themselves, not of the jobs. It only needs to grow with many workers or a long history of applications.
- **Where:** `docker-compose.yml`, `SPARK_DAEMON_MEMORY=256m` in `spark-master` (line 208) and `spark-worker` (line 237). The `spark-history` service has its own (line 339).
- **Sample value:** `512m`. Raise the container's `mem_limit` by the same amount.
- **Restart:** `spark-master` and `spark-worker` (the master first).
- **Confirm:** `docker top spark-worker` shows the Java command line with `-Xmx512m`.

### ClickHouse server settings

- **What it does:** settings for the whole ClickHouse server, as opposed to the per-query limits (`max_threads`, `max_memory_usage`) on the Advanced Settings page. Two useful ones:
  - `max_server_memory_usage_to_ram_ratio`: the share of the container's memory ClickHouse may use in total (default 0.9).
  - `background_pool_size`: the threads that merge table parts in the background, after imports.
- **Where:** `docker-compose.yml` has a commented example:
  1. Uncomment the `clickhouse-server-xml` config (lines 27–32) and set the values in it.
  2. Uncomment the two lines under the `clickhouse` service's `configs:` (lines 149–150). They mount the file as `/etc/clickhouse-server/config.d/server-tuning.xml`.
- **Sample value:** `<background_pool_size>4</background_pool_size>`.
- **Restart:** `clickhouse`. Its data is kept. The app and running jobs lose their connection while it restarts.
- **Confirm:** the "Needs restart" section shows `background_pool_size` and the memory ratio, or run:
  ```bash
  docker exec clickhouse clickhouse-client --password clickhouse --query "SELECT name, value, changed FROM system.server_settings WHERE name IN ('background_pool_size', 'max_server_memory_usage_to_ram_ratio')"
  ```
  `changed = 1` means the new file was read.

### Prometheus scrape interval

- **What it does:** how often Prometheus collects metrics. A shorter interval gives smoother resource charts in Grafana, at the cost of more stored data.
  - `scrape_interval: 15s` (line 43): the default, for ClickHouse's and Spark's own metrics. A Route Finder run of about 30 s gets only 2 points.
  - `scrape_interval: 5s` (line 68): the `docker-stats` job, which feeds the per-container CPU and memory charts.
- **Where:** `docker-compose.yml`, config `prometheus-yml`.
- **Sample value:** `5s` for the default, or `2s` for `docker-stats`.
- **Restart:** `prometheus`. Its stored metrics are kept.
- **Confirm:** http://localhost:9090/config shows the new `scrape_interval`.

## How to run a fair comparison

Run-time figures for the report are only comparable when everything except the setting under test stays the same.

1. **Fix the data.** Import once with the size you want to measure, for example `make br6-import MULTIPLIER=5 YES=1`, and don't re-import between the runs you compare.
2. **Fix the choices.** The same city, start, stops, end, place types and visitor for every run, and the same analysis rules. Istanbul (most check-ins) or Jakarta (most places) are the heaviest cases.
3. **Change one thing at a time.** Save each variant as a preset on the Advanced Settings page (for example `grid-udf`, `stats-run`) instead of editing values between runs. The built-in `baseline` preset changes all the optimizations at once.
4. **Discard the first run after a restart.** It includes warm-up: caches and the JVM.
5. **Repeat each variant at least 3 times,** alternating the variants, for example:
   ```bash
   make br6-run CITY=jakarta START=1549 END=1373 PRESET=baseline
   make br6-run CITY=jakarta START=1549 END=1373 PRESET=default
   ```
   Then compare **medians**, not single runs.
6. **Keep the machine quiet:** no other heavy programs, no BR5 jobs, and no stack restarts between the runs you compare.

**Reading the results.** Every run records its settings, where they came from, the cluster it ran on and its step durations in `br6.pipeline_runs`. This query gives the median per preset and engine:

```sql
SELECT settings['launch.settings_from'] AS settings_from,
       settings['pipeline.engine'] AS engine,
       criteria['city'] AS city,
       multiplier,
       count() AS runs,
       round(quantileExact(0.5)(duration_ms) / 1000, 1) AS median_seconds,
       round(quantileExact(0.5)(step_durations['match']) / 1000, 2) AS median_match_seconds
FROM br6.pipeline_runs FINAL
WHERE run_type = 'run' AND status = 'succeeded'
GROUP BY settings_from, engine, city, multiplier
ORDER BY city, multiplier, median_seconds
```

To check that two runs gave the same answer (spec C9 and C10: the matching methods, the distance functions and the two engines may only change the speed), run:

```bash
docker exec br5-app python3 /opt/jobs/br6_route/route_compare.py <run_id_1> <run_id_2>
```

It compares every place found, with its status, rank, detour, position and scores, and the check-in counts.
