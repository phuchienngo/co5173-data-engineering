# BR5 Guide: Settings That Need a Restart

Implements section 4 of [spec-pipeline-config.md](../../spec/br5/spec-pipeline-config.md) (C10, C11).

Most settings change live: edit them on the app's **Advanced Settings** page (http://localhost:8501), and the next run or import uses them. This guide covers the settings that are fixed when a container starts. Each one changes only after that container is restarted.

## Before you restart anything

- **Check that nothing is running.** Look at the app's Data page, or run `make br5-status`. Restarting Spark or ClickHouse during a run or an import kills it. A killed import leaves the stored data untouched; re-run it afterwards.
- **Run every command from the repository root.** The command below recreates one service with its new settings and leaves the others running:

  ```bash
  docker compose --profile full --profile monitoring --profile br5 up -d --force-recreate <service>
  ```

  `<service>` is the name in the table below. Data in the Docker volumes (ClickHouse tables, run history, Grafana) is kept.
- **Change one setting at a time** if you want to compare results (see [Fair comparison](#how-to-run-a-fair-comparison)).

## Settings

| Setting | File and line | Restart |
|---|---|---|
| [Spark worker memory and cores](#spark-worker-memory-and-cores) | `docker-compose.yml` lines 216–217 | `spark-worker` |
| [Container memory and CPU limits](#container-memory-and-cpu-limits) | `docker-compose.yml`, `mem_limit` / `cpus` of each service | the changed service |
| [Spark background memory](#spark-background-memory-spark_daemon_memory) | `docker-compose.yml` lines 190 and 218 | `spark-master`, `spark-worker` |
| [ClickHouse server settings](#clickhouse-server-settings) | `docker-compose.yml` lines 27–32 and 131–132 | `clickhouse` |
| [Prometheus scrape interval](#prometheus-scrape-interval) | `docker-compose.yml` line 43 | `prometheus` |

Line numbers are those of the version this guide was written with. If they have moved, search the file for the setting name.

### Spark worker memory and cores

- **What it does:** the total memory and cores the Spark worker offers to runs. It is the ceiling for the per-run settings "Executor memory", "Cores per executor" and "Total cores". The Advanced Settings page blocks a run whose executor can never fit in it.
- **Where:** `docker-compose.yml`, service `spark-worker`:
  ```yaml
  - SPARK_WORKER_MEMORY=2G     # line 216
  - SPARK_WORKER_CORES=2       # line 217
  ```
  Keep the container limit above the worker memory: the same service's `mem_limit` (line 208, `2500m`) must cover the worker memory plus about 300 MB for the worker process itself. Keep `cpus` (line 209) at least equal to the cores.
- **Sample value:** `SPARK_WORKER_MEMORY=3G`, `SPARK_WORKER_CORES=3`, with `mem_limit: 3500m` and `cpus: 3.0`. The laptop must have that much free memory.
- **Restart:** `spark-worker`.
- **Confirm:**
  - The "Needs restart" section at the bottom of Advanced Settings shows the new worker memory and cores.
  - Or the Spark master page (http://localhost:8082) lists the worker with them.

### Container memory and CPU limits

- **What it does:** caps how much memory and CPU each container may use. A container that goes over its memory limit is killed. In ClickHouse, a query fails instead, because ClickHouse keeps itself under 90% of its limit.
- **Where:** `docker-compose.yml`, `mem_limit` and `cpus` of each service:

  | Service | Lines | Current |
  |---|---|---|
  | `clickhouse` | 112–113 | 1g, 1.0 |
  | `kafka` (not used by BR5) | 147–148 | 1g, 1.0 |
  | `spark-master` | 185–186 | 512m, 0.5 |
  | `spark-worker` | 208–209 | 2500m, 2.0 |
  | `spark-local` | 239–240 | 6g, 2.0 |
  | `br5-app` (runs the Spark driver of every BR5 job) | 268–269 | 2g, 1.0 |

- **Sample value:** `clickhouse` with `mem_limit: 2g` and `cpus: 2.0`.
- **Restart:** the changed service. If you change `br5-app`, the app reloads; a job it started is stopped.
- **Confirm:**
  - Run `docker stats --no-stream` and check the MEM LIMIT column. For the CPU limit, `docker inspect --format "{{.HostConfig.NanoCpus}}" clickhouse` prints the CPU count × 10⁹.
  - For ClickHouse, the "Needs restart" section also shows its container memory and CPU limit.

### Spark background memory (`SPARK_DAEMON_MEMORY`)

- **What it does:** the memory of the Spark master and worker processes themselves, not of the runs. It only needs to grow with many workers or a long history of applications.
- **Where:** `docker-compose.yml`, `SPARK_DAEMON_MEMORY=256m` in `spark-master` (line 190) and `spark-worker` (line 218).
- **Sample value:** `512m`. Raise the container's `mem_limit` by the same amount.
- **Restart:** `spark-master` and `spark-worker` (restart the master first).
- **Confirm:** `docker top spark-worker` shows the Java command line with `-Xmx512m`.

### ClickHouse server settings

- **What it does:** settings for the whole ClickHouse server, as opposed to the per-query limits (`max_threads`, `max_memory_usage`) on the Advanced Settings page. Two useful ones:
  - `max_server_memory_usage_to_ram_ratio`: the share of the container's memory ClickHouse may use in total (default 0.9).
  - `background_pool_size`: the threads that merge table parts in the background, after imports.
- **Where:** `docker-compose.yml` has a commented example:
  1. Uncomment the `clickhouse-server-xml` config (lines 27–32) and set the values in it.
  2. Uncomment the two lines under the `clickhouse` service's `configs:` (lines 131–132). They mount the file as `/etc/clickhouse-server/config.d/server-tuning.xml`.
- **Sample value:** `<background_pool_size>4</background_pool_size>`.
- **Restart:** `clickhouse`. Its data is kept. The app and running jobs lose their connection while it restarts, so restart it only when nothing is running.
- **Confirm:**
  - The "Needs restart" section shows `background_pool_size` and the memory ratio.
  - Or run:
    ```bash
    docker exec clickhouse clickhouse-client --password clickhouse --query "SELECT name, value, changed FROM system.server_settings WHERE name IN ('background_pool_size', 'max_server_memory_usage_to_ram_ratio')"
    ```
    `changed = 1` means the new file was read.

### Prometheus scrape interval

- **What it does:** how often Prometheus collects metrics from ClickHouse and Spark. A run of about 30 s gets only 2 points at 15 s. A shorter interval gives smoother resource charts in Grafana, at the cost of more stored data.
- **Where:** `docker-compose.yml`, config `prometheus-yml`, `scrape_interval: 15s` (line 43).
- **Sample value:** `5s`.
- **Restart:** `prometheus`. Its stored metrics are kept.
- **Confirm:** http://localhost:9090/config shows the new `scrape_interval`.

## How to run a fair comparison

Run-time figures for the report are only comparable when everything except the setting under test stays the same.

1. **Fix the data.** Import once with the size you want to measure, for example `make br5-import MULTIPLIER=10 YES=1`, and don't re-import between the runs you compare.
2. **Fix the choices.** Use the same hotel, country and room type for every run. Use All / All / All for the heaviest case.
3. **Change one thing at a time.** Save each variant as a preset on the Advanced Settings page (for example `baseline`, `aqe-off`, `tuned`) instead of editing values between runs.
4. **Discard the first run after a restart.** It includes warm-up: images, caches, the JVM.
5. **Repeat each variant at least 3 times,** alternating the variants rather than running them in blocks, for example:
   ```bash
   make br5-run PRESET=baseline
   make br5-run PRESET=tuned
   ```
   Then compare **medians**, not single runs.
6. **Keep the machine quiet:** no other heavy programs, and no stack restarts between the runs you compare.

**Reading the results.** Every run records its settings, which preset they came from, the cluster it ran on and its step durations in `br5.pipeline_runs`. This query gives the median per preset and engine:

```sql
SELECT settings['launch.settings_from'] AS settings_from,
       settings['pipeline.engine'] AS engine,
       multiplier,
       count() AS runs,
       round(quantileExact(0.5)(duration_ms) / 1000, 1) AS median_seconds
FROM br5.pipeline_runs FINAL
WHERE run_type = 'analysis' AND status = 'succeeded'
GROUP BY settings_from, engine, multiplier
ORDER BY multiplier, median_seconds
```

To check that two runs gave the same answer (for example a tuned run against the baseline), run this. It compares the cards and the monthly figures:

```bash
docker exec br5-app python3 /opt/jobs/br5_promotion/compare_runs.py <run_id_1> <run_id_2>
```
