# BR5 Result: Monitoring and Run History

End-to-end test of [plan-monitoring.md](../../plan/br5/plan-monitoring.md) on 2026-10-19. Same setup as [result-pipeline.md](result-pipeline.md): Spark 1 worker (2 cores, 2 GB), ClickHouse (1 CPU, 1 GB), data imported at ×1 (119,390 bookings). Runs are All / All / All unless named.

This checks that the monitoring works and its figures are right, and records a first set of figures. The benchmark matrix (repeated runs, ×10/×50, reader, cache and skew variants) is planned separately.

## What a run now records

Resource figures stored with each run (exact, collected when the run ends; `pipeline_runs.resources`):

| Figure | Spark default | Spark `baseline` preset | ClickHouse engine | Import (×1) |
|---|---|---|---|---|
| Run time | 25.7 s | 79.2 s | 2.2 s | 83.5 s |
| Spark CPU time (JVM task threads) | 3.8 s | 33.8 s | – | 35.3 s |
| Spark task time (summed over tasks) | 11.9 s | 56.4 s | – | 75.5 s |
| Spark GC time | 0.18 s | 2.47 s | – | 8.65 s |
| Spark stages / tasks | 18 / 21 | 44 / **4,245** | – | 67 / 2,236 |
| Shuffle written | 0.2 MB | 22.4 MB | – | 30.7 MB |
| Executor peak heap | 73 MB | 260 MB | – | 213 MB |
| ClickHouse queries | 2 | 1 | 4 | 34 |
| ClickHouse rows read | 238,780 (2 × 119,390) | 119,390 | 477,734 | 1,197,721 |
| ClickHouse rows written | 0 | 0 | 0 | 240,689 |
| ClickHouse CPU time / peak query memory | 0.06 s / 11 MB | 0.04 s / 38 MB | 0.13 s / 8 MB | 0.39 s / 28 MB |

Per step, Spark CPU time:

| Step | Default | Baseline |
|---|---|---|
| read | 1.87 s (the cached read; the Arrow work in Python isn't counted) | 0 (lazy: no cache, so nothing runs in this step) |
| clean | 0.25 s | 8.75 s |
| aggregate | 0.53 s | 8.28 s |
| profile | 1.15 s | 16.76 s |

The import's most expensive steps by Spark CPU are `derive` (13.6 s) and `write_dims` (10.9 s).

Container figures during the same runs (from Prometheus, 5 s samples; CPU seconds / peak memory):

| Container | Spark default | Spark `baseline` | Import |
|---|---|---|---|
| `spark-worker` (executor) | 24.9 s / 892 MB | 121.2 s / 1,103 MB | 128.5 s / 1,214 MB |
| `br5-app` (driver + app) | 21.7 s / 579 MB | 63.6 s / 1,011 MB | 52.8 s / 747 MB |
| `clickhouse` | 4.0 s / 417 MB | 11.6 s / 740 MB | 12.4 s / 718 MB |

## Observations for the report

- **The baseline's cost is in the shuffles.** With AQE off, every shuffle keeps Spark's 200 partitions: 4,245 tasks against 21 with the defaults. Its shuffle is 22.4 MB against 0.2 MB, because the early join moves every booking row and the late join moves only aggregates. That gives 9× the Spark CPU time (33.8 vs 3.8 s) and 3.1× the run time.
- **Spark's own CPU figure misses most of the work.** The `spark-worker` container used 24.9 CPU-seconds during the default run, but Spark reports 3.8 s of task CPU. The rest is the Python workers (the parallel reader's Arrow conversion), JVM start-up and JIT, and GC threads. Resource comparisons in the report should use the container figures, with Spark's as the breakdown per step.
- **The parallel reader scans the fact table once per slice.** ClickHouse read 238,780 rows for 119,390 bookings with 2 read partitions. It is cheap here (60 ms of ClickHouse CPU), but it grows with partitions × data size: a point for the ×50 benchmark.
- **The ClickHouse engine's work is tiny in ClickHouse terms.** 4 queries, 0.13 s of CPU and 8 MB peak memory for the whole analysis.
- **The import's 200 shuffle partitions show up again.** `write_dims` takes 10.9 s of Spark CPU to build a few hundred dimension rows (see result-pipeline.md).

## Behaviour verified

| # | Check | Result |
|---|---|---|
| 1 | Unit tests | 51 pass (43 earlier + 8 new: figure parsing, per-step attribution, steps add up to totals, settings diff) |
| 2 | `make br5-check-dashboards` | 3 dashboards in the BR5 folder, 37 panel and annotation queries, 0 errors, 0 empty |
| 3 | Figures per run (table above) | Spark totals equal the history server's for the same app: 18 stages, 3,798.8 ms CPU, 202,713 shuffle bytes. Steps add up to the totals. The ClickHouse engine has ClickHouse figures only. An import's rows written = 2 × 119,390 + 1,909 dimension rows exactly. The run time is unchanged: figures are collected after the run is marked finished |
| 4 | Resource use during runs | Each run is a shaded region. `spark-worker` CPU rises to about 2 cores inside it; memory stays under the limits (dashed) |
| 5 | Compare by `spark.sql.adaptive.enabled` | true: 12 runs, median 30.9 s; false: 3 runs, median 78.1 s. Equal to a direct query on `pipeline_runs` |
| 6 | Data and result dashboard against the app | Exclusion counts (canceled 18,919, zero nights 224, zero guests 20, no price 416) and the card (room A · Portugal, potential 595.1) are those of the latest run |
| 7 | Run History: default vs baseline | The settings that differ are exactly the baseline's 6 keys, Spark's effective values for AQE and broadcast, and "settings from". Same result: yes, 10 cards and 132 month rows. Two runs with the same settings show no difference |
| 8 | Links | Sidebar → Grafana and Spark history; R4 → the run's Spark page, Resource use for the run's time window, and "compare with another run"; Grafana tables → app Run History and Spark page; live Spark page (port 4040) shown and answering during a run |
| 9 | `docker compose … down` then `make br5-up` | 37 runs still listed. The history server lists all 8 runs with an event log. Prometheus still has the series from before the restart (retention 90 days). All dashboard queries pass |
| 10 | Stop; BR3/BR4 | A stopped run is recorded as "Stopped by the user" with its Spark and ClickHouse figures, and is shaded on Resource use; no Spark application or process is left. The CO5173 folder and its dashboard are unchanged |

## Monitoring's own cost

| Item | Figure |
|---|---|
| `docker-stats` exporter | about 0.005 cores, 16 MB, 0.06 s per scrape |
| `spark-history` server | about 0.01 cores, 240 MB |
| Spark event log | about 0.4 MB per run. Run time with it on: 25.8 and 24.6 s; off: 23.3 and 32.2 s. No difference shows at this noise level |

## Issues found and fixed during the test

- **cAdvisor can't identify containers on this Docker host** (containerd image store). The plan's fallback was built: `docker-stats`, with cAdvisor's metric names.
- **ClickHouse logs async inserts twice** (`Insert` and `AsyncInsertFlush`), which doubled the import's rows written. Rows now count the client entries only.
- **A stopped run lost its Spark figures**, because the py4j call after the interrupt couldn't be trusted. The driver page's address is now read when the session starts.
- **`make br5-up` didn't start the new services**: `scripts/br5.py` starts a fixed list of services, which now includes them.

## Not yet measured

- Repeated runs per variant, ×10 and ×50, reader / cache / skew variants (benchmark plan).
- Container figures over short windows include anything else the container did at the time: Grafana's own queries to ClickHouse while a dashboard is open, background merges, the app itself. Fair resource comparisons need a quiet machine and repeated runs.

*Fixed on 2026-10-24 (during BR6 monitoring):* the Data and result dashboard's *Import status* and *Imported at* stats showed "No data", because a stat panel reads only number fields by default. They now read every field. The dashboard check didn't catch it, because it counts the rows a query returns, not what the panel shows.
