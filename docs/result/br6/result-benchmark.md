# BR6 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br6/plan-benchmark.md). **This file is updated after each block** of the benchmark.

## Progress

| Block | Configuration | Status | Runs |
|---|---|---|---|
| H1 | ClickHouse engine, ClickHouse at 1 CPU | **Done** (2026-11-03) | 1 warm-up + 60 measured, all succeeded |
| H2, H3 | ClickHouse at 2 and 4 CPUs | Not run yet | |
| S1, S2, S3 | Spark worker at 1, 2 and 4 cores | Not run yet | |
| V | Single options on 2 cores | Not run yet | |

## Setup

- **Stack:** as in [result-monitoring.md](result-monitoring.md):
  - ClickHouse: 1 GB; its CPUs are the block's (`BR5_CLICKHOUSE_CPUS`).
  - Spark: 1 worker with 2 GB; its cores are the block's (`BR5_WORKER_CORES`).
  - The app and the ClickHouse engine's Python job run in `br5-app` (1 CPU, 2 GB).
  - The Docker host has 14 CPUs and 16 GB.
- **Data:** the ×1 import of 2026-10-24 (2,629,541 check-ins, 15 cities), for the whole benchmark (plan D1).
- **Inputs** (plan 3.1), all with every place type:

  | Input | Route | Visitor | Check-ins read | Places near the route | Suggested |
  |---|---|---|---|---|---|
  | I1 Istanbul | Eminönü → Ortaköy → Fatih Sultan Mehmet Köprüsü, 11.4 km | 88647 | 544,471 | 4,574 (1,291 after the filters) | 10 |
  | I2 Jakarta | Emporium Pluit Mall → Gandaria City, 13.0 km | General | 412,100 | 7,178 | 10 |
  | I3 Beijing | IKEA → Joy City Chaoyang | General | 1,470 | 61 | 4 |

- **Measured runs:** 5 per ClickHouse cell, interleaved across the cells, after one warm-up. Every run is listed in [benchmark/runs.csv](benchmark/runs.csv).
- **How figures are given:** the median of a cell's runs, each figure separately. Query time, ClickHouse CPU, peak threads, peak memory and rows read are ClickHouse's own figures from `system.query_log`, for the run's queries (tagged with the run id).

## ClickHouse blocks (H1–H3)

The ClickHouse engine: a Python job in `br5-app` that sends 2 SQL queries (3 with a chosen visitor) to ClickHouse; no Spark. Live settings (plan 3.5):

| Live | Preset | Place statistics | ClickHouse threads |
|---|---|---|---|
| HL1 | `bench-clickhouse` | from the import (one row per place) | the default: one thread in a 1 GB container (BR5's finding) |
| HL2 | `bench-clickhouse-nomemlimit` | from the import | memory-per-thread limit off (`max_threads_min_free_memory_per_thread = 0`) |
| HL3 | `bench-clickhouse-stats-run` | counted from the city's check-ins in every query | the default |
| HL4 | `bench-clickhouse-stats-run-nomemlimit` | counted every run | limit off |

### H1: 1 CPU (median of 5)

"Inside the steps" leaves out starting Python and connecting (1.3–1.5 s in every run).

| Input | Cell | Run time (s) | Inside the steps (s) | Query time (ms) | ClickHouse CPU (ms) | Peak threads | Peak query memory (MB) | Rows scanned |
|---|---|---|---|---|---|---|---|---|
| I1 | HL1 | 1.82 | 0.40 | 121 | 112 | 1 | 24.3 | 841,693 |
| | HL2 | 2.04 | 0.51 | 125 | 120 | 1 | 24.3 | 841,693 |
| | HL3 | 2.11 | 0.63 | 282 | 211 | 1 | 48.2 | 2,327,650 |
| | HL4 | 2.17 | 0.63 | 287 | 233 | 1 | 48.2 | 2,327,650 |
| I2 | HL1 | 2.08 | 0.65 | 200 | 140 | 1 | 35.1 | 329,220 |
| | HL2 | 1.78 | 0.49 | 137 | 118 | 1 | 35.1 | 329,220 |
| | HL3 | 2.07 | 0.66 | 219 | 219 | 1 | 47.7 | 989,580 |
| | HL4 | 1.91 | 0.64 | 288 | 199 | 1 | 47.7 | 989,580 |
| I3 | HL1 | 1.71 | 0.26 | 32 | 32 | 1 | 7.1 | 34,308 |
| | HL2 | 1.58 | 0.26 | 31 | 29 | 1 | 7.1 | 34,308 |
| | HL3 | 1.76 | 0.27 | 45 | 37 | 1 | 7.2 | 20,864 |
| | HL4 | 1.68 | 0.27 | 46 | 42 | 1 | 7.2 | 20,864 |

Peak threads include the query's own thread. A few runs show 2 (a join's second thread); the median is 1 everywhere. "Rows scanned" counts the index granules ClickHouse read, once per query (each query reads the city's places, and with statistics every run also the city's check-ins), not the rows returned.

**Observations (H1):**
- **On 1 CPU the four settings are the same in total:** 1.6–2.2 s, with 0.3–0.7 s inside the steps. Most of a run is starting the Python job and connecting.
- **The memory-per-thread limit doesn't matter on 1 CPU,** as expected: ClickHouse's default is `auto(1)`, so a query has one thread either way. HL2 against HL1 and HL4 against HL3 differ only by noise (I1: 121 / 125 ms; 282 / 287 ms). H2 and H3 are where it can show.
- **Counting the statistics every run costs ClickHouse little.** On I1 (544,471 check-ins) the queries scan 2.8× the rows (0.84 → 2.33 million), take 2.3× the query time (121 → 282 ms) and twice the peak memory (24 → 48 MB): about 0.2 s more per run. On I2 the query time grows by 10–40%, and on I3 (1,470 check-ins) by about 13 ms. Compare Spark, where the same option took a run from 30.5 to 51.7 s (result-pipeline.md, route A).
- **ClickHouse's work follows the input:** I1's queries scan 25× the rows of I3's and take 4–6× the query time. The run time hardly follows it (1.82 against 1.71 s with HL1).

## Noise

- **H1:** the runs of a cell varied by 0.1–0.6 s. The widest were one I3-HL1 run (2.60 s against 1.63–1.72) and the I2-HL1 cell (1.77–2.48 s). The slow time was outside the queries, so I2-HL1's median of 2.08 s against HL2's 1.78 s is noise: the two do the same work on 1 CPU. Differences under about 0.3 s in run time between H1 cells should not be read as effects; the query figures are steadier.

## Behaviour verified (plan section 8)

| # | Check | Result |
|---|---|---|
| 3 | Inputs read what the plan says | Every H1 run: 544,471 / 412,100 / 1,470 check-ins read, 4,574 / 7,178 / 61 places near the route |
| 4 | Same input, same result | Every H1 run gives the same list as the first run of its input (`route_compare.py`): the same places, statuses, ranks, scores and cleaning counts, with either statistics mode and either thread setting. The comparison with Spark's S2 L2 reference follows when S2 is run |
| 5 | All succeeded | 61 of 61 |
| 6 | Screenshots | 12 `finder.jpg` and 9 `compare.jpg`. Samples checked by eye: the result, the run summary, the one setting that differs, and "Same result: yes" |
| 9 | ClickHouse start-up | At the start and end of H1: CPU limit 1, `max_threads` `auto(1)`; every run's snapshot has `cluster.clickhouse_max_threads` = `auto(1)` |
| 10 | ClickHouse live settings | HL2 and HL4: every query carries `max_threads_min_free_memory_per_thread` = 0; HL1 and HL3: none. HL3 and HL4 scan the city's check-ins (2.3 million rows on I1 against 0.84 million) |
| 11 | Threads and cache | Every query on 1 thread at the median; no query used the query cache |

## Issues found and fixed

- **`docker-compose.yml` tied two other containers to the ClickHouse size.** BR5's ClickHouse blocks changed every `cpus: 1.0` in the file to `${BR5_CLICKHOUSE_CPUS:-1}`, so `kafka` and `br5-app` followed the variable too. At its default of 1 nothing changed, and BR5's blocks recreated only `clickhouse`, so BR5's figures are not affected. Both are back to `cpus: 1.0`, so the H2 and H3 blocks change ClickHouse only.

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `H1-I1-HL1/`), from the cell's first measured run, stored with Git LFS:

| File | Shows |
|---|---|
| `finder.jpg` | The Route Finder result of the run (`?run=<id>`), with the run summary: check-ins, steps, settings used |
| `compare.jpg` | Run History against the reference run: run time, steps, settings that differ, ClickHouse figures, same result |

References: HL2 and HL3 against HL1 of the same input and block; HL4 against HL3. H1's HL1 cells will be compared with S2's L2 (Spark against ClickHouse) once S2 has run; HL1 of H2 and H3 with H1's HL1.

Known limit: with `?run=<id>`, the Route Finder sets the city to the run's, but the choice boxes above the result keep their defaults (for example "General visitor" for I1). The result, headed with its own route and visitor, is the run's.

## Appendix: every run (seconds, in order)

| Cell | H1 (1 CPU) |
|---|---|
| I1-HL1 | 1.78, 1.77, 1.82, 1.95, 2.00 |
| I1-HL2 | 2.04, 2.00, 1.96, 2.19, 2.12 |
| I1-HL3 | 2.16, 2.23, 2.11, 2.01, 1.94 |
| I1-HL4 | 2.17, 2.24, 2.06, 2.27, 2.17 |
| I2-HL1 | 2.48, 2.08, 2.38, 1.87, 1.77 |
| I2-HL2 | 1.67, 1.70, 1.78, 1.96, 1.86 |
| I2-HL3 | 2.30, 1.98, 2.07, 1.95, 2.28 |
| I2-HL4 | 2.01, 1.91, 2.44, 1.88, 1.88 |
| I3-HL1 | 1.71, 1.63, 2.60, 1.69, 1.72 |
| I3-HL2 | 1.64, 1.73, 1.58, 1.56, 1.52 |
| I3-HL3 | 1.49, 1.76, 1.86, 1.87, 1.71 |
| I3-HL4 | 2.13, 1.67, 1.76, 1.68, 1.64 |

Warm-up (not counted): I1-HL1, 2.09 s.
