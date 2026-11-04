# BR6 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br6/plan-benchmark.md). **This file is updated after each block** of the benchmark.

## Progress

| Block | Configuration | Status | Runs |
|---|---|---|---|
| H1 | ClickHouse engine, ClickHouse at 1 CPU | **Done** (2026-11-03) | 1 warm-up + 60 measured, all succeeded |
| H2 | ClickHouse at 2 CPUs | **Done** (2026-11-03, second attempt) | 1 warm-up + 60 measured, all succeeded. The first attempt ran at 1 CPU and is void (see "Issues") |
| H3 | ClickHouse at 4 CPUs | **Done** (2026-11-03) | 1 warm-up + 60 measured, all succeeded |
| S1 | Spark worker at 1 core | **Done** (2026-11-04) | 1 warm-up + 21 measured (2 per cell, a third for I1-L2, I2-L2 and I2-L3), all succeeded |
| S2 | Spark worker at 2 cores | **Done** (2026-11-04) | 1 warm-up + 19 measured (2 per cell, a third for I1-L2), all succeeded |
| S3 | Spark worker at 4 cores | **Done** (2026-11-03) | 1 warm-up + 20 measured (2 per cell, a third for I1-L1 and I3-L3), all succeeded |
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

- **Measured runs:** 2 per Spark cell (a third when the two differ by more than 15%), 5 per ClickHouse cell, interleaved across the cells, after one warm-up per block. Every run is listed in [benchmark/runs.csv](benchmark/runs.csv).
- **How figures are given:** the median of a cell's runs, each figure separately. Query time, ClickHouse CPU, peak threads, peak memory and rows read are ClickHouse's own figures from `system.query_log`, for the run's queries (tagged with the run id).

## Spark blocks (S1–S3)

The main solution: a Spark application per run, submitted from `br5-app` (the driver, 1 CPU, 512 MB) to the worker (one executor, 512 MB). Live settings (plan 3.3), all with `spark.cores.max=4` so a run takes every core the worker has:

| Live | Preset | What it changes from the default |
|---|---|---|
| L1 | `bench-baseline` | The built-in naive variant: driver reader, Python UDF distance, every place matched (no grid), statistics every run, AQE off, no broadcast joins |
| L2 | `bench-default` | Nothing: statistics from the import, parallel reader, grid matching, native distance |
| L3 | `bench-stats-run` | Place statistics counted from the city's check-ins every run (`pipeline.place_stats=run`) |

### Run time and steps (median)

Each figure is given as S1 / S2 / S3 (1 / 2 / 4 worker cores). Steps are the job's own timings, in seconds. "Start-up" is the run time outside the steps: spark-submit, the driver's start and the Spark session. Spark CPU is the executor's, summed over its tasks.

| Input | Cell | Run time (s) | Start-up | read | clean | match | visitor | Spark CPU (s) | Shuffle (MB) |
|---|---|---|---|---|---|---|---|---|---|
| I1 Istanbul | L1 baseline | 131.0 / 71.4 / 68.8 | 13.1 / 9.9 / 10.7 | 8.9 / 5.5 / 6.1 | 30.8 / 15.9 / 14.9 | 69.5 / 34.8 / 30.1 | 8.3 / 5.0 / 5.0 | 21.2 / 20.9 / 35.0 | 14.6 |
| | L2 default | 43.9 / 29.7 / 28.6 | 14.5 / 11.7 / 10.9 | 15.3 / 9.6 / 9.6 | 2.6 / 1.7 / 1.7 | 9.6 / 6.1 / 5.5 | 1.3 / 0.7 / 0.7 | 2.9 / 3.0 / 4.5 | 0.3 |
| | L3 statistics every run | 77.9 / 47.8 / 47.5 | 12.9 / 10.2 / 11.1 | 38.5 / 22.8 / 23.2 | 7.6 / 3.8 / 3.3 | 14.3 / 8.5 / 6.9 | 4.3 / 2.3 / 2.6 | 11.8 / 12.6 / 16.2 | 5.1 |
| I2 Jakarta | L1 | 122.0 / 69.8 / 61.4 | 12.8 / 10.2 / 11.2 | 8.5 / 5.2 / 5.9 | 27.8 / 17.1 / 14.8 | 72.6 / 37.0 / 29.1 | – | 20.4 / 21.5 / 31.2 | 14.3 |
| | L2 | 40.6 / 31.0 / 30.3 | 12.9 / 10.3 / 10.8 | 16.3 / 12.0 / 10.7 | 2.3 / 1.8 / 1.8 | 9.3 / 6.7 / 6.7 | – | 2.6 / 3.3 / 5.0 | 0.4 |
| | L3 | 78.0 / 49.9 / 46.3 | 13.6 / 10.9 / 11.2 | 36.9 / 25.0 / 23.6 | 7.2 / 4.1 / 3.2 | 20.1 / 9.6 / 8.0 | – | 12.0 / 13.2 / 15.7 | 4.3 |
| I3 Beijing | L1 | 102.4 / 64.3 / 55.4 | 14.0 / 10.9 / 11.6 | 5.6 / 4.5 / 4.9 | 27.7 / 15.5 / 13.7 | 54.9 / 33.2 / 24.9 | – | 13.1 / 15.3 / 19.5 | 0.2 |
| | L2 | 36.2 / 27.0 / 28.1 | 12.3 / 10.1 / 11.3 | 13.5 / 10.0 / 9.1 | 2.4 / 1.4 / 1.7 | 7.9 / 5.3 / 5.9 | – | 2.1 / 2.0 / 3.1 | 0.0 |
| | L3 | 68.7 / 48.4 / 41.3 | 13.4 / 13.3 / 10.7 | 34.3 / 22.3 / 20.3 | 6.3 / 4.6 / 3.2 | 14.5 / 8.0 / 7.7 | – | 9.4 / 9.2 / 11.2 | 0.1 |

Start-up is the median run time minus the median sum of the steps. Score and write take 0.1–0.3 s in every cell. The visitor step runs only for I1 (a chosen visitor). Shuffle is the same on every size (within 0.5 MB). The driver's heap peaks at 340–400 MB of its 512 MB for L1 on I1 and I2 (the driver reader holds the check-ins), and at 100–160 MB elsewhere.

### Effect of the worker's cores

Median run time relative to 2 cores (S2), per live setting, over the three inputs:

| Live | 1 core (S1) | 2 cores (S2) | 4 cores (S3) |
|---|---|---|---|
| L1 baseline | 1.59–1.83× (+38 to +60 s) | 1 | 0.86–0.96× (−3 to −9 s) |
| L2 default | 1.31–1.48× (+9 to +14 s) | 1 | 0.96–1.04× (within the noise) |
| L3 statistics every run | 1.42–1.63× (+20 to +30 s) | 1 | 0.85–0.99× (0 to −7 s) |

### Observations (S1–S3)

- **Going from 1 to 2 cores pays off; going from 2 to 4 hardly does.** On 1 core the baseline takes 102–131 s, nearly twice its time on 2 cores, and the default 36–44 s instead of 27–31 s. From 2 to 4 cores the default doesn't change and the other two gain 0–9 s.
- **One core is slow even where the job does little work.** The default spends 2–3 s of Spark CPU on 1, 2 and 4 cores alike, yet runs 9–14 s longer on 1 core. Start-up grows by 2–3 s (the executor's JVM starts on a single CPU) and every step slows, the read most (9.6 → 15.3 s on I1): with one core the small jobs of each step run one task at a time, and the executor's JVM shares that core with its own garbage collection and compilation.
- **The baseline gains the most from cores, in match and clean.** On I1, match takes 69.5 / 34.8 / 30.1 s and clean 30.8 / 15.9 / 14.9 s. Their 200 tasks run one, two or four at a time, and the Python UDF's worker processes do the work. From 2 to 4 cores the gain is small because the tasks wait on Python workers and the driver (1 CPU) rather than on the executor's cores.
- **Spark CPU stays the same from 1 to 2 cores and grows on 4** (I1-L1 21.2 / 20.9 / 35.0 s). The same work is spread over more cores; on 4 they also compete with each other and with the driver and ClickHouse on the laptop, so each task takes more CPU time.
- **The default (L2) doesn't depend on the input**: within 8 s of each other on 1 core, 4 s on 2 cores and 2 s on 4, though I1 has 370× the check-ins of I3. It reads one statistics row per place.
- **Counting the statistics every run (L3) adds 18–21 s on 2 cores** (33–37 s on 1, 13–19 s on 4), almost all in the read step (I1: 15.3 → 38.5 s on 1 core). Even I3, with 1,470 check-ins, pays it: the extra Spark jobs and their 200-partition shuffles cost more than the rows.
- **The baseline (L1) is 2.8–3.0× the default on 1 core, 2.3–2.4× on 2, 2.0–2.4× on 4.**
- **Same result everywhere.** Every S1–S3 run, and every ClickHouse run of H1–H3, gives the same list as S2's L2 run of its input (check 4).
- **Against ClickHouse at the same CPUs** (S1/H1, S2/H2, S3/H3): ClickHouse gives the same lists in 1.5–2.2 s, against 36–44 / 27–31 / 28–30 s for Spark's default and 69–78 / 48–50 / 41–48 s with statistics every run. Spark's start-up alone (10–14 s) is 6–8× a whole ClickHouse run. Inside the steps, Spark's default needs 17–29 s against ClickHouse's 0.2–0.7 s.

## ClickHouse blocks (H1–H3)

The ClickHouse engine: a Python job in `br5-app` that sends 2 SQL queries (3 with a chosen visitor) to ClickHouse; no Spark. Live settings (plan 3.5):

| Live | Preset | Place statistics | ClickHouse threads |
|---|---|---|---|
| HL1 | `bench-clickhouse` | from the import (one row per place) | the default: one thread in a 1 GB container (BR5's finding) |
| HL2 | `bench-clickhouse-nomemlimit` | from the import | memory-per-thread limit off (`max_threads_min_free_memory_per_thread = 0`) |
| HL3 | `bench-clickhouse-stats-run` | counted from the city's check-ins in every query | the default |
| HL4 | `bench-clickhouse-stats-run-nomemlimit` | counted every run | limit off |

### Run time and query figures (median of 5)

Each figure is given as H1 / H2 / H3 (ClickHouse at 1 / 2 / 4 CPUs). "Inside the steps" leaves out starting Python and connecting (1.2–1.5 s in every run).

| Input | Cell | Run time (s) | Inside the steps (s) | Query time (ms) | ClickHouse CPU (ms) | Peak threads | Peak query memory (MB) |
|---|---|---|---|---|---|---|---|
| I1 Istanbul | HL1 defaults | 1.82 / 1.81 / 1.73 | 0.40 / 0.42 / 0.40 | 121 / 121 / 106 | 112 / 119 / 103 | 1 / 1 / 1 | 24 |
| | HL2 limit off | 2.04 / 1.77 / 1.91 | 0.51 / 0.41 / 0.43 | 125 / 106 / 123 | 120 / 103 / 124 | 1 / 3 / 4 | 24 |
| | HL3 statistics every run | 2.11 / 1.81 / 1.86 | 0.63 / 0.45 / 0.50 | 282 / 179 / 202 | 211 / 173 / 198 | 1 / 1 / 1 | 48 |
| | HL4 both | 2.17 / 1.80 / 1.90 | 0.63 / 0.48 / 0.50 | 287 / 202 / 199 | 233 / 294 / 395 | 1 / 3 / 5 | 48 / 45 / 44 |
| I2 Jakarta | HL1 | 2.08 / 1.74 / 1.80 | 0.65 / 0.45 / 0.48 | 200 / 115 / 134 | 140 / 113 / 132 | 1 / 1 / 1 | 35 |
| | HL2 | 1.78 / 1.64 / 1.65 | 0.49 / 0.43 / 0.46 | 137 / 115 / 130 | 118 / 111 / 128 | 1 / 1 / 1 | 35 |
| | HL3 | 2.07 / 1.79 / 1.75 | 0.66 / 0.52 / 0.53 | 219 / 183 / 187 | 219 / 180 / 185 | 1 / 1 / 1 | 48 |
| | HL4 | 1.91 / 1.73 / 1.94 | 0.64 / 0.52 / 0.61 | 288 / 182 / 215 | 199 / 283 / 464 | 1 / 3 / 5 | 48 / 51 / 47 |
| I3 Beijing | HL1 | 1.71 / 1.46 / 1.51 | 0.26 / 0.23 / 0.25 | 32 / 26 / 33 | 32 / 24 / 30 | 1 / 1 / 1 | 7 |
| | HL2 | 1.58 / 1.45 / 1.55 | 0.26 / 0.24 / 0.24 | 31 / 27 / 34 | 29 / 23 / 31 | 1 / 1 / 1 | 7 |
| | HL3 | 1.76 / 1.51 / 1.46 | 0.27 / 0.24 / 0.25 | 45 / 27 / 29 | 37 / 22 / 28 | 1 / 1 / 1 | 7 |
| | HL4 | 1.68 / 1.48 / 1.53 | 0.27 / 0.24 / 0.24 | 46 / 31 / 35 | 42 / 30 / 35 | 1 / 3 / 3 | 7 / 8 / 8 |

Peak threads include the query's own thread, so 5 means 4 working threads. A few runs show 2 where the median is 1 (a join's second thread).

Rows scanned are the same in every block. They count the index granules ClickHouse read, once per query: each query reads the city's places, and with statistics every run also the city's check-ins.

| Input | Statistics from the import (HL1, HL2) | Statistics every run (HL3, HL4) | Queries per run |
|---|---|---|---|
| I1 | 841,693 | 2,327,650 | 3 (with the visitor step) |
| I2 | 329,220 | 989,580 | 2 |
| I3 | 34,308 | 20,864 | 2 |

### Observations

- **No ClickHouse setting or size changes the run time beyond the noise.** Every cell takes 1.5–2.2 s, of which 1.2–1.5 s is starting the Python job and connecting. The queries take 0.03–0.29 s.
- **With the defaults, ClickHouse stays on one thread whatever its CPUs** (HL1 and HL3: 1 thread on 1, 2 and 4 CPUs), as in BR5. ClickHouse 26.8 gives a query fewer threads unless each has 1 GiB of free memory, and the container has 1 GiB in total.
- **With the limit off, ClickHouse uses the CPUs but gains nothing at this size.**
  - HL4 on I1 runs on 3 threads (2 CPUs) and 5 (4 CPUs), but its query time equals HL3's single thread: 202 against 179 ms on 2 CPUs, 199 against 202 ms on 4.
  - It costs more CPU: I1 294 / 395 ms against 173 / 198; I2 283 / 464 against 180 / 185, up to 2.5×.
  - The reason: each query scans at most about 1.1 million rows (Istanbul's check-ins and places) and runs for 50–150 ms. That is too little to split. In BR5, lifting the limit made the queries 30% faster on 4 CPUs, but each of those queries scanned the whole 1.19-million-row fact table at ×10.
  - HL2 (statistics from the import) gets 3–4 threads on I1 only, and 1 on I2 and I3: those queries are too small for ClickHouse to split at all.
- **Counting the statistics every run costs ClickHouse 0.05–0.16 s.** On I1 the queries scan 2.8× the rows (0.84 → 2.33 million) and use twice the memory (24 → 48 MB). The query time grows from 106–121 ms to 179–282 ms. On I3 (1,470 check-ins) the difference is a few milliseconds. Spark needed about 21 s more for the same option (30.5 → 51.7 s on route A, result-pipeline.md).
- **ClickHouse's work follows the input, the run time doesn't.** I1's queries scan 25× (HL1) to 110× (HL3) the rows of I3's and take 4–7× the query time, but its runs take 1.7–1.8 s against 1.5–1.7 s.
- **H1's queries were slower than H2's and H3's, even on one thread** (I1-HL3 282 against 179–202 ms; I2-HL1 200 against 115–134 ms). With one CPU, ClickHouse's own background work shares that CPU with the query. H1 was also run about 40 minutes before H2 and H3. The two causes can't be separated from these runs.
- **For the presentation:** at ×1, the alternative is limited by fixed costs, not by ClickHouse. A whole BR6 query fits on one thread in a tenth of a second, so neither more CPUs nor parallel threads help, and the 1 GiB-per-thread rule costs nothing here. It mattered in BR5, whose queries scanned the full fact table at ×10.

## Noise

- **S1:** the two runs of a cell differed by 1.5–11.6 s (under 15%), except I1-L2 (38.0 and 47.7 s), I2-L2 (36.7 and 53.4 s) and I2-L3 (71.7 and 83.6 s), which got a third run (43.9, 40.6 and 78.0 s). The slow runs came in the second round. On 1 core a run is more sensitive to anything else on the laptop.
- **S2:** the two runs of a cell differed by 0.4–4.3 s, except I1-L2 (27.5 and 33.2 s), which got a third run (29.7 s).
- **S3:** the two runs of a cell differed by 0.4–1.6 s, except I1-L1 (57.2 and 70.2 s) and I3-L3 (49.7 and 41.2 s), which got a third run (68.8 and 41.3 s). Differences under about 3 s between Spark cells are noise.
- **H1–H3:** the runs of a cell varied by 0.1–0.6 s, with single slow runs of up to 2.71 s (H3 I2-HL3) and 2.60 s (H1 I3-HL1) where the cell's others took 1.4–1.9 s. The slow time was outside the queries. Run-time differences under about 0.3 s between cells are noise; the query figures are the steadier evidence.

## Behaviour verified (plan section 8)

| # | Check | Result |
|---|---|---|
| 2 | Each run used its preset (Spark) | Every S1, S2 and S3 run's snapshot: `launch.settings_from` is the cell's preset and `cluster.worker_cores` is the block's (1, 2 or 4) |
| 3 | Inputs read what the plan says | Every S1–S3 and H1–H3 run: 544,471 / 412,100 / 1,470 check-ins read, 4,574 / 7,178 / 61 places near the route |
| 4 | Same input, same result | Every S1–S3 and H1–H3 run gives the same list as S2's L2 run of its input (`route_compare.py`): the same places, statuses, ranks, scores and cleaning counts, with Spark (every live setting, 1, 2 and 4 cores) and ClickHouse (every size, statistics mode and thread setting). 240 runs checked |
| 5 | All succeeded | S1: 22 of 22, S2: 20 of 20 and S3: 21 of 21 (with the warm-ups). H1–H3: 183 of 183 (and the 61 void runs of the first H2 attempt) |
| 6 | Screenshots | S1, S2 and S3: 9 each of `finder.jpg`, `resources.jpg` and `spark.jpg`, and 9, 6 and 9 `compare.jpg`; samples checked by eye: 4 cores in use, the steps as Spark jobs. H1–H3: 36 `finder.jpg` and 36 `compare.jpg` (H1's HL1 against S2's L2: Spark against ClickHouse). Samples checked by eye: the result, the run summary, the one setting that differs, the ClickHouse figures, and "Same result: yes" |
| 7 | Stack restored | Worker back to 2 cores (the default) after S1 (`restore`). ClickHouse back to 1 CPU (`auto(1)`) after H3; `br5-app` at 1 CPU throughout |
| 9 | ClickHouse start-up | After the restart, after the warm-up and at the end of each block: CPU limit 1 / 2 / 4 with `max_threads` `auto(1)` / `auto(2)` / `auto(4)`. Every run's snapshot has its block's `cluster.clickhouse_max_threads` |
| 10 | ClickHouse live settings | HL2 and HL4: every query carries `max_threads_min_free_memory_per_thread` = 0; HL1 and HL3: none. HL3 and HL4 scan the city's check-ins (2.3 million rows on I1 against 0.84 million) |
| 11 | Threads and cache | HL1 and HL3: 1 thread at the median on every size. HL2 and HL4 on 2 and 4 CPUs: 3 and 4–5 threads where a query is big enough (see above). No query used the query cache |

## Issues found and fixed

- **`docker-compose.yml` tied two other containers to the ClickHouse size.** BR5's ClickHouse blocks changed every `cpus: 1.0` in the file to `${BR5_CLICKHOUSE_CPUS:-1}`, so `kafka` and `br5-app` followed the variable too. At its default of 1 nothing changed, and BR5's blocks recreated only `clickhouse`, so BR5's figures are not affected. Both are back to `cpus: 1.0`, so the H2 and H3 blocks change ClickHouse only.
- **The first H2 attempt ran on 1 CPU, so it is void.** The script restarted ClickHouse with 2 CPUs and then launched each run through `scripts/br6.py run`. That command runs `docker compose up` for the stack before launching, without the size variable, so it put ClickHouse back to 1 CPU before the warm-up. Every run's snapshot recorded `auto(1)`, and the end-of-block check stopped the script before H3.
  - The 61 runs stay in the manifest as block `H2-void`.
  - The script now passes the block's sizes (`BR5_WORKER_CORES`, `BR5_CLICKHOUSE_CPUS`) to every run's environment. It also checks the size after the warm-up, so a wrong size stops a block after one run.
  - H1 is valid, because its size is the default. The Spark blocks would have had the same problem with the worker's cores.
- **The H2 and H3 compare screenshots were 10,200 px tall**, mostly empty below the content: Run History sizes the page to its growing list of runs. The script now trims each `compare.jpg` to its content (about 2,800 px), and `benchmark.py trim` trimmed the ones already saved.

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `S3-I1-L2/` or `H1-I1-HL1/`), from the cell's first measured run, stored with Git LFS:

| File | Shows |
|---|---|
| `finder.jpg` | The Route Finder result of the run (`?run=<id>`), with the run summary: check-ins, steps, settings used |
| `compare.jpg` | Run History against the reference run: run time, steps, settings that differ, ClickHouse figures, same result |
| `resources.jpg` | Spark cells: Grafana BR6 Resource use around the run (CPU and memory of `spark-worker`, `br5-app` and `clickhouse`) |
| `spark.jpg` | Spark cells: the application's Jobs page in Spark history, jobs named after the steps |

References: L1 and L3 against L2 of the same input and block; L2 of S1 and S3 against S2's L2. HL2 and HL3 against HL1 of the same input and block; HL4 against HL3; HL1 of H2 and H3 against H1's HL1. H1's HL1 against S2's L2 (Spark against ClickHouse).

Known limit: with `?run=<id>`, the Route Finder sets the city to the run's, but the choice boxes above the result keep their defaults (for example "General visitor" for I1). The result, headed with its own route and visitor, is the run's.

## Appendix: every run (seconds, in order)

| Cell | H1 (1 CPU) | H2 (2 CPUs) | H3 (4 CPUs) |
|---|---|---|---|
| I1-HL1 | 1.78, 1.77, 1.82, 1.95, 2.00 | 2.03, 1.81, 1.78, 1.88, 1.65 | 1.83, 1.73, 1.61, 1.72, 1.91 |
| I1-HL2 | 2.04, 2.00, 1.96, 2.19, 2.12 | 1.71, 1.85, 1.82, 1.62, 1.77 | 1.92, 1.65, 1.93, 1.88, 1.91 |
| I1-HL3 | 2.16, 2.23, 2.11, 2.01, 1.94 | 2.04, 1.73, 1.75, 1.86, 1.81 | 1.81, 1.86, 2.22, 2.27, 1.81 |
| I1-HL4 | 2.17, 2.24, 2.06, 2.27, 2.17 | 1.71, 1.78, 1.88, 1.80, 1.89 | 1.93, 1.67, 1.79, 1.90, 1.93 |
| I2-HL1 | 2.48, 2.08, 2.38, 1.87, 1.77 | 1.76, 1.67, 1.57, 1.81, 1.74 | 1.64, 1.81, 1.88, 1.80, 1.55 |
| I2-HL2 | 1.67, 1.70, 1.78, 1.96, 1.86 | 1.63, 1.68, 1.71, 1.52, 1.64 | 1.65, 1.77, 1.62, 1.64, 1.81 |
| I2-HL3 | 2.30, 1.98, 2.07, 1.95, 2.28 | 1.90, 1.79, 1.71, 1.80, 1.76 | 1.64, 1.64, 1.75, 1.81, 2.71 |
| I2-HL4 | 2.01, 1.91, 2.44, 1.88, 1.88 | 1.91, 1.62, 1.79, 1.73, 1.55 | 1.94, 1.73, 2.27, 1.71, 2.08 |
| I3-HL1 | 1.71, 1.63, 2.60, 1.69, 1.72 | 1.49, 1.51, 1.33, 1.46, 1.40 | 1.44, 1.42, 1.51, 1.58, 1.54 |
| I3-HL2 | 1.64, 1.73, 1.58, 1.56, 1.52 | 1.54, 1.43, 1.53, 1.45, 1.33 | 1.49, 1.49, 1.63, 1.55, 1.63 |
| I3-HL3 | 1.49, 1.76, 1.86, 1.87, 1.71 | 1.43, 1.51, 1.57, 1.36, 1.54 | 1.46, 1.44, 1.45, 1.50, 1.57 |
| I3-HL4 | 2.13, 1.67, 1.76, 1.68, 1.64 | 1.59, 1.40, 1.53, 1.48, 1.36 | 1.46, 1.53, 1.56, 1.68, 1.47 |

Warm-ups (not counted), I1-HL1: 2.09 / 1.92 / 1.75 s.

| Cell | S1 (1 core) | S2 (2 cores) | S3 (4 cores) |
|---|---|---|---|
| I1-L1 | 128.99, 132.99 | 69.26, 73.54 | 57.23, 70.17, 68.75 |
| I1-L2 | 38.01, 47.71, 43.88 | 27.45, 33.23, 29.65 | 28.24, 28.87 |
| I1-L3 | 76.54, 79.25 | 47.75, 47.92 | 48.85, 46.14 |
| I2-L1 | 116.20, 127.75 | 70.02, 69.58 | 61.33, 61.38 |
| I2-L2 | 36.74, 53.35, 40.62 | 32.12, 29.79 | 29.49, 31.04 |
| I2-L3 | 71.72, 83.57, 78.03 | 49.31, 50.40 | 45.43, 47.07 |
| I3-L1 | 103.10, 101.60 | 65.92, 62.68 | 58.72, 52.03 |
| I3-L2 | 35.04, 37.30 | 26.45, 27.45 | 28.24, 27.86 |
| I3-L3 | 66.46, 71.02 | 46.76, 50.10 | 49.66, 41.19, 41.30 |

Warm-ups (not counted), I1-L1: S1 126.66 s, S2 74.72 s, S3 52.99 s.
