# BR5 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br5/plan-benchmark.md). **This file is updated after each block** of the benchmark.

## Progress

| Block | Spark worker | Status | Runs |
|---|---|---|---|
| S3 | 4 cores | **Done** (2026-10-19 and 2026-10-20) | 3 warm-ups + 23 measured, all succeeded |
| S2 + C1 | 2 cores; ClickHouse engine | **Done** (2026-10-20) | 1 warm-up + 19 Spark + 9 ClickHouse measured, all succeeded |
| S1 | 1 core | **Done** (2026-10-21) | 1 warm-up + 22 measured, all succeeded |

All blocks are done: 78 runs, of which 5 warm-ups.

## Setup

- **Stack:** as in [result-monitoring.md](result-monitoring.md):
  - Spark: 1 worker with 2 GB; its cores are the block's (`BR5_WORKER_CORES`).
  - ClickHouse: 1 CPU, 1 GB.
  - The driver runs in `br5-app` (1 CPU, 2 GB).
  - The Docker host has 14 CPUs and 16 GB.
- **Data:** imported once at ×10 for the whole benchmark: 1,193,900 bookings in 99.5 s, all import checks equal.
- **Configurations:** see the two tables below; the full definitions are in plan sections 3.2–3.4.
- **Measured runs:**
  - Each Spark cell has 2 runs, plus a third when the two differ by more than 15%. ClickHouse cells have 3 runs.
  - The runs are interleaved across the cells.
  - One warm-up after each worker restart is not counted.
  - Every run is listed in [benchmark/runs.csv](benchmark/runs.csv); the run times of every run are in the appendix.
- **How figures are given:**
  - Figures are the **median of a cell's measured runs**, each figure separately.
  - Spark and ClickHouse figures are exact and stored with each run. Container CPU and memory come from Prometheus (5 s samples).

### Start-up configurations (S1–S3)

Only the Spark worker's cores change, through `BR5_WORKER_CORES` in `docker-compose.yml`. It sets the cores the worker offers Spark (`SPARK_WORKER_CORES`) and the container's CPU limit (`cpus`) to the same number.

| ID | `SPARK_WORKER_CORES` = `spark-worker` `cpus` | What a run gets |
|---|---|---|
| S1 | 1 | 1 executor with 1 core |
| S2 | 2 (the project's default) | 1 executor with 2 cores |
| S3 | 4 | 1 executor with 4 cores |

The same in every block:
- **`spark-worker`:** 2G offered to Spark (`SPARK_WORKER_MEMORY`), container limit 2500m. A run's executor uses 512m plus 384m overhead.
- **`br5-app`** (the app and the Spark driver): 1 CPU, 2g; driver memory 512m.
- **`clickhouse`:** 1 CPU, 1g.

### Live settings (L1–L3, C1)

Each column is a preset. Only the settings that differ somewhere are listed; **bold** differs from the default.

| Setting | Default | L1 `bench-baseline` | L2 `bench-default` | L3 `bench-read4` | C1 `bench-clickhouse` |
|---|---|---|---|---|---|
| `spark.cores.max`: most cores a run may take | 2 | **4** | **4** | **4** | 2 (no Spark) |
| `pipeline.engine` | spark | spark | spark | spark | **clickhouse** (4 SQL queries, no Spark) |
| `pipeline.reader`: how Spark gets the bookings from ClickHouse | parallel (one slice per task) | **driver** (one query on the driver, then shipped to the executor) | parallel | parallel | – |
| `pipeline.read_partitions`: slices of the parallel reader | 2 | 2 (not used) | 2 | **4** | – |
| `pipeline.join`: when the lookup tables are joined | late (names attached to the result rows only) | **early** (every booking joined with 4 lookup tables first) | late | late | – |
| `pipeline.cache`: keep the cleaned bookings for clean, aggregate and profile | true | **false** (each step recomputes them) | true | true | – |
| `spark.sql.adaptive.enabled` (AQE) | true | **false** (every shuffle keeps 200 partitions) | true | true | – |
| `spark.sql.autoBroadcastJoinThreshold` | 10m | **-1** (no broadcast: sort-merge joins) | 10m | 10m | – |

- L1 is the project's built-in `baseline` preset plus `spark.cores.max=4`. That preset also sets `pipeline.writer=pandas`, which only affects imports, not these runs.
- `spark.cores.max=4` lets a run use all of the worker's cores in every block (the default of 2 would cap S3 at 2).
- Everything else is at its default in all four presets: executor memory 512m, `spark.sql.shuffle.partitions` 200, Java serializer, cache level MEMORY_AND_DISK, ClickHouse `max_threads` and `max_memory_usage` at the server's defaults, the Spark event log on, and all analysis rules (plan section 3.3 lists them).
- **Checked from the runs:** every run stores the settings it used. All runs of a preset recorded exactly the values above, whatever the block. Spark's own reported values match: AQE `false` and broadcast `-1` for L1, `true` and `10m` for L2 and L3; `spark.cores.max` 4, executor memory 512m and shuffle partitions 200 for all three.

## The inputs and their answers

The answer depends on the input only, never on the settings or the engine (see "Behaviour verified").

| Input | Bookings read | Used | Low season | Cards | Top card |
|---|---|---|---|---|---|
| I1 All / All / All | 1,193,900 | 743,890 | Dec – Jan: demand is 52% of peak | 10 (573 targets, 68 qualify) | Room A · United Kingdom: potential 1,036 room-nights per month; early-bird (≥ 50 days) + strong discount |
| I2 City Hotel / Portugal / All | 309,600 | 105,740 | Apr (54%), Aug (58%), Nov – Feb (52%) | 5 (7 targets, 5 qualify) | Room A: potential 595; last-minute deal + moderate discount |
| I3 Resort Hotel / United Kingdom / A | 35,360 | 31,450 | Jul – Aug (36%), Nov – Apr (37%) | 1 | potential 853; early-bird (≥ 70 days) + strong discount |

## Run time (seconds, median)

| Input | Live setting | S1 (1 core) | S2 (2 cores) | S3 (4 cores) |
|---|---|---|---|---|
| I1 (100% of the data) | L1 baseline | 129.1 | 99.5 | 78.7 |
| | L2 default | 38.3 | 32.0 | 29.5 |
| | L3 4 read partitions | 36.4 | 32.9 | 27.2 |
| | C1 ClickHouse engine | | 2.3 | |
| I2 (26%) | L1 baseline | 101.0 | 76.1 | 69.6 |
| | L2 default | 38.0 | 29.2 | 27.0 |
| | L3 4 read partitions | 37.6 | 29.7 | 25.0 |
| | C1 ClickHouse engine | | 1.9 | |
| I3 (3%) | L1 baseline | 94.1 | 70.3 | 61.5 |
| | L2 default | 29.7 | 27.7 | 26.7 |
| | L3 4 read partitions | 30.4 | 28.4 | 23.6 |
| | C1 ClickHouse engine | | 1.9 | |

The C1 runs were made in the S2 block; the ClickHouse engine doesn't use the Spark worker, so its worker size doesn't apply.

### Effect of each dimension

| Comparison | I1 | I2 | I3 |
|---|---|---|---|
| **Live:** baseline vs default, S1 | 3.37× | 2.66× | 3.17× |
| **Live:** baseline vs default, S2 | 3.11× | 2.61× | 2.54× |
| **Live:** baseline vs default, S3 | 2.67× | 2.58× | 2.30× |
| **Live:** 4 read partitions vs 2, S1 | −1.9 s | −0.4 s | +0.7 s |
| **Live:** 4 read partitions vs 2, S2 | +0.9 s | +0.5 s | +0.7 s |
| **Live:** 4 read partitions vs 2, S3 | −2.3 s | −2.0 s | −3.1 s |
| **Start-up:** 1 core vs 2, baseline | +29.6 s (+30%) | +24.9 s (+33%) | +23.8 s (+34%) |
| **Start-up:** 1 core vs 2, default | +6.3 s (+20%) | +8.8 s (+30%)* | +2.0 s (+7%) |
| **Start-up:** 4 cores vs 2, baseline | −20.8 s (−21%) | −6.5 s (−9%) | −8.8 s (−13%) |
| **Start-up:** 4 cores vs 2, default | −2.5 s (−8%) | −2.2 s (−8%) | −1.0 s (−4%) |
| **Start-up:** 1 core to 4, baseline | −50.4 s (−39%) | −31.4 s (−31%) | −32.6 s (−35%) |
| **Start-up:** 1 core to 4, default | −8.8 s (−23%) | −11.0 s (−29%)* | −3.0 s (−10%) |
| **Input:** I1 vs I3 (34× the data), default | S1 +8.6 s, S2 +4.3 s, S3 +2.8 s | | |

\* Two of S1-I2-L2's three runs came from the slow second set (see Noise). Its first run, 30.5 s, gives +1.3 s (+4%) against S2's median.

## Spark vs the ClickHouse engine

"Inside the steps" leaves out the time before the first step: creating the Spark session (Spark), starting Python and connecting (ClickHouse).

| Input | Spark default, S1 (1 core, as ClickHouse has): total / inside the steps | Spark default, S2: total / inside the steps | Best Spark cell (S3 L3): total / inside the steps | ClickHouse engine: total / inside the steps | Spark default vs ClickHouse (total): S1 / S2 |
|---|---|---|---|---|---|
| I1 | 38.3 / 27.8 | 32.0 / 22.3 | 27.2 / 18.5 | 2.3 / 0.7 | 16.7× / 13.9× |
| I2 | 38.0 / 26.8 | 29.2 / 19.1 | 25.0 / 16.3 | 1.9 / 0.4 | 20.0× / 15.4× |
| I3 | 29.7 / 19.8 | 27.7 / 17.4 | 23.6 / 15.1 | 1.9 / 0.3 | 15.6× / 14.6× |

ClickHouse engine work per run:
- 4 queries, using 0.4 / 0.2 / 0.1 s of ClickHouse CPU (I1 / I2 / I3).
- ClickHouse scanned 4,775,774 / 1,377,732 / 323,582 rows: the fact table once per query.
- The `clickhouse` container used 3.0–3.5 CPU-seconds in the run's window, which includes its background work; `br5-app` (the Python job) used 3.2–3.3.

## Where the time goes (seconds, median)

"Start-up" is the run time outside the steps, mostly creating the Spark session. The rank, recommend and write steps take under 0.3 s and are left out.

Each cell is given as S1 / S2 / S3.

| Cell | Run | Start-up | Read | Clean | Aggregate | Profile |
|---|---|---|---|---|---|---|
| I1-L1 | 129.1 / 99.5 / 78.7 | 10.3 / 10.1 / 9.9 | 3.6 / 3.1 / 3.3 | 38.1 / 27.6 / 22.5 | 24.5 / 19.1 / 12.4 | 51.2 / 32.2 / 26.4 |
| I1-L2 | 38.3 / 32.0 / 29.5 | 10.5 / 9.7 / 9.6 | 14.2 / 11.1 / 10.2 | 2.0 / 1.9 / 1.4 | 2.8 / 2.4 / 2.0 | 8.7 / 6.8 / 6.2 |
| I1-L3 | 36.4 / 32.9 / 27.2 | 9.7 / 9.9 / 8.7 | 13.5 / 11.0 / 9.1 | 1.9 / 1.8 / 1.4 | 2.5 / 3.0 / 1.9 | 8.6 / 7.1 / 5.8 |
| I2-L1 | 101.0 / 76.1 / 69.6 | 9.6 / 9.9 / 10.9 | 4.7 / 4.5 / 4.7 | 31.7 / 22.4 / 20.6 | 18.5 / 13.4 / 11.6 | 36.4 / 25.6 / 21.8 |
| I2-L2 | 38.0 / 29.2 / 27.0 | 11.2 / 10.1 / 9.6 | 13.8 / 9.6 / 8.6 | 1.4 / 1.7 / 1.6 | 2.6 / 2.0 / 1.7 | 7.6 / 5.6 / 5.4 |
| I2-L3 | 37.6 / 29.7 / 25.0 | 11.1 / 9.8 / 8.7 | 12.6 / 10.5 / 8.3 | 1.5 / 1.4 / 1.4 | 3.4 / 2.1 / 1.5 | 8.2 / 5.7 / 5.0 |
| I3-L1 | 94.1 / 70.3 / 61.5 | 10.0 / 9.6 / 10.2 | 4.0 / 3.9 / 3.6 | 30.6 / 22.5 / 19.0 | 16.9 / 11.6 / 10.1 | 32.6 / 22.5 / 19.0 |
| I3-L2 | 29.7 / 27.7 / 26.7 | 9.9 / 10.3 / 10.1 | 10.2 / 9.1 / 8.3 | 1.2 / 1.4 / 1.3 | 2.2 / 1.8 / 1.6 | 6.0 / 5.0 / 5.3 |
| I3-L3 | 30.4 / 28.4 / 23.6 | 9.7 / 9.9 / 8.5 | 11.1 / 9.8 / 7.8 | 1.2 / 1.2 / 1.1 | 1.9 / 1.8 / 1.5 | 6.3 / 5.6 / 4.5 |

Spark start-up (about 10 s) doesn't depend on the worker's cores. With the defaults the cores mostly shorten the read step; with the baseline they shorten clean, aggregate and profile, its 200-task shuffles.

## Work and resources (median)

The ClickHouse column counts the rows it **scanned** (index granules matching the filter, once per read query), not the rows returned. Container CPU covers the run's time window.

### S1 (1 core)

| Cell | Spark CPU (s) | Spark tasks | Shuffle (MB) | Spilled (MB) | Executor peak heap (MB) | ClickHouse rows scanned | `spark-worker` CPU (s) / peak memory (MB) | `br5-app` CPU (s) |
|---|---|---|---|---|---|---|---|---|
| I1-L1 | 36.6 | 4,825 | 34.9 | 114 | 352 | 1,193,900 | 114.2 / 1,057 | 61.4 |
| I1-L2 | 5.7 | 25 | 2.5 | 0 | 105 | 2,387,800 | 28.5 / 785 | 25.7 |
| I1-L3 | 5.4 | 37 | 2.7 | 0 | 135 | 4,775,600 | 26.0 / 787 | 24.0 |
| I2-L1 | 18.9 | 4,245 | 5.1 | 0 | 180 | 356,679 | 86.4 / 763 | 55.7 |
| I2-L2 | 3.5 | 21 | 0.0 | 0 | 88 | 688,826 | 23.8 / 686 | 28.9 |
| I2-L3 | 3.6 | 33 | 0.0 | 0 | 103 | 1,377,652 | 25.7 / 678 | 28.5 |
| I3-L1 | 16.5 | 4,245 | 1.4 | 0 | 125 | 113,408 | 81.2 / 702 | 53.6 |
| I3-L2 | 2.4 | 21 | 0.0 | 0 | 88 | 226,815 | 20.5 / 649 | 23.4 |
| I3-L3 | 2.6 | 33 | 0.0 | 0 | 90 | 453,630 | 22.3 / 652 | 23.8 |

### S2 (2 cores)

| Cell | Spark CPU (s) | Spark tasks | Shuffle (MB) | Spilled (MB) | Executor peak heap (MB) | ClickHouse rows scanned | `spark-worker` CPU (s) / peak memory (MB) | `br5-app` CPU (s) |
|---|---|---|---|---|---|---|---|---|
| I1-L1 | 51.6 | 4,825 | 35.0 | 95 | 359 | 1,193,900 | 164.0 / 1,164 | 65.7 |
| I1-L2 | 8.5 | 25 | 2.5 | 0 | 207 | 2,387,800 | 33.7 / 986 | 24.5 |
| I1-L3 | 9.2 | 37 | 2.7 | 0 | 185 | 4,775,600 | 38.2 / 983 | 25.8 |
| I2-L1 | 25.5 | 4,245 | 5.1 | 0 | 268 | 356,679 | 110.4 / 1,017 | 63.0 |
| I2-L2 | 5.1 | 21 | 0.0 | 0 | 85 | 713,358 | 29.4 / 837 | 24.2 |
| I2-L3 | 5.1 | 33 | 0.0 | 0 | 73 | 1,426,716 | 31.3 / 819 | 25.1 |
| I3-L1 | 21.5 | 4,245 | 1.4 | 0 | 189 | 113,408 | 105.6 / 912 | 60.1 |
| I3-L2 | 3.4 | 21 | 0.0 | 0 | 79 | 226,815 | 25.9 / 776 | 24.1 |
| I3-L3 | 3.6 | 33 | 0.0 | 0 | 91 | 453,630 | 29.1 / 777 | 24.5 |

### S3 (4 cores)

| Cell | Spark CPU (s) | Spark tasks | Shuffle (MB) | Spilled (MB) | Executor peak heap (MB) | ClickHouse rows scanned | `spark-worker` CPU (s) / peak memory (MB) | `br5-app` CPU (s) |
|---|---|---|---|---|---|---|---|---|
| I1-L1 | 69.6 | 4,861 | 35.3 | 229 | 300 | 1,193,900 | 215.0 / 1,390 | 68.1 |
| I1-L2 | 8.3 | 29 | 2.5 | 0 | 160 | 2,387,800 | 44.9 / 1,184 | 24.8 |
| I1-L3 | 10.9 | 41 | 2.7 | 0 | 141 | 4,775,600 | 42.8 / 1,338 | 22.2 |
| I2-L1 | 31.4 | 4,289 | 5.2 | 48 | 166 | 344,413 | 142.7 / 1,274 | 67.6 |
| I2-L2 | 5.4 | 25 | 0.0 | 0 | 94 | 688,826 | 34.5 / 988 | 23.5 |
| I2-L3 | 6.3 | 37 | 0.0 | 0 | 68 | 1,426,716 | 37.3 / 1,136 | 21.7 |
| I3-L1 | 25.6 | 4,289 | 1.5 | 0 | 169 | 97,275 | 138.7 / 1,136 | 60.9 |
| I3-L2 | 3.9 | 25 | 0.0 | 0 | 71 | 194,550 | 33.5 / 923 | 24.0 |
| I3-L3 | 4.4 | 37 | 0.0 | 0 | 68 | 460,500 | 31.6 / 1,040 | 21.4 |

## Observations

- **The live settings matter far more than the input or the cluster size.**
  - The naive baseline is 2.3–3.4× slower than the defaults on every input and every worker size.
  - Its time is in clean, aggregate and profile, 10–51 s each against 1–9 s with the defaults:
    - Without the cache, each of these steps works on the data again.
    - With AQE off, every shuffle keeps Spark's 200 partitions: 4,245–4,861 tasks against 21–41.
    - With broadcast off and the early join, every lookup join is a sort-merge join over all bookings.
  - On I1 the baseline spilled 114 MB (1 core), 95 MB (2 cores) and 229 MB (4 cores) to disk; the defaults spilled nothing.
- **The start-up configuration helps the baseline more than the defaults.**
  - From 1 to 4 cores the baseline gained 31–39% (up to 50 s): its thousands of small tasks can use the extra cores. On 1 core the worker was busy almost all the time (I1: 114 CPU-seconds in 129 s); on 4 cores it used 2.7 cores on average (215 CPU-seconds in 79 s).
  - The defaults gained 10–29% (3.0–11.0 s) from 1 to 4 cores, and only 4–8% from 2 to 4. They run 21–41 tasks, and most of their time is fixed cost.
  - So more hardware narrows the gap between a bad and a good configuration, but doesn't close it. The baseline is 2.7–3.4× slower than the defaults on 1 core, 2.5–3.1× on 2 and 2.3–2.7× on 4. The defaults on **1 core** (29.7–38.3 s) are still 1.8–2.1× as fast as the baseline on **4 cores** (61.5–78.7 s).
  - More cores cost more CPU for the same work. The baseline's I1 tasks used 36.6 / 51.6 / 69.6 Spark CPU-seconds on 1 / 2 / 4 cores, with GC time 1.4 / 4.2 / 6.0 s. Tasks running side by side in one executor probably compete for its heap and memory bandwidth.
- **The best live setting depends on the start-up configuration (plan D5).**
  - 4 read partitions instead of 2 give no consistent gain on 1 core (−1.9 to +0.7 s, within the noise): the 4 slices are read one after another. On 2 cores they are slightly *slower* (+0.5 to +0.9 s; their read step is no shorter). On 4 cores they are faster (−2.0 to −3.1 s).
  - Part of the 4-core gain may be the machine: the S3 L3 runs were made a day before S3 L2, and their Spark start-up, which the read setting can't change, was also 0.9–1.6 s shorter. Inside the steps the 4-core gain is 1.1–1.6 s.
  - The extra slices aren't free on any size: ClickHouse scans twice as many rows, since each slice scans the whole filtered range.
- **With the defaults, the data size matters little at this scale, and less with more cores.**
  - 34× more data (I3 to I1) adds 8.6 s on 1 core, 4.3 s on 2 and 2.8 s on 4.
  - Fixed costs dominate: about 10 s of Spark start-up, and a read step of 8–11 s even for 35,360 rows. That step includes creating and filling the cache and the Arrow conversion.
  - The baseline's cost hardly depends on the data size either: its task count is fixed by the 200 partitions per shuffle (I3 at 3% of the data: 94.1 s against 129.1 s on 1 core, 70.3 s against 99.5 s on 2).
- **The ClickHouse engine is 16–20× faster than Spark's defaults on the same CPU (1 core), and 14–15× faster than on 2.**
  - It answers in 1.9–2.3 s against 29.7–38.3 s (1 core) and 27.7–32.0 s (2 cores). Even without Spark's session start-up, Spark's steps take 17–28 s against 0.3–0.7 s.
  - This is the "push the query to the column store" alternative: for these aggregations over data already in ClickHouse, sending the query to the data wins at this size.
  - Both engines give exactly the same cards and monthly figures.
- **The baseline's driver reader loads the driver:** `br5-app` used 54–68 CPU-seconds against 21–29 with the parallel reader. Its driver memory (512 MB) was enough at ×10 (plan risk R2).

## Noise

- **S3:** the second set of runs on 2026-10-20 was slower across the board, so 5 of the 6 L1/L2 cells needed a third run.
  - In the baselines the work itself got slower: I1's Spark CPU time for the same tasks rose from 49 s (run 1) to 70–72 s (runs 2 and 3). That points to the laptop's CPU: heat or load outside Docker. Nothing else in the stack was busy: no ClickHouse merge, no other busy container, no open app page.
  - One default run (I3-L2 run 2, 42.2 s) lost its time in Spark start-up instead: 21.9 s outside the steps, against 9–10 s.
  - The L3 cells, run the day before as two separate sessions, were within 4%.
- **S2:** only I1-L1 needed a third run (76.5, 100.8, 99.5 s). All other cells' runs were within 8%. The ClickHouse engine's runs varied by 0.2–0.5 s.
- **S1:** as in S3, the second set of runs was slower, and 4 cells needed a third run: I1-L1, I1-L2, I2-L2 and I2-L3.
  - I1-L1's Spark CPU time for the same tasks rose from 31.7 s (run 1) to 43.2 s (run 2), again pointing to the host's CPU.
  - In the default cells the read step grew: I2-L2 took 10.6 s in run 1 and 13.8–14.5 s in runs 2 and 3. Two of I2-L2's and I2-L3's three runs are slow ones, so their medians lean high. That is why S1 against S2 for I2 with the defaults (+30%) is larger than for I1 and I3.
  - The I3 cells were within 4% in both sets.

The medians are robust to one slow run, but differences under about 10% between cells measured at different times should not be over-read.

## Behaviour verified (plan section 8)

| # | Check | Result |
|---|---|---|
| 1 | Start-up configuration | Every run recorded the block's `cluster.worker_cores`: 1 for all 23 S1 runs, 2 for all 29 S2 runs, 4 for all 26 S3 runs. During S3, Prometheus showed the `spark-worker` CPU limit at 4. In S1, Grafana shows the worker at 1 core, with its CPU close to the 1.0 limit through the baseline runs |
| 2 | Live settings | Each run's "settings from" is its cell's preset. For L1, Spark itself reports AQE off and broadcast -1. Every L2 and L3 run sent 2 or 4 read queries whose rows add up exactly to the rows read. L1 (driver reader) and C1 send one query or their own SQL instead |
| 3 | Rows read per input | 1,193,900 / 309,600 / 35,360 in every run, as planned |
| 4 | Same input, same result | All 78 runs agree per input, Spark (all three worker sizes, all live settings) and the ClickHouse engine: 10 cards and 132 month rows; 5 and 72; 1 and 24. Each S1 run was compared with S2's L2 reference run. This needed a fix to the check (below) |
| 5 | All succeeded; repeats close | All succeeded, and every run has its Spark figures. Third runs: 5 cells in S3, 1 in S2, 4 in S1 (see Noise) |
| 6 | Screenshots | 36 files for S1, 39 for S2 and 36 for S3. A sample from each live setting and the engine comparison was checked by eye |
| 7 | Stack restored | Worker back to 2 cores after S3 and after S1; `make br5-check-dashboards` passes (37 queries, 0 failed, 0 empty) |

## Issues found and fixed

- **The "same result" check rounded before comparing.**
  - City Hotel / Portugal / All with 4 read partitions seemed to differ from 2 partitions in two average prices (102.08 vs 102.09).
  - The stored values are 102.08499999999998 and 102.0850000000001: the same sum, added in another order, rounds to different 2-decimal values.
  - `compare_runs.differences` now compares numbers within a relative 1e-9, and has a unit test. The ClickHouse engine against Spark, and ×1 against ×10, still match under it.
- **One run's Spark figures were not collected** (S2-I1-L3 run 2).
  - The driver's page didn't answer within 3 s: it shares `br5-app`'s one CPU with the app. The run itself succeeded.
  - Its figures were recovered from the Spark history server, which serves the same API from the event log. The executor's peak heap was left out, because the history server doesn't keep it.
  - The collection now waits up to 10 s and tries twice. It happens after the run is marked finished, so run times are unaffected.
- **Screenshots of the first trial:**
  - The Promotion Finder showed All / All / All above a run's result.
  - The Run History tables cut off rows.
  - The Grafana window included part of the next run.

  All three were fixed before the blocks ran (plan sections 4 and 6).
- **The script's screenshot height limit** was raised from 8,000 to 12,000 px: I1's result page is 7,841 px tall with its 10 cards.
- **The plan's check for 4 read slices was wrong:** ClickHouse counts scanned rows, not returned rows. It now counts the read queries and their rows.

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `S2-I1-L1/`), from the cell's first measured run:

| File | Shows |
|---|---|
| `finder.jpg` | The result as the user sees it, with the run summary: rows, steps, settings used |
| `compare.jpg` | Run History against the reference run: run time, steps, settings that differ, resources, same result |
| `resources.jpg` | Grafana Resource use during the run: CPU and memory per container (Spark cells only) |
| `spark.jpg` | The run's Spark jobs page, jobs named after the steps (Spark cells only) |

References in `compare.jpg`:
- L1 and L3 are compared with L2 of the same input and block.
- L2 of S1 and S3 is compared with S2's L2: the only setting that differs is the worker's cores.
- C1 is compared with S2's L2: Spark against the ClickHouse engine.
- S2's L2 cells are the references, so they have no `compare.jpg`.

## Appendix: every run (seconds, in order)

| Cell | S1 | S2 | S3 |
|---|---|---|---|
| I1-L1 | 108.9, 143.7, 129.1 | 76.5, 100.8, 99.5 | 60.3, 78.7, 81.2 |
| I1-L2 | 33.4, 46.1, 38.3 | 30.7, 33.3 | 27.7, 31.3 |
| I1-L3 | 37.0, 35.8 | 32.1, 33.7 | 27.1, 27.2 |
| I2-L1 | 99.7, 102.2 | 71.9, 80.2 | 59.2, 69.6, 73.6 |
| I2-L2 | 30.5, 38.0, 39.2 | 28.9, 29.4 | 24.8, 29.9, 27.0 |
| I2-L3 | 31.5, 37.6, 40.0 | 30.0, 29.3 | 24.5, 25.4 |
| I3-L1 | 92.6, 95.6 | 67.7, 72.9 | 56.3, 69.1, 61.5 |
| I3-L2 | 30.1, 29.4 | 27.9, 27.5 | 25.7, 42.2, 26.7 |
| I3-L3 | 30.4, 30.3 | 29.4, 27.4 | 23.4, 23.7 |
| I1-C1 | | 2.5, 2.3, 2.1 | |
| I2-C1 | | 2.2, 1.9, 1.7 | |
| I3-C1 | | 1.9, 1.7, 1.9 | |

Warm-ups (not counted): S1 112.7 (I1-L1); S2 80.8 (I1-L1); S3 24.1 (I3-L3), 27.7 (I1-L3), 59.9 (I1-L1).
