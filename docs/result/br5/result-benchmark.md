# BR5 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br5/plan-benchmark.md). **This file is updated after each block** of the benchmark.

## Progress

| Block | Spark worker | Status | Runs |
|---|---|---|---|
| S3 | 4 cores | **Done** (2026-10-19 and 2026-10-20) | 3 warm-ups + 23 measured, all succeeded |
| S2 + C1 | 2 cores; ClickHouse engine | **Done** (2026-10-20) | 1 warm-up + 19 Spark + 9 ClickHouse measured, all succeeded |
| S1 | 1 core | **Done** (2026-10-21) | 1 warm-up + 22 measured, all succeeded |
| X | 4 cores; shuffle partitions and salting from the baseline | **Done** (2026-10-24) | 1 warm-up + 27 measured, all succeeded |
| H1–H3 | ClickHouse engine on 1 / 2 / 4 ClickHouse CPUs | **Done** (2026-11-02) | 6 warm-ups + 135 measured, all succeeded |

All blocks are done: 247 runs, of which 12 warm-ups.

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

## Block X: shuffle partitions and data skew

Added for the presentation (Memory management & Data skew handling); plan section 3.6. All cells run on 4 cores and start from the baseline L1, the only configuration where both effects exist: AQE is off, so the shuffles keep exactly the partitions they are given, and the 4 lookup joins are sort-merge joins on skewed keys. Each variant changes one setting:

| Cell | Preset | Change from L1 |
|---|---|---|
| L1 | `bench-baseline` | – (measured again in this block) |
| P1 | `bench-baseline-shuffle8` | `spark.sql.shuffle.partitions` 200 → **8** |
| K1 | `bench-baseline-salt8` | `pipeline.salt_buckets` 1 → **8** (salting) |

Salting is done in the Spark job at join time: each booking gets a random bucket 0–7, each lookup row is copied once per bucket, and the joins match on (key, bucket). The imported data is unchanged.

### Run time (seconds, median of 3)

| Input | L1 baseline | P1 8 partitions | K1 salting | P1 vs L1 | K1 vs L1 |
|---|---|---|---|---|---|
| I1 (100%) | 74.8 | 50.0 | 87.2 | **−24.8 s (−33%)** | +12.4 s (+17%) |
| I2 (26%) | 75.9 | 42.3 | 73.4 | **−33.6 s (−44%)** | −2.5 s (−3%) |
| I3 (3%) | 64.2 | 36.8 | 63.6 | **−27.4 s (−43%)** | −0.6 s (−1%) |

### Steps and work (median)

| Cell | Read / clean / aggregate / profile (s) | Spark tasks | Spark CPU (s) | GC (s) | Shuffle (MB) | Spilled, in memory (MB) | Executor peak heap (MB) |
|---|---|---|---|---|---|---|---|
| I1-L1 | 3.2 / 23.0 / 13.5 / 25.0 | 4,861 | 64.5 | 5.0 | 35.3 | 216 | 348 |
| I1-P1 | 3.2 / 16.3 / 6.7 / 13.4 | 829 | 30.9 | 2.0 | 30.2 | 146 | 357 |
| I1-K1 | 3.0 / 24.4 / 16.0 / 33.8 | 4,861 | 112.1 | 9.0 | 136.2 | 0 | 432 |
| I2-L1 | 5.2 / 22.4 / 12.0 / 22.5 | 4,289 | 34.5 | 4.0 | 5.2 | 26 | 192 |
| I2-P1 | 4.7 / 12.9 / 4.7 / 10.1 | 257 | 11.7 | 0.8 | 4.4 | 0 | 177 |
| I2-K1 | 4.3 / 23.3 / 12.5 / 23.0 | 4,289 | 46.7 | 3.9 | 20.3 | 0 | 255 |
| I3-L1 | 3.4 / 18.9 / 10.5 / 20.1 | 4,289 | 25.5 | 3.9 | 1.5 | 0 | 238 |
| I3-P1 | 4.0 / 9.9 / 4.5 / 7.7 | 257 | 8.0 | 1.1 | 1.1 | 0 | 122 |
| I3-K1 | 3.6 / 20.1 / 10.5 / 19.5 | 4,289 | 33.5 | 3.3 | 6.4 | 0 | 281 |

The disk part of the spill was small (at most 6 MB). The steps add up to about 10 s less than the run time: that is the Spark session start-up.

### Skew inside Spark (median of 3; figures in [benchmark/stages-X.csv](benchmark/stages-X.csv))

"Room join" is the first join, by room type: it reads every booking. A stage counts as skewed when one task reads at least 25% of its rows. Each run has 21 stages that read shuffle data.

| Cell | Room join: tasks | Largest task's share of the rows | Largest task / median task | Skewed stages | Time of their largest tasks (s) |
|---|---|---|---|---|---|
| I1-L1 | 200 | 72% | 2.9 s / 9 ms | 12 | 8.5 |
| I1-P1 | 8 | 75% | 2.1 s / 0.7 s | 13 | 6.9 |
| I1-K1 | 200 | **9%** | 1.2 s / 9 ms | **0** | 0 |
| I2-L1 | 200 | 90% | 1.5 s / 12 ms | 13 | 3.4 |
| I2-P1 | 8 | 91% | 1.0 s / 0.3 s | 13 | 2.9 |
| I2-K1 | 200 | **11%** | 0.7 s / 10 ms | **1** | 0.0 |
| I3-L1 | 200 | 100% | 0.5 s / 10 ms | 13 | 1.5 |
| I3-P1 | 8 | 100% | 0.7 s / 0.3 s | 16 | 2.9 |
| I3-K1 | 200 | **13%** | 0.4 s / 10 ms | **1** | 0.1 |

The `stages.jpg` screenshots show the room join of I1. In L1 one task reads 859,941 of the 1,193,910 rows and spills 81 MiB. In K1 the largest task reads 107,855 rows and nothing spills.

### Observations

- **Shuffle partitions: 8 instead of 200 makes the baseline 33–44% faster (25–34 s), on every input.**
  - With AQE off, every shuffle keeps the configured partition count. At 200, most tasks have almost nothing to do: the room join's median task reads 0 rows and runs about 10 ms. Scheduling and running 4,289–4,861 tiny tasks is the cost.
  - At 8 partitions, 2 per core, the run needs 257–829 tasks. Spark CPU halves or better (I1: 64.5 → 30.9 s), and GC time falls (I1: 5.0 → 2.0 s).
  - Clean, aggregate and profile, the steps that shuffle, account for the whole gain. The read step is unchanged.
  - **Memory:** bigger partitions did not cost memory here. The executor's peak heap is about the same (348 / 357 MB on I1), and the spill went down (216 → 146 MB on I1, 26 → 0 on I2). At ×10 the data is small, so 8 partitions are still small enough. On a much larger input, fewer partitions would mean more data per task and more spill: the partition count trades task overhead against memory per task.
  - Fewer partitions do not fix skew: the room join's largest task still reads 75% of the rows (I1), and 12–16 stages stay skewed.
  - The same effect is what AQE does automatically: L2 and L3, with AQE on, merge the 200 partitions into a few by size and run 21–41 tasks.
- **Salting removes the skew, but at this size it doesn't make the run faster.**
  - Inside Spark it works as intended. The room join's largest task drops from 72% of the rows to 9% (I1), from 90% to 11% (I2) and from 100% to 13% (I3), 1/8 of the hot key plus a little. The skewed stages go from 12–13 to 0–1. All spill disappears (I1: 216 MB → 0), because no single task has to sort most of the data any more.
  - The total doesn't improve: I1 is 12.4 s (17%) slower, I2 and I3 are within the noise (−2.5 and −0.6 s).
  - **The cost:** the shuffled data grows 4× for the same rows (I1: 35 → 136 MB; the room join alone: 2.9 → 11.1 MiB for 1,193,910 rows). Spark CPU rises by 31–74% (I1: 64.5 → 112.1 s), and the executor's peak heap by 18–33%. The likely reason: without salting, the rows that land in one partition are near-copies of each other and compress extremely well (2.5 bytes a row in the room join). A random bucket in every row breaks those repeats.
  - **The gain is small at this size:** the skewed tasks lasted 8.5 s in total on I1, and while they ran, the other 3 cores still had thousands of small tasks to do. So balancing saved less time than the extra shuffle work cost. On I2 and I3, the skewed tasks lasted only 1.5–3.4 s in total.
  - When salting pays: when one hot key's task takes most of a stage's time and the other cores sit idle. That needs much more data per key than ×10 gives, or a task that runs out of memory. Salting turns spill or out-of-memory into balanced work, at the price of more shuffle data and replicated lookup rows.
- **For the presentation:**
  - The cheapest answer to skew is not to shuffle by the skewed key at all. The default pipeline broadcasts the lookup tables and groups on integer keys before attaching names (L2/L3), and it is 2.3–3.4× faster than the baseline.
  - The next answer is AQE (on by default), which merges small partitions and can split skewed ones.
  - Manual salting is the last resort, and it has a measurable cost.

## ClickHouse blocks (H1–H3): the alternative solution on its own matrix

Plan section 3.4.2. The ClickHouse engine (4 SQL queries from a Python job, no Spark) on the same 3 inputs, with ClickHouse's container at 1, 2 and 4 CPUs (`BR5_CLICKHOUSE_CPUS`; memory 1 GB in all three), and 3 live settings, 5 runs per cell:

| Live | Preset | Change from the defaults |
|---|---|---|
| HL1 | `bench-clickhouse` (C1's preset) | – (`max_threads` = the server's `auto(N)`) |
| HL2 | `bench-clickhouse-1thread` | `clickhouse.max_threads` = 1 |
| HL3 | `bench-clickhouse-nomemlimit` (added during the test, see below) | `clickhouse.limit_threads_by_memory` = false, which sends `max_threads_min_free_memory_per_thread = 0` |

### The finding that changed the test: ClickHouse used one thread whatever its CPUs

With the defaults (HL1), ClickHouse reported `max_threads` = `auto(2)` and `auto(4)` on 2 and 4 CPUs, as planned. Yet 55–56 of each block's 60 queries ran on **one thread**, and the query times did not change with the CPUs.

The cause is a ClickHouse 26.8 setting, `max_threads_min_free_memory_per_thread`, with a default of 1 GiB. ClickHouse lowers a query's thread count unless each thread has that much free memory. Our container has 1 GiB in total, so every query is cut to one thread. The query plan shows it:
- with the default, the fact table's 5 parts are read one after another (`Concat 5 → 1`, one stream);
- with the setting at 0, 4 parallel streams (`MergeTreeSelect … × 4`).

Lowering the read-splitting thresholds (`merge_tree_min_rows/bytes_for_concurrent_read`) changed nothing. A synthetic 30-million-row table was also read in one stream until this setting was turned off.

This is a memory-management effect, not a CPU one: ClickHouse protects itself from running out of memory by giving up parallelism. HL3 was added to measure what the extra CPUs give once that limit is lifted. The new setting is in the registry, so it is also on the Advanced Settings page.

### Run time and query figures (median of 5)

"Inside the steps" leaves out starting Python and connecting (1.3–1.5 s in every run). Query time, CPU, threads and memory are ClickHouse's own figures from `system.query_log`, for the run's 4 queries.

| Input | Cell | Run time (s): H1 / H2 / H3 | Inside the steps (s) | Query time (ms) | ClickHouse CPU (ms) | Peak threads | Peak query memory (MB) |
|---|---|---|---|---|---|---|---|
| I1 (100%) | HL1 defaults | 1.86 / 1.97 / 1.91 | 0.47 / 0.52 / 0.49 | 260 / 289 / 267 | 251 / 283 / 264 | 1 / 1 / 1 | 10.9 |
| | HL2 1 thread | 1.88 / 1.85 / 1.86 | 0.47 / 0.48 / 0.46 | 254 / 278 / 255 | 249 / 271 / 249 | 1 / 1 / 1 | 10.9 |
| | HL3 no memory limit | 2.03 / 1.91 / **1.78** | 0.57 / 0.46 / **0.40** | 358 / 233 / **187** | 341 / 379 / 468 | 1 / 3 / 5 | 10.9 / 14.6 / 16.6 |
| I2 (26%) | HL1 | 1.72 / 1.80 / 1.68 | 0.30 / 0.36 / 0.31 | 102 / 159 / 99 | 96 / 145 / 95 | 1 / 1 / 1 | 6.1 |
| | HL2 | 1.71 / 1.74 / 1.73 | 0.29 / 0.30 / 0.31 | 93 / 101 / 110 | 87 / 97 / 105 | 1 / 1 / 1 | 6.1 |
| | HL3 | 1.79 / 1.80 / 1.77 | 0.33 / 0.28 / 0.29 | 122 / 87 / 86 | 115 / 130 / 179 | 1 / 3 / 5 | 6.1 / 8.7 / 10.2 |
| I3 (3%) | HL1 | 1.61 / 1.62 / 1.60 | 0.25 / 0.26 / 0.26 | 44 / 50 / 57 | 42 / 46 / 47 | 1 / 1 / 1 | 5.2 |
| | HL2 | 1.60 / 1.66 / 1.60 | 0.25 / 0.24 / 0.24 | 50 / 46 / 41 | 39 / 44 / 36 | 1 / 1 / 1 | 5.2 |
| | HL3 | 1.74 / 1.64 / 1.74 | 0.26 / 0.25 / 0.25 | 56 / 45 / 45 | 56 / 56 / 79 | 1 / 3 / 5 | 5.2 / 6.6 / 7.3 |

Peak threads include the query's own thread, so 5 means 4 reading threads. Rows scanned are the same in every cell of an input: 4,775,774 / 1,377,732 / 323,582.

### Observations

- **With the defaults, more CPUs change nothing.** HL1's query time on I1 is 260 / 289 / 267 ms on 1 / 2 / 4 CPUs, and HL2 (forced to 1 thread) is the same as HL1 in every block, because HL1 is also on one thread (see the finding above).
- **With the memory limit lifted (HL3), the CPUs are used.**
  - I1's query time falls from 358 ms (1 CPU) to 233 ms (2) and 187 ms (4): 1.9× faster on 4 CPUs. Against HL1 on the same 4 CPUs: 267 → 187 ms (−30%). I2 gains 99 → 86 ms on 4 CPUs; I3 (35,360 rows) gains nothing measurable.
  - Parallel work costs more CPU in total: 264 → 468 ms of ClickHouse CPU on I1 (4 threads), and the peak query memory grows with the threads (10.9 → 16.6 MB). That is the memory the default limit protects.
  - **The run time barely moves:** 1.91 → 1.78 s on I1. The queries are only 0.2–0.5 s of a 1.6–2.0 s run; the rest is starting the Python job and connecting, which no ClickHouse setting changes.
- **Spark against ClickHouse at the same CPUs** (Spark's default L2 and best L3 against ClickHouse's defaults HL1; Spark ran with ClickHouse at 1 CPU, which only served the reader):

  | Input | 1 CPU: Spark L2 / ClickHouse | 2 CPUs: Spark L2 / ClickHouse | 4 CPUs: Spark L3 (best) / ClickHouse | ClickHouse faster by (1 / 2 / 4 CPUs) |
  |---|---|---|---|---|
  | I1 | 38.3 / 1.86 s | 32.0 / 1.97 s | 27.2 / 1.91 s | 20.6× / 16.2× / 14.2× |
  | I2 | 38.0 / 1.72 s | 29.2 / 1.80 s | 25.0 / 1.68 s | 22.1× / 16.2× / 14.9× |
  | I3 | 29.7 / 1.61 s | 27.7 / 1.62 s | 23.6 / 1.60 s | 18.4× / 17.1× / 14.8× |

  Inside the steps the gap is larger: on 4 CPUs, 18.5 / 16.3 / 15.1 s for Spark's best against 0.49 / 0.31 / 0.26 s. More CPUs narrow the gap only because Spark gains from them and ClickHouse, at this size, does not need them.
- **For the presentation (memory management):** both engines trade speed for memory safety by default. Spark spills to disk when a task's data doesn't fit (block X). ClickHouse runs fewer threads when memory is short. In a 1 GB container that means one thread, and lifting the limit buys 30% on the queries at the cost of 77% more CPU and 50% more memory per query.

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
- **H1–H3:** the ClickHouse runs varied by 0.1–0.35 s within a cell (5 runs). The HL3 cells were run 4–13 minutes after HL1 and HL2. H1's HL3 cell, which is the same work as HL1 on 1 CPU (one thread in both), was 0.07–0.17 s slower in total and up to 100 ms slower in query time: differences of that size between the HL3 and HL1 rows are noise. The HL3 rows among themselves (H1 → H3) were run within 3 minutes.
- **X:** all 9 cells had 3 runs within 11% of each other (D13 of the plan: no third-run rule needed). The I1-L1 runs (74.2–78.0 s) were close to the earlier S3 ones (60.3–81.2 s).

The medians are robust to one slow run, but differences under about 10% between cells measured at different times should not be over-read.

## Behaviour verified (plan section 8)

| # | Check | Result |
|---|---|---|
| 1 | Start-up configuration | Every run recorded the block's `cluster.worker_cores`: 1 for all 23 S1 runs, 2 for all 29 S2 runs, 4 for all 26 S3 runs. During S3, Prometheus showed the `spark-worker` CPU limit at 4. In S1, Grafana shows the worker at 1 core, with its CPU close to the 1.0 limit through the baseline runs |
| 2 | Live settings | Each run's "settings from" is its cell's preset. For L1, Spark itself reports AQE off and broadcast -1. Every L2 and L3 run sent 2 or 4 read queries whose rows add up exactly to the rows read. L1 (driver reader) and C1 send one query or their own SQL instead |
| 3 | Rows read per input | 1,193,900 / 309,600 / 35,360 in every run, as planned |
| 4 | Same input, same result | All 247 runs agree per input (blocks X and H1–H3: each run compared with S2's L2 reference, including salting and every ClickHouse setting), Spark (all three worker sizes, all live settings) and the ClickHouse engine: 10 cards and 132 month rows; 5 and 72; 1 and 24. Each S1 run was compared with S2's L2 reference run. This needed a fix to the check (below) |
| 5 | All succeeded; repeats close | All succeeded, and every run has its Spark figures. Third runs: 5 cells in S3, 1 in S2, 4 in S1 (see Noise) |
| 6 | Screenshots | 36 files for S1, 39 for S2 and 36 for S3. A sample from each live setting and the engine comparison was checked by eye |
| 7 | Stack restored | Worker back to 2 cores after S3, S1 and X; ClickHouse back to 1 CPU (`auto(1)`) after H1–H3; `make br5-check-dashboards` passes (37 queries, 0 failed, 0 empty). After H1–H3: 0 failed, 2 empty ("Import steps" and "Spark task time per step"), because the dashboard shows the last 7 days and the last import and Spark run were older |
| 8 | Block X settings | All 28 block X runs recorded 4 worker cores, AQE off and broadcast -1, as Spark itself reports. P1 recorded `spark.sql.shuffle.partitions` 8 and K1 `pipeline.salt_buckets` 8. The plans of all three show only `SortMergeJoin` (18 in each run), no `BroadcastHashJoin`. K1's joins have `salt` in their keys |
| 9 | Salting really happened | See the skew table: the largest task's share of the room join drops to 9–13% in K1, against 72–100% in L1 and P1 |
| 10 | ClickHouse start-up configuration | At the start and end of each block, the container's CPU limit and `max_threads` were `1` / `auto(1)`, `2` / `auto(2)`, `4` / `auto(4)`; every run's snapshot has the same `cluster.clickhouse_max_threads` |
| 11 | ClickHouse live settings, no cache | Every query: `query_cache_usage` = `None` (the cache was not used). HL3 queries carry `max_threads_min_free_memory_per_thread` = 0; HL2 queries `max_threads` = 1 (on H1 the setting equals the server's `auto(1)`, so ClickHouse doesn't log it as changed). Threads: HL2 used 1 thread in all 180 queries; HL1 1 thread in 55–56 of 60 per block (the rest, joins, 2–5); HL3 on 2 and 4 CPUs used more than one thread in all 120 (3 and 5 at the median). The plan expected HL1 to use N threads on N CPUs: see the finding above |

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
- **ClickHouse failed a query right after its first restart** (before the H blocks, in a smoke run): "memory limit exceeded: would use 921.67 MiB".
  - Cause: a background merge of old `system.metric_log` parts (29,470 rows written up to 2026-10-24, although the log is turned off) needed about 728 MiB, failed on the 1 GB limit, and was retried every 1–2 s, at about 90% CPU. This is the merge for which the project turned `metric_log` off.
  - The retries also ran on 2026-10-23 and early 2026-10-24, but not during any benchmark block (checked in `system.part_log`).
  - Fix: `TRUNCATE TABLE system.metric_log`. ClickHouse then used 118 MiB instead of 847 MiB and had no merges. All H runs were made after this.
- **The ClickHouse compare screenshots were 10,795 px tall,** mostly empty below the page. They were trimmed to the content (about 2,650 px).
- **The plan's check for 4 read slices was wrong:** ClickHouse counts scanned rows, not returned rows. It now counts the read queries and their rows.

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `S2-I1-L1/`), from the cell's first measured run. The JPEGs are stored with Git LFS (204 files, 62 MB; plan risk R6):

| File | Shows |
|---|---|
| `finder.jpg` | The result as the user sees it, with the run summary: rows, steps, settings used |
| `compare.jpg` | Run History against the reference run: run time, steps, settings that differ, resources, same result |
| `resources.jpg` | Grafana Resource use during the run: CPU and memory per container (Spark cells only) |
| `spark.jpg` | The run's Spark jobs page, jobs named after the steps (Spark cells only) |
| `stages.jpg` | Block X only: the room join's stage page, with min / median / max per task of the duration, the rows read and the spill |

References in `compare.jpg`:
- L1 and L3 are compared with L2 of the same input and block.
- L2 of S1 and S3 is compared with S2's L2: the only setting that differs is the worker's cores.
- C1 is compared with S2's L2: Spark against the ClickHouse engine.
- S2's L2 cells are the references, so they have no `compare.jpg`.
- Block X: P1 and K1 are compared with L1 of the same input in block X, which has no `compare.jpg`.
- ClickHouse blocks (`H1-I1-HL1/` and so on, `finder.jpg` and `compare.jpg` only): HL2 and HL3 are compared with HL1 of the same input and block; HL1 of H2 and H3 with H1's HL1. Run History's compare page shows ClickHouse's queries, rows read, CPU, peak memory and peak threads.

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

Block X (4 cores):

| Cell | L1 | P1 | K1 |
|---|---|---|---|
| I1 | 78.0, 74.8, 74.2 (warm-up 76.1) | 50.0, 49.6, 50.6 | 83.9, 88.6, 87.2 |
| I2 | 71.0, 76.7, 75.9 | 43.2, 41.8, 42.3 | 73.4, 73.3, 74.5 |
| I3 | 65.6, 62.2, 64.2 | 39.5, 36.8, 35.6 | 67.0, 63.5, 63.6 |

ClickHouse blocks (5 runs each; the warm-ups after each restart were I1-HL1, then I1-HL3 when HL3 was added: 2.20 / 1.99 / 1.87 s on H1 / H2 / H3):

| Cell | H1 (1 CPU) | H2 (2 CPUs) | H3 (4 CPUs) |
|---|---|---|---|
| I1-HL1 | 1.91, 1.83, 1.94, 1.79, 1.86 | 1.82, 1.97, 2.17, 2.08, 1.94 | 2.04, 1.91, 1.78, 1.76, 1.93 |
| I1-HL2 | 1.91, 1.82, 1.88, 1.90, 1.88 | 1.79, 2.01, 1.85, 1.85, 1.95 | 1.77, 1.88, 1.92, 1.76, 1.86 |
| I1-HL3 | 2.18, 2.03, 2.05, 1.97, 1.89 | 1.85, 1.91, 2.06, 1.95, 1.89 | 1.94, 1.99, 1.76, 1.75, 1.78 |
| I2-HL1 | 1.68, 1.72, 1.64, 1.72, 1.78 | 1.87, 2.00, 1.80, 1.62, 1.65 | 1.85, 1.65, 1.68, 1.78, 1.64 |
| I2-HL2 | 1.72, 1.72, 1.71, 1.63, 1.68 | 1.65, 1.81, 1.75, 1.74, 1.63 | 1.75, 1.70, 1.66, 1.76, 1.73 |
| I2-HL3 | 1.73, 1.67, 1.85, 1.79, 1.84 | 1.71, 1.67, 1.80, 1.89, 1.82 | 1.72, 1.83, 1.75, 1.80, 1.77 |
| I3-HL1 | 1.61, 1.65, 1.60, 1.65, 1.60 | 1.75, 1.61, 1.59, 1.72, 1.62 | 1.60, 1.63, 1.59, 1.56, 1.72 |
| I3-HL2 | 1.60, 1.54, 1.60, 1.75, 1.68 | 1.78, 1.85, 1.60, 1.64, 1.66 | 1.57, 1.61, 1.66, 1.60, 1.57 |
| I3-HL3 | 1.99, 1.97, 1.62, 1.62, 1.74 | 1.87, 1.80, 1.64, 1.61, 1.63 | 1.69, 1.57, 1.76, 1.74, 1.75 |
