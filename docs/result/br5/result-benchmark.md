# BR5 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br5/plan-benchmark.md). **This file is updated after each block** of the benchmark.

## Progress

| Block | Spark worker | Status | Runs |
|---|---|---|---|
| S3 | 4 cores | **Done** (2026-10-19 and 2026-10-20) | 3 warm-ups + 23 measured, all succeeded |
| S2 + C1 | 2 cores; ClickHouse engine | **Done** (2026-10-20) | 1 warm-up + 19 Spark + 9 ClickHouse measured, all succeeded |
| S1 | 1 core | Not run yet | |

## Setup

- **Stack:** as in [result-monitoring.md](result-monitoring.md):
  - Spark: 1 worker with 2 GB; its cores are the block's (`BR5_WORKER_CORES`).
  - ClickHouse: 1 CPU, 1 GB.
  - The driver runs in `br5-app` (1 CPU, 2 GB).
  - The Docker host has 14 CPUs and 16 GB.
- **Data:** imported once at ×10 for the whole benchmark: 1,193,900 bookings in 99.5 s, all import checks equal.
- **Live presets** (plan section 3.3):
  - L1 `bench-baseline`, L2 `bench-default`, L3 `bench-read4`. All of them allow a run up to 4 cores.
  - C1 `bench-clickhouse`: the ClickHouse SQL engine.
- **Measured runs:**
  - Each Spark cell has 2 runs, plus a third when the two differ by more than 15%. ClickHouse cells have 3 runs.
  - The runs are interleaved across the cells.
  - One warm-up after each worker restart is not counted.
  - Every run is listed in [benchmark/runs.csv](benchmark/runs.csv); the run times of every run are in the appendix.
- **How figures are given:**
  - Figures are the **median of a cell's measured runs**, each figure separately.
  - Spark and ClickHouse figures are exact and stored with each run. Container CPU and memory come from Prometheus (5 s samples).

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
| I1 (100% of the data) | L1 baseline | | 99.5 | 78.7 |
| | L2 default | | 32.0 | 29.5 |
| | L3 4 read partitions | | 32.9 | 27.2 |
| | C1 ClickHouse engine | | 2.3 | |
| I2 (26%) | L1 baseline | | 76.1 | 69.6 |
| | L2 default | | 29.2 | 27.0 |
| | L3 4 read partitions | | 29.7 | 25.0 |
| | C1 ClickHouse engine | | 1.9 | |
| I3 (3%) | L1 baseline | | 70.3 | 61.5 |
| | L2 default | | 27.7 | 26.7 |
| | L3 4 read partitions | | 28.4 | 23.6 |
| | C1 ClickHouse engine | | 1.9 | |

The C1 runs were made in the S2 block; the ClickHouse engine doesn't use the Spark worker, so its worker size doesn't apply.

### Effect of each dimension

| Comparison | I1 | I2 | I3 |
|---|---|---|---|
| **Live:** baseline vs default, S2 | 3.11× | 2.61× | 2.54× |
| **Live:** baseline vs default, S3 | 2.67× | 2.58× | 2.30× |
| **Live:** 4 read partitions vs 2, S2 | +0.9 s | +0.5 s | +0.7 s |
| **Live:** 4 read partitions vs 2, S3 | −2.3 s | −2.0 s | −3.1 s |
| **Start-up:** 4 cores vs 2, baseline | −20.8 s (−21%) | −6.5 s (−9%) | −8.8 s (−13%) |
| **Start-up:** 4 cores vs 2, default | −2.5 s (−8%) | −2.2 s (−8%) | −1.0 s (−4%) |
| **Input:** I1 vs I3 (34× the data), default S2 | +4.3 s | | |

## Spark vs the ClickHouse engine

"Inside the steps" leaves out the time before the first step: creating the Spark session (Spark), starting Python and connecting (ClickHouse).

| Input | Spark default, S2: total / inside the steps | Best Spark cell (S3 L3): total / inside the steps | ClickHouse engine: total / inside the steps | Spark default vs ClickHouse (total) |
|---|---|---|---|---|
| I1 | 32.0 / 22.3 | 27.2 / 18.5 | 2.3 / 0.7 | 13.9× |
| I2 | 29.2 / 19.1 | 25.0 / 16.3 | 1.9 / 0.4 | 15.4× |
| I3 | 27.7 / 17.4 | 23.6 / 15.1 | 1.9 / 0.3 | 14.6× |

ClickHouse engine work per run:
- 4 queries, using 0.4 / 0.2 / 0.1 s of ClickHouse CPU (I1 / I2 / I3).
- ClickHouse scanned 4,775,774 / 1,377,732 / 323,582 rows: the fact table once per query.
- The `clickhouse` container used 3.0–3.5 CPU-seconds in the run's window, which includes its background work; `br5-app` (the Python job) used 3.2–3.3.

## Where the time goes (seconds, median)

"Start-up" is the run time outside the steps, mostly creating the Spark session. The rank, recommend and write steps take under 0.3 s and are left out.

| Cell | S2: run | start-up | read | clean | aggregate | profile | S3: run | start-up | read | clean | aggregate | profile |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| I1-L1 | 99.5 | 10.1 | 3.1 | 27.6 | 19.1 | 32.2 | 78.7 | 9.9 | 3.3 | 22.5 | 12.4 | 26.4 |
| I1-L2 | 32.0 | 9.7 | 11.1 | 1.9 | 2.4 | 6.8 | 29.5 | 9.6 | 10.2 | 1.4 | 2.0 | 6.2 |
| I1-L3 | 32.9 | 9.9 | 11.0 | 1.8 | 3.0 | 7.1 | 27.2 | 8.7 | 9.1 | 1.4 | 1.9 | 5.8 |
| I2-L1 | 76.1 | 9.9 | 4.5 | 22.4 | 13.4 | 25.6 | 69.6 | 10.9 | 4.7 | 20.6 | 11.6 | 21.8 |
| I2-L2 | 29.2 | 10.1 | 9.6 | 1.7 | 2.0 | 5.6 | 27.0 | 9.6 | 8.6 | 1.6 | 1.7 | 5.4 |
| I2-L3 | 29.7 | 9.8 | 10.5 | 1.4 | 2.1 | 5.7 | 25.0 | 8.7 | 8.3 | 1.4 | 1.5 | 5.0 |
| I3-L1 | 70.3 | 9.6 | 3.9 | 22.5 | 11.6 | 22.5 | 61.5 | 10.2 | 3.6 | 19.0 | 10.1 | 19.0 |
| I3-L2 | 27.7 | 10.3 | 9.1 | 1.4 | 1.8 | 5.0 | 26.7 | 10.1 | 8.3 | 1.3 | 1.6 | 5.3 |
| I3-L3 | 28.4 | 9.9 | 9.8 | 1.2 | 1.8 | 5.6 | 23.6 | 8.5 | 7.8 | 1.1 | 1.5 | 4.5 |

## Work and resources (median)

The ClickHouse column counts the rows it **scanned** (index granules matching the filter, once per read query), not the rows returned. Container CPU covers the run's time window.

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

## Observations so far (S2, S3 and C1)

- **The live settings matter far more than the input or the cluster size.**
  - The naive baseline is 2.3–3.1× slower than the defaults on every input and both worker sizes.
  - Its time is in clean, aggregate and profile, 10–32 s each against 1–7 s with the defaults:
    - Without the cache, each of these steps works on the data again.
    - With AQE off, every shuffle keeps Spark's 200 partitions: 4,245–4,861 tasks against 21–29.
    - With broadcast off and the early join, every lookup join is a sort-merge join over all bookings.
  - On I1 the baseline spilled 95 MB (2 cores) and 229 MB (4 cores) to disk; the defaults spilled nothing.
- **The start-up configuration helps the baseline more than the defaults.**
  - From 2 to 4 cores the baseline gained 9–21%: its thousands of small tasks can use the extra cores. The executor used 2.7 cores on average for I1 on 4 cores (215 CPU-seconds in 79 s).
  - The defaults gained only 4–8% (1.0–2.5 s): they run 21–29 tasks, and most of their time is fixed cost.
  - So more hardware narrows the gap between a bad and a good configuration, but doesn't close it: the baseline on 4 cores is still 2.2–2.5× slower than the defaults on 2.
- **The best live setting depends on the start-up configuration (plan D5).**
  - 4 read partitions instead of 2 are slightly *slower* on 2 cores (+0.5 to +0.9 s; its read step is no shorter). On 4 cores they are faster (−2.0 to −3.1 s).
  - Part of the 4-core gain may be the machine: the S3 L3 runs were made a day before S3 L2, and their Spark start-up, which the read setting can't change, was also 0.9–1.6 s shorter. Inside the steps the 4-core gain is 1.1–1.6 s.
  - The extra slices aren't free on either size: ClickHouse scans twice as many rows, since each slice scans the whole filtered range.
- **With the defaults, the data size matters little at this scale.**
  - 34× more data (I3 to I1) adds 4.3 s on 2 cores and 2.8 s on 4 cores.
  - Fixed costs dominate: about 10 s of Spark start-up, and a read step of about 9 s even for 35,360 rows. That step includes creating and filling the cache and the Arrow conversion.
  - The baseline's cost hardly depends on the data size either: its task count is fixed by the 200 partitions per shuffle (I3 at 3% of the data: 70.3 s against 99.5 s on 2 cores).
- **The ClickHouse engine is 14–15× faster than Spark's defaults, with 1 CPU against Spark's 2.**
  - It answers in 1.9–2.3 s against 27.7–32.0 s. Even without Spark's session start-up, Spark's steps take 17–22 s against 0.3–0.7 s.
  - This is the "push the query to the column store" alternative: for these aggregations over data already in ClickHouse, sending the query to the data wins at this size.
  - Both engines give exactly the same cards and monthly figures.
- **The baseline's driver reader loads the driver:** `br5-app` used 60–68 CPU-seconds against 21–26 with the parallel reader. Its driver memory (512 MB) was enough at ×10 (plan risk R2).

## Noise

- **S3:** the second set of runs on 2026-10-20 was slower across the board, so 5 of the 6 L1/L2 cells needed a third run.
  - In the baselines the work itself got slower: I1's Spark CPU time for the same tasks rose from 49 s (run 1) to 70–72 s (runs 2 and 3). That points to the laptop's CPU: heat or load outside Docker. Nothing else in the stack was busy: no ClickHouse merge, no other busy container, no open app page.
  - One default run (I3-L2 run 2, 42.2 s) lost its time in Spark start-up instead: 21.9 s outside the steps, against 9–10 s.
  - The L3 cells, run the day before as two separate sessions, were within 4%.
- **S2:** only I1-L1 needed a third run (76.5, 100.8, 99.5 s). All other cells' runs were within 8%. The ClickHouse engine's runs varied by 0.2–0.5 s.

The medians are robust to one slow run, but differences under about 10% between cells measured at different times should not be over-read.

## Behaviour verified (plan section 8)

| # | Check | Result |
|---|---|---|
| 1 | Start-up configuration | Every run recorded the block's `cluster.worker_cores`: 4 for all 26 S3 runs, 2 for all 29 S2 runs. During S3, Prometheus showed the `spark-worker` CPU limit at 4. Grafana shows the cores given to each run |
| 2 | Live settings | Each run's "settings from" is its cell's preset. For L1, Spark itself reports AQE off and broadcast -1. Every L2 and L3 run sent 2 or 4 read queries whose rows add up exactly to the rows read. L1 (driver reader) and C1 send one query or their own SQL instead |
| 3 | Rows read per input | 1,193,900 / 309,600 / 35,360 in every run, as planned |
| 4 | Same input, same result | All 55 runs agree per input, Spark (both worker sizes, all live settings) and the ClickHouse engine: 10 cards and 132 month rows; 5 and 72; 1 and 24. This needed a fix to the check (below) |
| 5 | All succeeded; repeats close | All succeeded. Third runs: 5 cells in S3, 1 in S2 (see Noise) |
| 6 | Screenshots | 36 files for S3 and 39 for S2. A sample from each live setting and the engine comparison was checked by eye |
| 7 | Stack restored | Worker back to 2 cores after S3; `make br5-check-dashboards` passes (37 queries, 0 failed, 0 empty) |

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
- L2 of S3 (and later S1) is compared with S2's L2: the only setting that differs is the worker's cores.
- C1 is compared with S2's L2: Spark against the ClickHouse engine.
- S2's L2 cells are the references, so they have no `compare.jpg`.

## Appendix: every run (seconds, in order)

| Cell | S2 | S3 |
|---|---|---|
| I1-L1 | 76.5, 100.8, 99.5 | 60.3, 78.7, 81.2 |
| I1-L2 | 30.7, 33.3 | 27.7, 31.3 |
| I1-L3 | 32.1, 33.7 | 27.1, 27.2 |
| I2-L1 | 71.9, 80.2 | 59.2, 69.6, 73.6 |
| I2-L2 | 28.9, 29.4 | 24.8, 29.9, 27.0 |
| I2-L3 | 30.0, 29.3 | 24.5, 25.4 |
| I3-L1 | 67.7, 72.9 | 56.3, 69.1, 61.5 |
| I3-L2 | 27.9, 27.5 | 25.7, 42.2, 26.7 |
| I3-L3 | 29.4, 27.4 | 23.4, 23.7 |
| I1-C1 | 2.5, 2.3, 2.1 | |
| I2-C1 | 2.2, 1.9, 1.7 | |
| I3-C1 | 1.9, 1.7, 1.9 | |

Warm-ups (not counted): S3 24.1 (I3-L3), 27.7 (I1-L3), 59.9 (I1-L1); S2 80.8 (I1-L1).
