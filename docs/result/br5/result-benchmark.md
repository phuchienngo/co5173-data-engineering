# BR5 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br5/plan-benchmark.md). **This file is updated after each block** of the benchmark.

## Progress

| Block | Spark worker | Status | Runs |
|---|---|---|---|
| S3 | 4 cores | **Done** (2026-10-19 and 2026-10-20) | 3 warm-ups + 23 measured, all succeeded |
| S2 + C1 | 2 cores; ClickHouse engine | Not run yet | |
| S1 | 1 core | Not run yet | |

## Setup

- **Stack:** as in [result-monitoring.md](result-monitoring.md):
  - Spark: 1 worker with 2 GB; its cores are the block's (`BR5_WORKER_CORES`).
  - ClickHouse: 1 CPU, 1 GB.
  - The driver runs in `br5-app` (1 CPU, 2 GB).
  - The Docker host has 14 CPUs and 16 GB.
- **Data:** imported once at ×10 for the whole benchmark: 1,193,900 bookings in 99.5 s, all import checks equal.
- **Live presets** (plan section 3.3): `bench-baseline`, `bench-default`, `bench-read4`. All of them allow a run up to 4 cores.
- **Measured runs:**
  - Each cell has 2 runs, plus a third when the two differ by more than 15%.
  - The runs are interleaved across the cells.
  - One warm-up after each worker restart is not counted.
  - Every run is listed in [benchmark/runs.csv](benchmark/runs.csv).
- **How figures are given:**
  - Figures are the **median of a cell's measured runs**, each figure separately.
  - Run times also list every run, in order.
  - Spark and ClickHouse figures are exact and stored with each run. Container CPU and memory come from Prometheus (5 s samples).

## The inputs and their answers

The answer depends on the input only, never on the settings (see "Behaviour verified").

| Input | Bookings read | Used | Low season | Cards | Top card |
|---|---|---|---|---|---|
| I1 All / All / All | 1,193,900 | 743,890 | Dec – Jan: demand is 52% of peak | 10 (573 targets, 68 qualify) | Room A · United Kingdom: potential 1,036 room-nights per month; early-bird (≥ 50 days) + strong discount |
| I2 City Hotel / Portugal / All | 309,600 | 105,740 | Apr (54%), Aug (58%), Nov – Feb (52%) | 5 (7 targets, 5 qualify) | Room A: potential 595; last-minute deal + moderate discount |
| I3 Resort Hotel / United Kingdom / A | 35,360 | 31,450 | Jul – Aug (36%), Nov – Apr (37%) | 1 | potential 853; early-bird (≥ 70 days) + strong discount |

## Block S3: Spark worker with 4 cores

### Run time (seconds)

Median in bold; each run in brackets, in order.

| Input | L1 baseline | L2 default | L3 4 read partitions | Baseline vs default |
|---|---|---|---|---|
| I1 (100% of the data) | **78.7** (60.3, 78.7, 81.2) | **29.5** (27.7, 31.3) | **27.2** (27.1, 27.2) | 2.67× |
| I2 (26%) | **69.6** (59.2, 69.6, 73.6) | **27.0** (24.8, 29.9, 27.0) | **25.0** (24.5, 25.4) | 2.58× |
| I3 (3%) | **61.5** (56.3, 69.1, 61.5) | **26.7** (25.7, 42.2, 26.7) | **23.6** (23.4, 23.7) | 2.30× |

### Where the time goes (seconds)

"Start-up" is the run time outside the steps, mostly creating the Spark session. The rank, recommend and write steps take under 0.3 s and are left out.

| Cell | Run | Start-up | read | clean | aggregate | profile |
|---|---|---|---|---|---|---|
| I1-L1 | 78.7 | 9.9 | 3.3 | 22.5 | 12.4 | 26.4 |
| I1-L2 | 29.5 | 9.6 | 10.2 | 1.4 | 2.0 | 6.2 |
| I1-L3 | 27.2 | 8.7 | 9.1 | 1.4 | 1.9 | 5.8 |
| I2-L1 | 69.6 | 10.9 | 4.7 | 20.6 | 11.6 | 21.8 |
| I2-L2 | 27.0 | 9.6 | 8.6 | 1.6 | 1.7 | 5.4 |
| I2-L3 | 25.0 | 8.7 | 8.3 | 1.4 | 1.5 | 5.0 |
| I3-L1 | 61.5 | 10.2 | 3.6 | 19.0 | 10.1 | 19.0 |
| I3-L2 | 26.7 | 10.1 | 8.3 | 1.3 | 1.6 | 5.3 |
| I3-L3 | 23.6 | 8.5 | 7.8 | 1.1 | 1.5 | 4.5 |

### Work and resources

The ClickHouse column counts the rows it **scanned** (index granules matching the filter, once per read query), not the rows returned. Container CPU covers the run's time window.

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

## Observations so far (S3 only)

- **The live settings matter far more than the input.**
  - The naive baseline is 2.3–2.7× slower than the defaults on every input.
  - Its time is in clean, aggregate and profile, 10–26 s each against 1–6 s with the defaults:
    - Without the cache, each of these steps works on the data again.
    - With AQE off, every shuffle keeps Spark's 200 partitions: 4,289–4,861 tasks against 25–29.
    - With broadcast off and the early join, every lookup join is a sort-merge join over all bookings.
  - The executor used 2.7 cores on average during the I1 baseline (215 CPU-seconds in 79 s). It spilled 229 MB, against nothing with the defaults.
- **The baseline's cost hardly depends on the data size.** Its task count is fixed by the 200 partitions per shuffle, so I3 (3% of the data) still takes 61.5 s against 78.7 s for all of it.
- **With the defaults, the data size matters little at this scale.**
  - 34× more data (I3 to I1) adds 2.8 s (26.7 to 29.5 s) and 4.4 s of Spark CPU.
  - Fixed costs dominate: about 9–10 s of Spark start-up, and a read step of about 8 s even for 35,360 rows. That step includes creating and filling the cache and the Arrow conversion.
- **4 read partitions are 2–3 s faster than 2 on 4 cores (8–12%), but only part of that is the setting.**
  - The L3 cells ran the day before the L2 cells. Their Spark start-up, which the read setting can't change, was also 0.9–1.6 s shorter.
  - Inside the steps the gain is 1.1–1.6 s. Most of it is in the read step: 0.3–1.1 s shorter.
  - The parallelism isn't free: ClickHouse scans twice as many rows, because each slice scans the whole filtered range. Spark also uses more CPU in total (10.9 against 8.3 s for I1).
  - On 1 core (S1) the extra slices are expected to cost time instead (plan D5): to be measured.
- **The baseline's driver reader loads the driver:** `br5-app` used 61–68 CPU-seconds against 21–24 with the parallel reader. It read through ClickHouse once instead of once per slice. Its driver memory (512 MB) was enough at ×10 (plan risk R2).

## Noise

The second set of runs on 2026-10-20 was slower across the board, so 5 of the 6 L1/L2 cells needed a third run.
- **In the baselines the work itself got slower.**
  - I1's Spark CPU time for the same tasks rose from 49 s (run 1) to 70–72 s (runs 2 and 3).
  - That points to the laptop's CPU: heat or load outside Docker.
  - Nothing else in the stack was busy: no ClickHouse merge, no other busy container, no open app page.
- **One default run (I3-L2 run 2, 42.2 s) lost its time in Spark start-up instead:** 21.9 s outside the steps, against 9–10 s.
- **The L3 cells, run the day before as two separate sessions, were within 4%.**

The medians are robust to one slow run, but differences under about 10% between cells measured at different times should not be over-read.

## Behaviour verified (plan section 8)

| # | Check | S3 result |
|---|---|---|
| 1 | Start-up configuration | All 26 S3 runs recorded `cluster.worker_cores` = 4. Prometheus showed the `spark-worker` CPU limit at 4, and Grafana shows 4 cores given to each run |
| 2 | Live settings | Each run's "settings from" is its cell's preset. For L1, Spark itself reports AQE off and broadcast -1. For L3, each run sent 4 read queries whose rows add up exactly to the rows read |
| 3 | Rows read per input | 1,193,900 / 309,600 / 35,360 in every run, as planned |
| 4 | Same input, same result | All 26 runs agree per input: 10 cards and 132 month rows; 5 and 72; 1 and 24. This needed a fix to the check (below) |
| 5 | All succeeded; repeats close | All succeeded. 5 cells needed a third run (see Noise) |
| 6 | Screenshots | 36 files, 4 per cell; a sample from each live setting checked by eye |
| 7 | Stack restored | Worker back to 2 cores; `make br5-check-dashboards` passes (37 queries, 0 failed, 0 empty) |

## Issues found and fixed

- **The "same result" check rounded before comparing.**
  - City Hotel / Portugal / All with 4 read partitions seemed to differ from 2 partitions in two average prices (102.08 vs 102.09).
  - The stored values are 102.08499999999998 and 102.0850000000001: the same sum, added in another order, rounds to different 2-decimal values.
  - `compare_runs.differences` now compares numbers within a relative 1e-9, and has a unit test. The ClickHouse engine against Spark, and ×1 against ×10, still match under it.
- **Screenshots of the first trial:**
  - The Promotion Finder showed All / All / All above a run's result.
  - The Run History tables cut off rows.
  - The Grafana window included part of the next run.

  All three were fixed before the block ran (plan sections 4 and 6).
- **The script's screenshot height limit** was raised from 8,000 to 12,000 px: I1's result page is 7,841 px tall with its 10 cards.
- **The plan's check for 4 read slices was wrong:** ClickHouse counts scanned rows, not returned rows. It now counts the read queries and their rows.

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `S3-I1-L1/`), from the cell's first measured run:

| File | Shows |
|---|---|
| `finder.jpg` | The result as the user sees it, with the run summary: rows, steps, settings used |
| `compare.jpg` | Run History against the reference run: run time, steps, settings that differ, resources, same result |
| `resources.jpg` | Grafana Resource use during the run: CPU and memory per container |
| `spark.jpg` | The run's Spark jobs page, jobs named after the steps |

References in `compare.jpg`:
- L1 and L3 are compared with L2 of the same input.
- The S3 L2 cells are compared with their own second run until S2 has run. Then they are retaken against S2's L2.
