# BR8 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br7/plan-benchmark.md) (BR8 part, section 12). First runs of the tooling: [result-benchmark-smoke.md](result-benchmark-smoke.md).

## Progress

| Block | Configuration | Status | Runs |
|---|---|---|---|
| S2 | Spark worker at 2 cores | **Done** (2026-11-08 00:10–00:52) | 1 warm-up + 19 measured (2 per cell, a third for I1-L2 because of an executor restart), all succeeded |
| S1 | Spark worker at 1 core | **Done** (2026-11-08 05:35–06:46, second attempt) | 1 warm-up + 18 measured, all succeeded. The first attempt is void (see "Issues") |
| S3 | Spark worker at 4 cores | **Done** (06:46–07:26) | 1 warm-up + 19 measured (a third for I1-L2 because of an executor restart), all succeeded |
| V | Single options, 2 cores, I1 and I2 | **Done** (07:26–08:48) | 1 warm-up + 42 measured (3 per cell), all succeeded |
| H1 | ClickHouse engine, ClickHouse at 1 CPU | **Done** (08:48–08:56) | 1 warm-up + 30 measured (5 per cell), all succeeded |
| H2 | ClickHouse at 2 CPUs | **Done** (08:56–09:03) | 1 warm-up + 30 measured, all succeeded |
| H3 | ClickHouse at 4 CPUs | **Done** (09:03–09:10) | 1 warm-up + 30 measured, all succeeded |

`make br8-bench-check` passed for every block: 188 measured runs, 0 with problems. Each run had the block's settings, worker cores and ClickHouse CPUs, the expected check-ins read, and the **expected result fingerprint** (the rows of `prepare_br8.py`, see the table of inputs). Every run is listed in [benchmark/runs.csv](benchmark/runs.csv).

## Setup

- **Stack:** dev laptop, WSL with 7.4 GB RAM. ClickHouse 1 GB, with the block's CPUs. Spark: 1 worker with 2 GB and the block's cores, one executor of 512 MB, and the driver in `spark-local` (512 MB, 2 GB for the baseline, plan O5).
- **Data:** the TripWorld import of 2026-11-07, used for the whole benchmark: 2,764,448 destination check-ins, 687,173 places, 1,173 regions.
- **Inputs:**

  | Input | Region | Check-ins read | Daily visits | Places | Fingerprint (visits · places) |
  |---|---|---|---|---|---|
  | I1 | all destinations | 2,764,448 | 107,912 | 10,639 | `107912:7797127441102435416 · 10639:6599842764725940206` |
  | I2 | Q1490 (Tokyo) | 164,969 | 5,416 | 533 | `5416:15693983204303305139 · 533:4136802300216255460` |
  | I3 | Q1865 | 27,544 | 468 | 51 | `468:14393461901935006069 · 51:3928297759179194327` |

- **Live settings:**

  | Live | Preset | Change from the default |
  |---|---|---|
  | L1 | `bench-baseline` | Driver reader, Python UDF for the attraction test, no cache, pandas writer (`toPandas` → `insert_df`), AQE off, no broadcast joins, driver 2 GB |
  | L2 | `bench-default` | Nothing: parallel reader (2 slices), native `rlike`, cache, partition writer, AQE on |
  | L3 | `bench-read-4` | Parallel reader with 4 slices |
  | V1–V6 | `bench-python-udf`, `-pandas-udf`, `-no-cache`, `-no-broadcast`, `-no-aqe`, `-pandas-writer` | One option of the default each |
  | HL1 | `bench-clickhouse` | The same steps as ClickHouse SQL (`br8_bench_sql.py`) |
  | HL2 | `bench-clickhouse-nomemlimit` | HL1, with ClickHouse's memory-based thread limit off |

- **How figures are given:** the median of a cell's measured runs (no warm-up), each figure separately. Steps are the job's own timings. "Start-up" is the run time outside the steps (spark-submit, driver, session).

## Spark blocks (S1–S3)

### Run time and steps (median, seconds)

Given as S1 / S2 / S3 (1 / 2 / 4 worker cores).

| Input | Cell | Run time | read | clean | match | aggregate | write | Spark CPU |
|---|---|---|---|---|---|---|---|---|
| I1 all | L1 baseline | 531.9 / 276.2 / 258.8 | 23.2 / 16.4 / 18.2 | 27.0 / 15.2 / 17.2 | 97.8 / 49.5 / 44.3 | 89.4 / 44.2 / 39.0 | 287.9 / 145.2 / 132.6 | 170 / 172 / 305 |
| | L2 default | 164.4 / 105.7\* / 94.7\* | 27.6 / 17.0 / 16.8 | 41.7 / 31.9 / 23.6 | 58.6 / 34.7 / 26.7 | 10.2 / 6.1 / 5.7 | 19.4 / 10.2 / 8.7 | 63 / 72 / 107 |
| | L3 read 4 | 163.9 / 88.4 / 85.1 | 31.6 / 17.5 / 19.7 | 40.4 / 19.2 / 17.1 | 56.8 / 28.7 / 26.4 | 10.5 / 5.9 / 5.8 | 18.0 / 10.1 / 7.9 | 63 / 65 / 108 |
| I2 Tokyo | L1 | 260.2 / 145.9 / 126.2 | 15.6 / 10.3 / 10.6 | 11.1 / 7.2 / 6.0 | 53.0 / 29.8 / 25.9 | 42.2 / 22.3 / 18.6 | 132.1 / 70.9 / 57.6 | 57 / 62 / 105 |
| | L2 | 109.9 / 62.5 / 58.0 | 19.1 / 12.3 / 11.9 | 19.1 / 11.9 / 10.2 | 32.3 / 16.4 / 14.7 | 9.5 / 5.3 / 5.2 | 22.6 / 10.4 / 7.7 | 22 / 25 / 38 |
| | L3 | 111.8 / 60.6 / 58.4 | 21.9 / 12.5 / 13.6 | 20.2 / 11.2 / 9.3 | 29.9 / 16.6 / 14.3 | 10.5 / 5.6 / 6.0 | 21.7 / 9.0 / 8.3 | 24 / 24 / 41 |
| I3 Q1865 | L1 | 234.7 / 124.6 / 105.0 | 14.9 / 9.3 / 9.3 | 9.3 / 6.3 / 5.2 | 51.1 / 26.9 / 21.9 | 36.8 / 18.3 / 14.6 | 116.5 / 58.2 / 46.3 | 45 / 46 / 75 |
| | L2 | 94.6 / 57.5 / 52.7 | 19.1 / 11.9 / 11.3 | 17.9 / 10.3 / 9.1 | 25.0 / 14.1 / 12.2 | 9.3 / 5.7 / 4.6 | 17.2 / 9.6 / 6.9 | 18 / 19 / 29 |
| | L3 | 100.7 / 56.8 / 54.0 | 19.7 / 12.3 / 12.9 | 16.9 / 9.8 / 8.6 | 23.8 / 14.6 / 11.9 | 8.7 / 5.0 / 5.5 | 23.7 / 8.5 / 7.5 | 17 / 19 / 32 |

\* 2 of the 3 runs of this cell had an executor restart (see "Issues"); the run without one took **88.7 s** (S2) and **83.9 s** (S3), its clean step 19.9 s and 18.1 s.

Start-up is 6–8.5 s in every cell. Shuffle written: L1 1,677 MB on I1 (336 MB on I2, 271 MB on I3), L2 and L3 382 / 92 / 78 MB, the same on every core count. The baseline's driver heap peaks at 0.5–1.4 GB of its 2 GB (the driver reader holds every check-in); the default's stays at 130–200 MB.

### Baseline vs default

| Input | 1 core (S1) | 2 cores (S2) | 4 cores (S3) |
|---|---|---|---|
| I1 all (2.76 M check-ins) | **3.2×** (531.9 → 164.4 s) | **2.6×** (276.2 → 105.7 s); **3.1×** against the run without executor restart (88.7 s) | **2.7×** (258.8 → 94.7 s); **3.1×** against 83.9 s |
| I2 Tokyo (165 k) | 2.4× | 2.3× | 2.2× |
| I3 Q1865 (28 k) | 2.5× | 2.2× | 2.0× |

### Effect of the worker's cores (run time relative to 2 cores)

| Live | 1 core (S1) | 4 cores (S3) |
|---|---|---|
| L1 baseline | 1.78–1.93× | 0.84–0.94× |
| L2 default | 1.56–1.76× (I1 without executor restarts: 1.85×) | 0.90–0.93× (I1 without restarts: 0.95×) |
| L3 read 4 | 1.77–1.85× | 0.95–1.03× |

### Observations (S1–S3)

- **The default is 2–3.2× faster than the baseline, and the gap grows with the data.** On all destinations the baseline needs 4.4 min on 2 cores, the default 1.8 min. The baseline loses most in **write** (145 s on I1, 2 cores): without cache, the write is where Spark computes the whole plan, Python UDF included, and every row then goes through the driver with `toPandas`. In **match** the Python UDF costs 49.5 s against native `rlike`'s 34.7 s.
- **1 → 2 cores pays off (1.6–1.9× faster); 2 → 4 cores hardly does (−3% to +16%).** On 4 cores the executor still has 512 MB, and the driver and ClickHouse (1 CPU) share the laptop; Spark CPU grows (I1-L1 172 → 305 s) for the same work.
- **4 read slices (L3) do not read faster, but on all destinations they avoided the executor restarts.** L3 took 88.4 s on 2 cores and 85.1 s on 4, the same as L2's runs without a restart (88.7 s and 83.9 s); L2's medians (105.7 s and 94.7 s) are higher because 4 of its 6 runs on 2–4 cores lost an executor to `OutOfMemoryError` and redid part of the clean step. No L3 run did (likely because each task holds a smaller slice). On Tokyo and Q1865 the read is too small to gain (±2 s), and no run there lost an executor.
- **The default on Q1865 still takes 53–58 s on 2–4 cores** for 28 k check-ins: the job reads the 687 k places and runs five Spark steps however small the region is. This is the fixed cost the ClickHouse engine doesn't pay (below).

## Block V: single options (2 cores)

| Option | I2 Tokyo: run time (vs L2 72.2 s) | I1 all: match step (vs L2 35–41 s) |
|---|---|---|
| V1 Python UDF | 84.3 s (**+17%**) | 57.9 s (+17–23 s) |
| V2 pandas UDF | 87.4 s (**+21%**) | 46.9 s (+6–12 s) |
| V3 no cache | 64.9 s (**−10%**) | run time 176.3 s (**+43%** vs L2's 123.6 s, whose 3 runs all had an executor restart; **+70%** vs the ~104 s of V4–V6 runs without one) |
| V4 no broadcast | 72.8 s (+1%) | within the noise (see below) |
| V5 no AQE | 71.3 s (−1%) | within the noise |
| V6 pandas writer | 72.9 s (+1%) | within the noise |

- **Native `rlike` beats both UDFs.** The attraction test is a regex over each place's categories: a Python UDF pays a Python round trip per row; a pandas UDF batches the rows but still serializes them to Python with Arrow, and on Tokyo it is even slower than the plain UDF (the batches are small).
- **Cache depends on the size:** on all destinations it saves 53 s (the job caches the check-ins and places it reads and the output of each step; without cache every later step reads ClickHouse and recomputes the steps before it: 48 s of ClickHouse query time instead of 13 s, 1.3 GB shuffle instead of 0.4 GB). On Tokyo the cache costs more than the second read (−7 s without it).
- **Broadcast, AQE and the pandas writer don't matter at this size:** the result is only 108 k visits and 10.6 k places, so the writer is never the bottleneck, and the default already forces a merge join for the big places table.
- **The I1 run times of block V are bimodal because of executor restarts, so the run-time medians for V4–V6 on I1 are not used.** In 8 of the 21 I1 runs of block V, a task of the clean step ran the 512 MB executor out of heap (`java.lang.OutOfMemoryError: Java heap space`, executor exit code 52); Spark started a new executor and ran the task again, and the run still gave the right result. Those runs take 29–39 s in clean instead of 22–25 s, and 123–151 s in all (V4–V6 without a restart: 103–115 s). All three L2 runs of block V had a restart, most V4–V6 runs none; this makes V4 (104.4 s), V5 (115.3 s) and V6 (106.3 s) look faster than L2 (123.6 s). In the third round, where all four had one, they took 128–131 s against L2's 123.6 s. The match step comes after the restart and is used for V1–V2 instead. The restarted runs are listed in [benchmark/executor-restarts.csv](benchmark/executor-restarts.csv).

## ClickHouse blocks (H1–H3)

Run time median, seconds, as H1 / H2 / H3 (1 / 2 / 4 ClickHouse CPUs). Peak threads from `system.query_log`.

| Input | HL1 ClickHouse SQL | HL2 memory limit off | Peak threads HL1 / HL2 (H3) |
|---|---|---|---|
| I1 all | 22.8 / 19.0 / 19.5 | 21.6 / **12.8** / **9.7** | 5 / 6 |
| I2 Tokyo | 3.3 / 2.8 / 2.6 | 2.7 / 2.0 / 1.9 | 5 / 6 |
| I3 Q1865 | 2.6 / 2.2 / 2.1 | 2.0 / 1.8 / 1.7 | 2 / 6 |

- **ClickHouse SQL is 3.9–30× faster than Spark** for the same rows: on all destinations 22.8 s (1 CPU) against Spark's 88.7–105.7 s (default, 2 cores, without and with executor restarts), and 9.7 s (4 CPUs, HL2) against Spark's best 85.1 s (L3, 4 cores). On Tokyo 1.9–3.3 s against 58–62 s. There is no start-up and no data movement: the data never leaves ClickHouse.
- **In the 1 GB container, ClickHouse's memory-based thread limit stops it from using its CPUs.** With the limit on (HL1), 2 and 4 CPUs bring all destinations from 22.8 s only to 19.0–19.5 s. With it off (HL2), 4 CPUs bring it to 9.7 s (2.2× faster than 1 CPU), with a peak of 431 MB memory, still within the container.
- On the small inputs ClickHouse is already at about 2 s, of which 1.1–1.2 s is the job's own start-up (Python, client).

## Noise

- **S1:** the two runs of a cell differed by 1.2–12.9%, the largest I2-L2 (117.0 and 102.8 s).
- **S2 and S3:** 0.3–8%, except I1-L2 (S2: 88.7, 105.7 and 118.5 s; S3: 83.9, 99.2 and 94.7 s), where the slower runs had an executor restart and the cell got a third run.
- **V:** the three runs of a cell varied by 1–15% on Tokyo; on all destinations by 15–27% for V1, V2, V4, V5 and V6, each with one run that had an executor restart (and V5 with a second slow run, 115.3 s, without one).
- **H1–H3:** the five runs varied by 0.1–4.4 s. The small inputs (about 2 s a run) had single slow runs of 2.7–2.9 s, up to 38% above the others; all destinations varied by 10–21%. Differences under about 0.5 s on the small inputs, or 15% on all destinations, are noise.

## Behaviour verified

| # | Check | Result |
|---|---|---|
| 1 | Each run used its cell's settings and size | Every measured run's snapshot: the cell's preset, the engine, the block's worker cores (S, V) or ClickHouse CPUs with `max_threads` `auto(1)` / `auto(2)` / `auto(4)` (H) |
| 2 | Inputs read what they should | Every run read 2,764,448 (I1), 164,969 (I2) or 27,544 (I3) check-ins rows |
| 3 | Same input, same result | Every run's fingerprint equals its input's expected one and that of its reference run (`make br8-bench-check`) |
| 4 | All succeeded | 188 of 188 measured runs and the 7 warm-ups (the 4 runs of the void S1 attempt not counted) |
| 5 | ClickHouse live settings | HL2: every query carries `max_threads_min_free_memory_per_thread` = 0; HL1: none. No query used the query cache |
| 6 | Spark logs | Every run's log scanned for `Lost executor` and `OutOfMemoryError`: 12 runs had one (see "Issues") |
| 7 | One run at a time | No two BR7 or BR8 runs overlapped; no container restarted or was killed for memory during the benchmark |
| 8 | Stack restored | After H3: Spark worker at 2 cores, ClickHouse at 1 CPU (`auto(1)`) |
| 9 | Figures in this file | Every run-time median above was checked against `br8.pipeline_runs` |
| 10 | Screenshots | 59 `run.jpg` (every cell) and 41 `spark.jpg` (every Spark cell), from each cell's first measured run; samples checked by eye: the run's settings, the compared run, the steps as Spark jobs |

## Issues

- **The first attempt of block S1 is void: the laptop went to sleep.** Its fourth run (I1-L3, run 1) started at 01:11; Spark ran no job between 01:12 and 05:32, so the run's time included 4 h 20 min of sleep. The run was stopped (`failed`), the 4 S1 runs of that attempt were removed from `runs.csv` and re-tagged `bench_block=S1-void-sleep` in `br8.pipeline_runs` (so no panel counts them), and S1 was run again from the start. The rest of the benchmark ran with Windows kept awake (`SetThreadExecutionState`).
- **The laptop was ~13% slower after the sleep than before:** the void S1 runs took 464 s (I1-L1) and 142 s (I1-L2), the new ones 527–537 s and 163–166 s; block V's L2 on I1 (123.6 s) is slower than S2's (105.7 s) with the same settings. Comparisons are made within a block, whose cells are interleaved; comparisons across blocks (the core tables) carry this drift.
- **12 succeeded runs had an executor restart** (found after the benchmark, in the Spark logs): all on all destinations (I1), with the default cache and 2–4 cores: S2 and S3 L2 (2 runs each; the restarts also caused these cells' third runs), and 8 runs of block V. A task of the clean step ran the 512 MB executor out of heap, Spark replaced the executor and ran the task again, so the result is right but the run time includes the restart and the redone work (+11–30 s). No run on 1 core (S1), of the baseline (no cache), of L3, or on Tokyo and Q1865 had one. The medians above include these runs, as measured; where it changes a conclusion the figure without them is given too. List: [benchmark/executor-restarts.csv](benchmark/executor-restarts.csv).

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `S3-I1-L2/` or `H1-I1-HL1/`), from the cell's first measured run, stored with Git LFS (`make br8-bench-screens BLOCK=…`, Playwright with Chrome):

| File | Shows |
|---|---|
| `run.jpg` | Grafana `br8-benchmark-run` for the run against its reference run: status, run time, correct result, seconds per step, settings that differ, Spark and ClickHouse figures, rows excluded, the result, CPU and memory per container during the run |
| `spark.jpg` | Spark cells: the application's Jobs page in Spark history, jobs named after the steps |

References: L1 and L3 against L2 of the same input and block; L2 of S1 and S3 against S2's L2; in block V every option against V's L2. HL2 against HL1 of the same input and block; HL1 of H2 and H3 against H1's HL1; H1's HL1 against S2's L2 (ClickHouse against Spark).

## Appendix: every run (seconds, in order)

Measured runs in the order they ran; \* marks a run with an executor restart.

| Cell | S1 (1 core) | S2 (2 cores) | S3 (4 cores) |
|---|---|---|---|
| I1-L1 | 526.5, 537.3 | 278.8, 273.6 | 260.9, 256.7 |
| I1-L2 | 163.4, 165.5 | 88.7, 105.7\*, 118.5\* | 83.9, 99.2\*, 94.7\* |
| I1-L3 | 169.5, 158.2 | 87.9, 88.9 | 86.9, 83.2 |
| I2-L1 | 252.2, 268.3 | 145.4, 146.4 | 127.6, 124.8 |
| I2-L2 | 117.0, 102.8 | 64.9, 60.1 | 58.7, 57.3 |
| I2-L3 | 110.4, 113.3 | 58.2, 62.9 | 59.3, 57.4 |
| I3-L1 | 240.0, 229.5 | 119.4, 129.8 | 106.0, 103.9 |
| I3-L2 | 98.1, 91.1 | 57.2, 57.7 | 52.6, 52.8 |
| I3-L3 | 101.8, 99.6 | 55.6, 58.0 | 54.2, 53.8 |

| Cell | V (2 cores) |
|---|---|
| I1-L2 | 123.1\*, 124.3\*, 123.6\* |
| I1-V1 | 151.0\*, 131.2, 132.7 |
| I1-V2 | 142.4\*, 119.8, 125.4 |
| I1-V3 | 175.0, 176.3, 183.8 |
| I1-V4 | 102.9, 104.4, 131.1\* |
| I1-V5 | 102.8, 115.3, 128.0\* |
| I1-V6 | 102.6, 106.3, 129.7\* |
| I2-L2 | 67.9, 72.2, 76.6 |
| I2-V1 | 83.5, 84.3, 88.0 |
| I2-V2 | 87.4, 85.9, 88.3 |
| I2-V3 | 64.9, 64.5, 67.7 |
| I2-V4 | 72.8, 70.1, 77.5 |
| I2-V5 | 71.3, 69.2, 73.2 |
| I2-V6 | 70.1, 72.9, 75.9 |

| Cell | H1 (1 CPU) | H2 (2 CPUs) | H3 (4 CPUs) |
|---|---|---|---|
| I1-HL1 | 22.4, 21.4, 25.7, 23.2, 22.8 | 18.5, 21.2, 19.0, 18.4, 19.6 | 21.7, 19.4, 19.4, 19.5, 20.5 |
| I1-HL2 | 21.6, 20.8, 21.3, 21.6, 21.6 | 12.7, 13.9, 13.4, 12.8, 12.5 | 10.9, 8.92, 9.98, 9.72, 9.67 |
| I2-HL1 | 3.32, 3.08, 3.33, 3.27, 3.41 | 2.89, 2.78, 2.86, 2.48, 2.63 | 2.88, 2.64, 2.56, 2.60, 2.91 |
| I2-HL2 | 2.71, 2.85, 2.31, 2.85, 2.69 | 2.04, 2.15, 2.45, 1.97, 1.96 | 1.94, 1.75, 1.98, 1.89, 1.84 |
| I3-HL1 | 2.85, 2.17, 2.59, 2.56, 2.11 | 2.19, 2.23, 2.34, 1.96, 1.97 | 2.01, 2.09, 2.06, 1.97, 2.10 |
| I3-HL2 | 2.66, 1.88, 2.05, 2.02, 2.00 | 1.87, 1.75, 1.74, 1.78, 1.78 | 1.68, 1.57, 1.73, 1.71, 1.63 |
