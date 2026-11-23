# BR7 Result: Benchmark

Measured results of [plan-benchmark.md](../../plan/br7/plan-benchmark.md) (BR7 part, section 13), with the same matrix as BR8 ([result-benchmark.md](../br8/result-benchmark.md)).

## Progress

Times are the machine's clock (UTC; Vietnam time is UTC+7).

| Block | Configuration | Status | Runs |
|---|---|---|---|
| S2 | Spark worker at 2 cores | **Done** (2026-11-08 13:16–14:30) | 1 warm-up + 18 measured (2 per cell), all succeeded |
| S1 | Spark worker at 1 core | **Done** (14:30–16:13) | 1 warm-up + 18 measured, all succeeded |
| S3 | Spark worker at 4 cores | **Done** (2026-11-09 01:52–03:03, after a pause, see "Issues") | 1 warm-up + 18 measured, all succeeded |
| V | Single options, 2 cores, I1 and I2 | **Done** (03:03–05:44) | 1 warm-up + 42 measured (3 per cell). One run failed (executor out of memory) and was run again, see "Issues" |
| H1 | ClickHouse engine, ClickHouse at 1 CPU | **Done** (05:44–05:59) | 1 warm-up + 30 measured (5 per cell), all succeeded |
| H2 | ClickHouse at 2 CPUs | **Done** (05:59–06:10) | 1 warm-up + 30 measured, all succeeded |
| H3 | ClickHouse at 4 CPUs | **Done** (06:10–06:20) | 1 warm-up + 30 measured, all succeeded |

`make br7-bench-check` passed for every block: 186 measured runs, 0 with problems. Each run had the block's settings, worker cores and ClickHouse CPUs, the expected trips read, and the **expected result fingerprint**: the rows of `prepare.py` (see the table of inputs). No cell needed a third run (every pair was within 15%). Every run is listed in [benchmark/runs.csv](benchmark/runs.csv); the failed run is in [benchmark/runs-failed.csv](benchmark/runs-failed.csv).

## Setup

- **Stack:** dev laptop, WSL with 7.4 GB RAM. ClickHouse 1 GB, with the block's CPUs. Spark: 1 worker with 2 GB and the block's cores, one executor of 512 MB, and the driver in `spark-local` (512 MB, 2 GB for the baseline).
- **Data:** the BR7 import (`make br7-import`): 13 months of yellow taxi trips, 2025-09 to 2026-09, 50,745,563 trips, and the 265-zone lookup.
- **Inputs:** the first training month. Training runs to 2026-08 and the replay month is 2026-09 for every input, as in `prepare_br7.py`.

  | Input | Months | Trips read | Quality ok | Tourist-zone trips used | Duplicates | Baseline cells | Replay events |
  |---|---|---|---|---|---|---|---|
  | I1 | 12 + 1 (from 2025-09) | 50,745,563 | 47,955,300 | 6,588,726 | 7,608 | 1,680 | 495,883 |
  | I2 | 6 + 1 (from 2026-03) | 26,454,644 | 25,166,115 | 3,456,638 | 7,608 | 1,680 | 495,883 |
  | I3 | 2 + 1 (from 2026-07) | 10,742,869 | 10,173,441 | 1,421,460 | 7,608 | 1,680 | 495,883 |

  Fingerprints (count:`groupBitXor(cityHash64(...))` per result table) are in `tests/br7/benchmark.py` (`EXPECTED`) and in the `br7.run_history` view. I1's are those of `prepare.py`'s own output (`data/br7/prepared/`): its `summary.json`, the 1,680 baseline rows and the 495,883 replay rows were compared row by row before the benchmark. The replay and zone parts are the same for every input, since the replay month is the same.

- **Live settings:**

  | Live | Preset | Change from the default |
  |---|---|---|
  | L1 | `bench-baseline` | Single reader (1 slice), Python UDF for the quality label, no cache, pandas writer (`toPandas` → `insert_df`), AQE off, no broadcast joins, driver 2 GB |
  | L2 | `bench-default` | Nothing: parallel reader (2 slices), native quality expression, cache of the deduplicated trips, partition writer, AQE on |
  | L3 | `bench-read-4` | Parallel reader with 4 slices |
  | V1–V6 | `bench-python-udf`, `-pandas-udf`, `-no-cache`, `-no-broadcast`, `-no-aqe`, `-pandas-writer` | One option of the default each |
  | HL1 | `bench-clickhouse` | The same steps as ClickHouse SQL (`br7_bench_sql.py`) |
  | HL2 | `bench-clickhouse-nomemlimit` | HL1, with ClickHouse's memory-based thread limit off |

  BR7 has no driver reader (BR8's L1 has one): the baseline reads with one streamed slice instead, because 50 M trips do not fit in a driver.

- **How figures are given:** the median of a cell's measured runs (no warm-up), each figure separately. Steps are the job's own timings: **quality** (read the trips, label each one, count per month and label), **clean** (tourist zones, quality ok, UTC time, deduplicate, cache), **baseline** (zone × hour-of-week grid, mean and p90), **replay** (the replay month's events) and **write**. "Start-up" is the run time outside the steps.

## Spark blocks (S1–S3)

### Run time and steps (median, seconds)

Given as S1 / S2 / S3 (1 / 2 / 4 worker cores).

| Input | Cell | Run time | quality | clean | baseline | replay | write | Spark CPU |
|---|---|---|---|---|---|---|---|---|
| I1 12+1 | L1 baseline | 677.6 / 449.2 / 415.7 | 221.1 / 135.4 / 115.2 | 115.3 / 80.2 / 77.7 | 93.9 / 60.3 / 58.4 | 68.6 / 44.5 / 43.4 | 171.0 / 120.5 / 112.9 | 307 / 342 / 374 |
| | L2 default | 274.6 / 223.5 / 200.5 | 52.2 / 49.3 / 45.3 | 152.8 / 131.6 / 122.6 | 28.1 / 18.8 / 14.7 | 10.2 / 4.5 / 3.1 | 23.4 / 10.7 / 6.7 | 120 / 146 / 169 |
| | L3 read 4 | 302.9 / 259.1 / 291.1 | 59.1 / 62.9 / 78.8 | 176.5 / 157.4 / 178.6 | 27.9 / 17.8 / 15.1 | 7.5 / 4.1 / 3.2 | 23.7 / 9.8 / 7.8 | 120 / 148 / 199 |
| I2 6+1 | L1 | 425.3 / 274.7 / 246.4 | 120.5 / 73.9 / 63.2 | 76.9 / 50.9 / 43.8 | 68.0 / 40.9 / 36.1 | 40.6 / 25.6 / 23.8 | 112.5 / 75.3 / 70.9 | 180 / 197 / 222 |
| | L2 | 198.2 / 149.5 / 134.7 | 35.1 / 30.5 / 30.5 | 97.3 / 80.0 / 71.1 | 36.3 / 18.5 / 13.7 | 9.2 / 4.2 / 3.1 | 13.0 / 9.5 / 7.6 | 71 / 90 / 105 |
| | L3 | 214.9 / 166.0 / 175.2 | 43.0 / 37.5 / 44.7 | 108.2 / 90.9 / 98.7 | 37.2 / 16.8 / 13.6 | 6.4 / 3.9 / 2.8 | 13.5 / 9.0 / 6.7 | 74 / 90 / 118 |
| I3 2+1 | L1 | 245.7 / 165.2 / 153.0 | 64.1 / 42.5 / 41.7 | 43.0 / 29.4 / 23.7 | 40.6 / 26.4 / 21.9 | 20.8 / 13.2 / 12.6 | 70.0 / 46.3 / 44.8 | 90 / 101 / 124 |
| | L2 | 134.9 / 96.0 / 85.1 | 25.2 / 17.4 / 17.0 | 58.5 / 41.4 / 36.0 | 25.6 / 17.1 / 14.0 | 4.9 / 3.7 / 3.3 | 12.3 / 8.4 / 7.2 | 40 / 50 / 63 |
| | L3 | 144.4 / 104.2 / 102.9 | 25.4 / 22.0 / 25.1 | 60.1 / 46.3 / 45.6 | 28.9 / 16.9 / 14.1 | 9.7 / 3.6 / 3.4 | 13.3 / 8.2 / 7.0 | 39 / 50 / 70 |

Start-up is 6.7–8.8 s in every cell. Shuffle written: L1 1,198 MB on I1 (630 MB on I2, 260 MB on I3), L2 and L3 480 / 272 / 137 MB, the same on every core count. The executor's heap peaks at 290–460 MB of its 512 MB in every cell. The baseline's driver heap peaks at 320–800 MB of its 2 GB (`toPandas` of the 495,883 replay rows); the default's stays at 140–210 MB.

### Baseline vs default

| Input | 1 core (S1) | 2 cores (S2) | 4 cores (S3) |
|---|---|---|---|
| I1 12+1 (50.7 M trips) | **2.5×** (677.6 → 274.6 s) | **2.0×** (449.2 → 223.5 s) | **2.1×** (415.7 → 200.5 s) |
| I2 6+1 (26.5 M) | 2.1× | 1.8× | 1.8× |
| I3 2+1 (10.7 M) | 1.8× | 1.7× | 1.8× |

### Effect of the worker's cores (run time relative to 2 cores)

| Live | 1 core (S1) | 4 cores (S3) |
|---|---|---|
| L1 baseline | 1.49–1.55× | 0.90–0.93× |
| L2 default | 1.23–1.41× | 0.89–0.90× |
| L3 read 4 | 1.17–1.39× | 0.99–1.12× |

### Observations (S1–S3)

- **The default is 1.7–2.5× faster than the baseline, and the gap grows with the data.** On 12 + 1 months the baseline needs 7.5 min on 2 cores, the default 3.7 min. The baseline loses in **quality** (135 s against 49 s on I1, 2 cores: the Python UDF labels all 50.7 M trips, against a native expression), in **write** (120 s against 11 s: without cache the write recomputes the plan, and the replay rows go through the driver with `toPandas`) and in **baseline** and **replay**, which also recompute the deduplicated trips without the cache.
- **The default spends most of its time in clean (122–153 s on I1)**: it reads the trips from ClickHouse a second time (only the deduplicated trips are cached, not the raw read), filters them to the tourist zones, and deduplicates 6.6 M trips with a shuffle. The steps after it (baseline, replay, write) read the cache and take 25–62 s together.
- **1 → 2 cores pays off (1.2–1.6× faster); 2 → 4 cores hardly does (7–11% for L1 and L2).** The executor keeps 512 MB on 4 cores, and ClickHouse (1 CPU) serves every read: the read is in quality and clean, and those two steps gain least (I1-L2 quality 49.3 → 45.3 s).
- **4 read slices (L3) are slower than 2 (L2) everywhere: +7–16% on 1–2 cores, +21–45% on 4 cores** (I1: 291.1 s against 200.5 s). ClickHouse has 1 CPU in the S blocks: four parallel streams share it, so the read gets no faster, and their ClickHouse query time grows (I1 on 4 cores: 803 s summed over the streams against 226 s for L2). Unlike BR8's small read, BR7 reads 50 M rows of 13 months, so the read is bound by ClickHouse, not by Spark.
- **The default on 2 + 1 months still takes 85–135 s** for 10.7 M trips. The ClickHouse engine does it in 3–9 s (below).

## Block V: single options (2 cores)

| Option | I1 12+1: run time (vs L2 214.9 s) | I2 6+1: run time (vs L2 129.2 s) | Step that changes (I1) |
|---|---|---|---|
| V1 Python UDF | 375.0 s (**+75%**; +58% without the run with an executor restart, see "Issues") | 185.4 s (**+43%**) | quality 133.4 s (vs 52.0) and clean 172.5 s without that run (vs 123.6) |
| V2 pandas UDF | 244.6 s (**+14%**) | 155.7 s (**+21%**) | quality 75.0 s |
| V3 no cache | 318.0 s (**+48%**) | 185.0 s (**+43%**) | clean 76.8 s, but baseline 48.0, replay 42.7, write 94.0 s; shuffle 1,127 MB (vs 480) |
| V4 no broadcast | 199.8 s (−7%) | 138.2 s (+7%) | none (within the noise, see below) |
| V5 no AQE | 208.4 s (−3%) | 150.9 s (**+17%**) | baseline 24.5 s (vs 16.6) |
| V6 pandas writer | 205.6 s (−4%) | 140.6 s (+9%) | write 16.1 s (vs 11.3) |

- **The quality label is where UDFs cost.** The label is a chain of 6 tests on every trip read (50.7 M on I1): a Python UDF pays a Python round trip per row (+81 s in quality, and clean recomputes the label too, +49 s), a pandas UDF batches the rows with Arrow and costs +23 s. The native expression stays in the JVM. The baseline (L1) carries this cost on top of the others.
- **Cache saves 43–48%:** with it, baseline, replay and write read the 6.6 M cached trips; without it, each of them reads ClickHouse again and recomputes quality, filter and deduplication (2.2 GB spilled to disk on I1 against 0.3 GB).
- **AQE helps the baseline step** (24.5 s without it against 16.6 s on I1, 24.7 against 15.0 s on I2), most likely by coalescing the shuffle partitions of the small zone × hour grid. On I2 the run is 17% slower without it.
- **The pandas writer costs 5–9 s more in write** (495,883 replay rows through the driver); small, because the result is small.
- **Broadcast joins don't matter here:** the joins are with the 265-zone lookup and the zone × hour grid, both small. V4–V6 on I1 measured 3–7% faster than L2; L2's runs spread from 210.6 to 235.7 s, so this is not counted as an effect.

## ClickHouse blocks (H1–H3)

Run time median, seconds, as H1 / H2 / H3 (1 / 2 / 4 ClickHouse CPUs). Peak threads from `system.query_log`.

| Input | HL1 ClickHouse SQL | HL2 memory limit off | Peak threads HL1 / HL2 (H3) | Peak memory (H3, I1) |
|---|---|---|---|---|
| I1 12+1 | 35.9 / 32.0 / 29.3 | 37.3 / **20.3** / **12.7** | 2 / 7 | 332 / 338 MB |
| I2 6+1 | 20.3 / 15.6 / 15.1 | 19.6 / 9.2 / 6.5 | 2 / 6 | |
| I3 2+1 | 9.1 / 7.2 / 7.1 | 7.8 / 4.7 / 3.4 | 2 / 6 | |

- **ClickHouse SQL is 6–25× faster than Spark** for the same rows: on 12 + 1 months 35.9 s (1 CPU) against Spark's 223.5 s (default, 2 cores), and 12.7 s (4 CPUs, HL2) against Spark's best 200.5 s (L2, 4 cores, 15.8×). On 2 + 1 months 3.4–9.1 s against 85–96 s. The trips never leave ClickHouse; Spark has to stream 50.7 M rows out of it twice.
- **ClickHouse's memory-based thread limit keeps it at 2 threads in the 1 GB container.** With the limit on (HL1), 2 and 4 CPUs bring 12 + 1 months from 35.9 s only to 32.0–29.3 s. With it off (HL2), 4 CPUs bring it to 12.7 s (2.9× faster than 1 CPU) with 5–7 threads, at a peak of 338 MB, still within the container.
- On 1 CPU the limit makes no difference (HL1 and HL2 within 2–15%).

## Noise

- **S1–S3:** the two runs of a cell differed by 0.1–12.5% (S1), 0.8–5.6% (S2) and 1.0–11.7% (S3), so no cell needed a third run. The largest: S1 I2-L2 (185.8 and 210.6 s), S3 I3-L1 (144.1 and 162.0 s) and S3 I1-L3 (274.6 and 307.6 s). Differences under about 10% between Spark cells are noise.
- **V:** the three runs of a cell varied by 2–18%, except I1-V1 (375.0, 463.0 and 304.4 s; the 463.0 s run had an executor restart), I2-V6 (140.6, 177.2 and 140.6 s; every step of the second run was about 1.3× slower, without restart) and I2-V2 (186.4, 151.4 and 155.7 s).
- **H1–H3:** the five runs of a cell varied by 9–30%, with single slow runs: H1 I2-HL1 44.2 s where the others took 19.0–21.8 s (every step 2.2× slower, so the laptop rather than the queries), and H3 I1-HL2 14.6 and 16.8 s against 12.1–12.7 s. On 1 CPU, ClickHouse feels anything else running on the laptop. Differences under about 15% between ClickHouse cells are noise.

## Behaviour verified

| # | Check | Result |
|---|---|---|
| 1 | Each run used its cell's settings and size | Every measured run's snapshot: the cell's preset, the engine, the block's worker cores (S, V) or ClickHouse CPUs with `max_threads` `auto(1)` / `auto(2)` / `auto(4)` (H) |
| 2 | Inputs read what they should | Every run read 50,745,563 (I1), 26,454,644 (I2) or 10,742,869 (I3) trips rows |
| 3 | Same input, same result | Every run's fingerprint equals its input's expected one and that of its reference run (`make br7-bench-check`); before the benchmark, I1's result was compared row by row with `prepare.py`'s own output |
| 4 | All succeeded | 186 of 187 measured runs and the 7 warm-ups; the failed one (I2-V1, executor out of memory) was run again |
| 5 | ClickHouse live settings | HL2: every query carries `max_threads_min_free_memory_per_thread` = 0; HL1: none. No query used the query cache |
| 6 | Spark logs | Every run's log scanned for `Lost executor` and `OutOfMemoryError`: 1 run had one (see "Issues") |
| 7 | One run at a time | No two BR7 or BR8 runs overlapped; no container restarted or was killed for memory during the benchmark |
| 8 | Stack restored | After H3: Spark worker at 2 cores, ClickHouse at 1 CPU (`auto(1)`) |
| 9 | Figures in this file | Every run-time median above was checked against `br7.pipeline_runs` |
| 10 | Screenshots | 59 `run.jpg` (every cell) and 41 `spark.jpg` (every Spark cell), from each cell's first measured run; samples checked by eye: the run's settings, the compared run, the steps as Spark jobs |

## Issues

- **One run failed with an executor out of memory, and was run again.** Block V, cell I2-V1 (Python UDF, 6 + 1 months), repeat 1 (run `fb165e39`): in clean, the deduplication's sort merged its spill files and ran out of the executor's 512 MB heap (`java.lang.OutOfMemoryError: Java heap space`, executor exit code 52), four times, so the job failed. Repeats 2 and 3 of the same cell succeeded (179.8 s and 185.5 s), as did the same option on twice the data (I1-V1), so it is an occasional failure close to the heap limit, not one the option always causes; every cell's executor peaks at 290–460 MB of 512 MB. The cell was run again after the block (run `74e55d0d`, 185.4 s, correct result); the failed row moved from `runs.csv` to `runs-failed.csv` and stays `failed` in `br7.pipeline_runs`.
- **One succeeded run had an executor restart** (found after the benchmark, in the Spark logs): block V, I1-V1, repeat 2 (run `05b5cfa2`, 463.0 s). In clean, the same out-of-memory as the failed I2-V1 run hit two executors; Spark replaced them and the run still gave the right result, but its clean step took 281 s instead of 149–196 s. It is the slowest of the cell's three runs, so the median (375.0 s) is a run without restart; without it the median is 339.7 s. No other BR7 run had one: all Spark logs of the 186 runs were checked. List: [benchmark/executor-restarts.csv](benchmark/executor-restarts.csv).
- **The benchmark was paused for 9.5 hours between S1 and S3** (2026-11-08 16:13 to 2026-11-09 01:52) because the laptop was running hot. The chain stopped at a block boundary, after S1's check, so no run was cut. Drift across the pause is small: L2 on I1 took 223.5 s in S2 (before) and 214.9 s in block V (after), −4%. Comparisons are made within a block, whose cells are interleaved; the core tables compare across blocks and carry this drift.
- **`prepare.py`'s deduplication is not deterministic for `passenger_count`.** `dropDuplicates` on the trip key keeps any one of the duplicate rows, and in 7,608 groups the duplicates differ in `passenger_count`, so two runs of `prepare.py` (or of the benchmark) can write different values for it in up to 7,608 replay rows: the default run differed from `prepare.py`'s CSV in 1,519 rows, the baseline in 929. `passenger_count` is left out of the replay fingerprint for this reason; every other column matched row by row. `prepare.py` was not changed.

## Screenshots

In [benchmark/](benchmark/), one folder per cell (for example `S3-I1-L2/` or `H1-I1-HL1/`), from the cell's first measured run, stored with Git LFS (`make br7-bench-screens BLOCK=…`, Playwright with Chrome):

| File | Shows |
|---|---|
| `run.jpg` | Grafana `br7-benchmark-run` for the run against its reference run: status, run time, correct result, seconds per step, settings that differ, Spark and ClickHouse figures, rows excluded, the result, CPU and memory per container during the run |
| `spark.jpg` | Spark cells: the application's Jobs page in Spark history, jobs named after the steps |

References: L1 and L3 against L2 of the same input and block; L2 of S1 and S3 against S2's L2; in block V every option against V's L2. HL2 against HL1 of the same input and block; HL1 of H2 and H3 against H1's HL1; H1's HL1 against S2's L2 (ClickHouse against Spark).

## Appendix: every run (seconds, in order)

Measured runs in the order they ran; \* marks a run with an executor restart.

| Cell | S1 (1 core) | S2 (2 cores) | S3 (4 cores) |
|---|---|---|---|
| I1-L1 | 663.5, 691.6 | 459.7, 438.8 | 397.0, 434.3 |
| I1-L2 | 285.4, 263.7 | 228.1, 219.0 | 192.0, 209.1 |
| I1-L3 | 313.2, 292.5 | 254.8, 263.4 | 274.6, 307.6 |
| I2-L1 | 448.7, 402.0 | 271.5, 278.0 | 250.1, 242.7 |
| I2-L2 | 185.8, 210.6 | 148.9, 150.2 | 134.1, 135.4 |
| I2-L3 | 214.7, 215.0 | 164.2, 167.8 | 182.6, 167.8 |
| I3-L1 | 256.2, 235.1 | 166.2, 164.2 | 144.1, 162.0 |
| I3-L2 | 139.7, 130.2 | 93.9, 98.1 | 89.2, 81.0 |
| I3-L3 | 153.3, 135.6 | 101.3, 107.2 | 108.1, 97.8 |

| Cell | V (2 cores) |
|---|---|
| I1-L2 | 235.7, 210.6, 214.9 |
| I1-V1 | 375.0, 463.0\*, 304.4 |
| I1-V2 | 244.6, 246.5, 232.9 |
| I1-V3 | 354.8, 318.0, 297.3 |
| I1-V4 | 212.2, 199.8, 199.7 |
| I1-V5 | 234.4, 202.6, 208.4 |
| I1-V6 | 216.8, 205.6, 198.5 |
| I2-L2 | 134.0, 126.2, 129.2 |
| I2-V1 | 179.8, 185.5, 185.4 |
| I2-V2 | 186.4, 151.4, 155.7 |
| I2-V3 | 203.5, 172.1, 185.0 |
| I2-V4 | 148.3, 138.2, 134.3 |
| I2-V5 | 152.6, 150.9, 149.2 |
| I2-V6 | 140.6, 177.2, 140.6 |

| Cell | H1 (1 CPU) | H2 (2 CPUs) | H3 (4 CPUs) |
|---|---|---|---|
| I1-HL1 | 42.6, 35.9, 38.9, 34.5, 34.0 | 34.0, 32.1, 30.5, 30.7, 32.0 | 29.3, 29.2, 30.6, 28.8, 32.4 |
| I1-HL2 | 40.2, 37.3, 37.4, 34.4, 33.7 | 19.3, 21.7, 19.5, 20.3, 21.2 | 12.4, 12.1, 14.6, 12.7, 16.8 |
| I2-HL1 | 21.8, 20.3, 44.2, 19.0, 19.6 | 15.8, 14.7, 14.6, 15.6, 16.3 | 14.7, 14.9, 15.2, 15.1, 16.9 |
| I2-HL2 | 22.2, 21.3, 19.6, 18.5, 18.3 | 9.45, 9.06, 9.21, 8.82, 9.86 | 6.47, 6.94, 6.45, 6.07, 6.51 |
| I3-HL1 | 10.1, 9.14, 9.14, 7.48, 7.35 | 6.79, 7.20, 7.18, 7.31, 6.62 | 7.37, 7.69, 7.11, 6.90, 7.08 |
| I3-HL2 | 9.43, 7.82, 7.68, 7.33, 8.60 | 5.03, 4.67, 4.69, 4.48, 5.02 | 3.25, 3.49, 3.37, 3.56, 3.32 |
