# BR5 Plan: Benchmark Test

Measures how the BR5 analysis performs across inputs, start-up configurations and live settings, and compares it with the alternative solution (the ClickHouse SQL engine). It uses what [plan-monitoring.md](plan-monitoring.md) built: the figures stored with each run, the dashboards and Run History.

The test runs on the dev laptop only, so it is kept to **just enough runs to show each effect with real figures** for the report. It is not a full tuning study.

## 1. What was asked

| # | Ask | How this plan covers it |
|---|---|---|
| 1 | 2–3 inputs: different choices read different amounts of data, so they give different results | 3 inputs, from 100% down to 3% of the data (section 3.1) |
| 2 | For each input, 2–3 start-up configurations: they change performance | 3 Spark worker sizes, set at start-up (3.2) |
| 3 | For each input and start-up configuration, 2–3 live settings: the same input and cluster still give different run times | 3 settings presets, applied per run (3.3) |
| 4 | The ClickHouse engine with 1–2 setups, as the optimized alternative to compare with | 1 setup, on each input (3.4) |
| 5 | Little time and machine load | 27 Spark cells × 2 runs and 3 ClickHouse cells, about 1 h 40 min of machine time in 3 blocks that can run on different days (section 5) |
| 6 | Screenshots of each test, with the real figures, for the report | Saved for every cell by a script (section 6) |
| 7 | Added for the presentation (Memory management & Data skew handling): one variant that changes only `spark.sql.shuffle.partitions` | Block X, cell P1: the baseline L1 with 8 shuffle partitions (section 3.6) |
| 8 | Added for the presentation: the salting technique against data skew, measured like the other variants | Block X, cell K1: the baseline L1 with salting (section 3.6). Salting is applied in Spark when joining, so the imported data stays as it is (D11) |

## 2. Decisions taken in this plan

| # | Decision | Why |
|---|---|---|
| D1 | **The data is imported once at ×10** (1,193,900 bookings) and every run reads that import. | At ×1, Spark's fixed cost (about 11 s to start the session) hides most differences: ×1 and ×10 differed by only 6.6 s (result-pipeline.md). ×10 still takes about 40 s per default run. ×50 would make the baseline runs several minutes long and risks the driver reader running out of memory. The ×1 figures already exist in the earlier result files. |
| D2 | **Start-up configurations change the Spark worker's cores only** (1, 2, 4). Worker memory stays 2 GB. | A run's executor uses 512 MB plus overhead whatever the worker has, so changing the worker's memory alone changes nothing. Cores change how many tasks run at once. The Docker host has 14 CPUs and 16 GB, so 4 cores fits. |
| D3 | **The worker's cores come from an environment variable** `BR5_WORKER_CORES` (default 2) in `docker-compose.yml`, used for both `SPARK_WORKER_CORES` and the container's `cpus`. | The benchmark script can restart the worker with another size, and nothing changes when the variable isn't set (BR3/BR4 unaffected). |
| D4 | **Every live preset sets `spark.cores.max` to 4.** | The default is 2, which would cap the 4-core worker at 2 cores and hide the start-up effect. With 4, a run takes whatever the worker has (1, 2 or 4): on the current 2-core worker, `bench-default` behaves exactly like the default settings. |
| D5 | **The third live setting changes one thing from the default: 4 read partitions instead of 2.** | The read is the longest step (10–13 s). One change keeps the cause clear. It should help on 4 cores and may hurt on 1, where 4 slices run one after the other and each scans the fact table in ClickHouse. That shows that the best live setting depends on the start-up configuration. |
| D6 | **The ClickHouse engine gets one setup** (default settings, `engine = clickhouse`), run on the 2-core block only. | It doesn't use the Spark worker, so the worker's size can't change it. Its whole run uses about 0.13 s of ClickHouse CPU (result-monitoring.md), so giving ClickHouse more cores couldn't show either. |
| D7 | **2 measured runs per Spark cell, plus a third when the two differ by more than 15%.** One warm-up run after each worker restart is not counted. | Two identical runs differed by up to 30% earlier (23.3 vs 32.2 s). One run per cell could show a "difference" that is only noise. 3 runs everywhere would add about 45 min. |
| D8 | **Runs within a block are interleaved** (repeat 1 of every cell, then repeat 2), not run cell by cell. | Background load or heat on the laptop then spreads over all cells instead of hitting one. |
| D9 | **Screenshots are taken by a script after each block, not during runs.** It uses Playwright for Python with the Chrome already installed (`channel="chrome"`, no browser download), headless, and saves JPEGs. | My browser tool returns screenshots to me only and can't save them to files. Taking them after the runs keeps Grafana's and the app's queries off the machine while it is measured. JPEG keeps the whole set to about 20–30 MB in git. I check a sample of the images myself. |
| D10 | **Two small app additions** for the screenshots: the Promotion Finder shows a given run's result with `?run=<id>`, and Run History takes `?a=<id>` for run A (run B is already `?run=`). | The Promotion Finder only shows the latest result, so without this every screenshot would have to be taken straight after its run. Both are also useful links in normal use. |
| D11 | **Salting is done in the Spark job at join time, not in the stored data.** It is a new live setting, `pipeline.salt_buckets`; the import and `br5.fact_booking` are unchanged. | Salting adds a random bucket number (0..N−1) to each row of the big side and copies each row of the small side N times, once per bucket. The join then runs on (key, bucket), so one hot key is spread over N partitions. Both columns exist only inside the run. Storing a salt would only matter if ClickHouse itself had to spread a hot key, and it doesn't here. |
| D12 | **Block X starts from the baseline, L1** (`bench-baseline`), not from the best setting, and runs on S3 (4 cores). It measures L1 again in the same block. | Both effects need a configuration where they exist. The best setting (L3) has AQE on, which already merges the 200 shuffle partitions into a few, so changing their number does little. L3 also groups on integer keys and broadcasts the lookup tables, so it never moves bookings by a skewed key. Avoiding the skewed join is the first answer to skew, and the presentation can say so. L1 has AQE off, so every shuffle keeps exactly the configured partitions (4,245–4,861 tasks per run). It also has the skewed sort-merge joins. The earlier S3 L1 runs were made on another day, so the reference is measured next to the variants. Skew needs parallel tasks to show: on 4 cores, one task holding 72% of the rows leaves 3 cores idle. |
| D13 | **Block X takes 3 measured runs per cell**, not 2. AQE's skew join is not measured. | L1 is the noisiest cell: the S3 I1-L1 runs took 60, 79 and 81 s, and the gains expected here are a few seconds. AQE's skew join needs AQE on, and turning AQE on in L1 also merges the partitions, which is a much bigger change: the effect of the skew handling couldn't be separated. |

## 3. The matrix

### 3.1 Inputs (the user's choices)

| ID | Hotel / country / room type | Bookings read at ×10 | Used (not excluded) | What the analysis does |
|---|---|---|---|---|
| I1 | All / All / All | 1,193,900 (100%) | about 744,000 | Ranks room type × country targets (573 found), top 10 cards |
| I2 | City Hotel / Portugal / All | 309,600 (26%) | about 106,000 | Ranks the room types for one country |
| I3 | Resort Hotel / United Kingdom / A | 35,360 (3%) | about 31,500 | One exact target, one card |

The choices are pushed down into the ClickHouse query, so the rows read really differ. Counts come from the CSV × 10 and are confirmed by the first runs.

### 3.2 Start-up configurations (need a restart)

Only the Spark worker's cores change. They are set by one environment variable, `BR5_WORKER_CORES`, which `docker-compose.yml` passes to two places (service `spark-worker`):

| ID | `BR5_WORKER_CORES` | `SPARK_WORKER_CORES` (cores the worker offers Spark) | Container `cpus` (CPU limit of `spark-worker`) | What a run gets | Restart |
|---|---|---|---|---|---|
| S1 | 1 | 1 | 1 | 1 executor with 1 core | `spark-worker` only, done by the benchmark script |
| S2 | unset (default 2) | 2 | 2 | 1 executor with 2 cores | – (the project's normal setup) |
| S3 | 4 | 4 | 4 | 1 executor with 4 cores | `spark-worker` only |

"What a run gets" follows from two live settings left at their defaults: `spark.executor.cores` is unset, so one executor takes all the worker's cores, and `spark.cores.max` is 4 in every Spark preset (3.3), so the worker's cores are the limit.

The same in all three:

| Container | Setting | Value |
|---|---|---|
| `spark-worker` | `SPARK_WORKER_MEMORY` (memory the worker offers Spark) / `mem_limit` | 2G / 2500m. A run's executor uses 512m plus 384m overhead, so memory is never the limit |
| `spark-master` | `cpus` / `mem_limit` | 0.5 / 512m |
| `br5-app` (Streamlit app and the Spark driver) | `cpus` / `mem_limit` | 1.0 / 2g; driver memory 512m |
| `clickhouse` | `cpus` / `mem_limit` | 1.0 / 1g; server settings at ClickHouse's defaults |

### 3.3 Live settings (presets, applied per run)

A preset is a named set of values from the Advanced Settings page (`jobs/br5_promotion/run_settings.py`). The benchmark script saves these presets, so they appear in the app and in Grafana's "settings from" filter.

| ID | Preset | Built from |
|---|---|---|
| L1 | `bench-baseline` | The built-in `baseline` preset (the naive variant) + `spark.cores.max=4` |
| L2 | `bench-default` | All defaults + `spark.cores.max=4` |
| L3 | `bench-read4` | All defaults + `spark.cores.max=4` + `pipeline.read_partitions=4` |

**Every setting that differs between the presets or from the default** (bold = differs from the default):

| Setting | What it controls | Default | L1 | L2 | L3 | C1 (3.4) |
|---|---|---|---|---|---|---|
| `spark.cores.max` | The most cores one run may take from the cluster | 2 | **4** | **4** | **4** | 2 (no Spark) |
| `pipeline.engine` | `spark`: the Spark pipeline. `clickhouse`: the same totals as ClickHouse SQL, no Spark | spark | spark | spark | spark | **clickhouse** |
| `pipeline.reader` | `parallel`: N Spark tasks each query their own slice (`source_row % N = i`) from ClickHouse. `driver`: one query on the driver, whose rows are then sent to the executor (`spark.createDataFrame`) | parallel | **driver** | parallel | parallel | – |
| `pipeline.read_partitions` | N, the slices of the parallel reader | 2 | 2 (not used) | 2 | **4** | – |
| `pipeline.join` | `late`: group on integer keys, attach names to the result rows only. `early`: join every booking with the 4 lookup tables (room, country, channel, date) before cleaning | late | **early** | late | late | – |
| `pipeline.cache` | Keep the cleaned bookings (`persist`, level `MEMORY_AND_DISK`) for the clean, aggregate and profile steps. Off: each step recomputes them from the read | true | **false** | true | true | – |
| `spark.sql.adaptive.enabled` | AQE: re-plan with real sizes; merges small shuffle partitions (into 64m targets) and splits skewed join partitions | true | **false** | true | true | – |
| `spark.sql.autoBroadcastJoinThreshold` | Tables under this size are copied to every executor instead of shuffled. -1: never, so every join is a sort-merge join | 10m | **-1** | 10m | 10m | – |
| `pipeline.writer` | How an import writes to ClickHouse. Part of the built-in `baseline`; **no effect on an analysis run** | partitions | pandas | partitions | partitions | partitions |

"–" means the setting is ignored by that engine.

**Left at their defaults in all four presets**, so they are the same in every run:
- Resources: `spark.executor.memory` 512m, `spark.executor.cores` unset (one executor takes all the worker's cores), `spark.driver.memory` 512m.
- Shuffle: `spark.sql.shuffle.partitions` 200. With AQE on, these are merged into a few (21–41 Spark tasks per run in total); with AQE off (L1), every shuffle runs 200 tasks (4,245–4,861 per run). `spark.shuffle.compress` true, `spark.io.compression.codec` lz4.
- AQE details (used only when AQE is on): `coalescePartitions.enabled` true, `advisoryPartitionSizeInBytes` 64m, `skewJoin.enabled` true (factor 5, threshold 256m).
- Memory: `spark.memory.fraction` 0.6, `spark.memory.storageFraction` 0.5, off-heap off.
- `spark.serializer` java, `pipeline.storage_level` MEMORY_AND_DISK.
- ClickHouse per-query limits: `clickhouse.max_threads` 0 and `clickhouse.max_memory_usage` 0 (both: the server's default).
- `spark.eventLog.enabled` true (needed for the Spark screenshots).
- All analysis rules (`rules.*`: low-season threshold 60%, minimum bookings 100, top 10 cards, …). They change the answer, not the performance, so the answer depends on the input only.

### 3.4 ClickHouse engine

| ID | Preset | Runs |
|---|---|---|
| C1 | `bench-clickhouse`: all defaults + `pipeline.engine=clickhouse` (column C1 above) | I1, I2, I3 in the S2 block, 3 runs each (about 3 s per run) |

The ClickHouse engine runs as a Python job in `br5-app` that sends 4 SQL queries to ClickHouse; no Spark session is created. So the Spark settings and the worker's cores don't apply to it. Its resources are ClickHouse's 1 CPU and 1 GB, plus `br5-app` for the Python side.

### 3.5 Counts

| | Cells | Measured runs | Warm-ups |
|---|---|---|---|
| Spark: 3 inputs × 3 start-up × 3 live | 27 | 54 (+ thirds where needed) | 3 |
| ClickHouse | 3 | 9 | – |
| Import ×10 | 1 | 1 | – |
| Block X (3.6): 3 inputs × 3 presets, S3 only | 9 | 27 | 1 |

### 3.6 Block X: shuffle partitions and data skew (added for the presentation)

All cells run on S3 (4 cores) and on the three inputs, 3 measured runs each, interleaved (D8, D12, D13).

| ID | Preset | Built from | Compared with | What the comparison shows |
|---|---|---|---|---|
| L1 | `bench-baseline` (unchanged) | – | – | The reference: the baseline, measured again in this block |
| P1 | `bench-baseline-shuffle8` | L1 + `spark.sql.shuffle.partitions=8` | L1 | Shuffle partitions alone: 8 (2 per core, the usual rule) against Spark's default 200, with AQE off so the number is used as set |
| K1 | `bench-baseline-salt8` | L1 + `pipeline.salt_buckets=8` | L1 | Salting alone: the 4 skewed lookup joins with 8 buckets per key |

**Every setting that differs** (bold = differs from L1):

| Setting | Default | L1 | P1 | K1 |
|---|---|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | 200 | **8** | 200 |
| `pipeline.salt_buckets` (new) | 1 (off) | 1 | 1 | **8** |

Everything else is as in L1 (section 3.3): `spark.cores.max` 4, the driver reader, the early join, cache off, AQE off, broadcast off (-1), executor memory 512m.

**Where the skew is.** The early join with broadcast off joins every booking with the 4 lookup tables by sort-merge joins, each shuffling the bookings by its key. The keys are skewed: room type A is 72% of the bookings, Portugal 41%, the Online TA channel 47%. Checked in an existing S3 I1 L1 run: in the room join, one of 200 tasks read 72% of the rows and ran 1.9 s against a 6 ms median. With the cache off, these joins run again in each of the clean, aggregate and profile steps, so the skew shows about 3 times per run. I2 (City Hotel, Portugal) and I3 (one room type, one country) put 100% of their rows on one country key, and I3 on one room key too: the worst case for hash partitioning, but on less data.

**Why these values:**
- **P1, 8 partitions.** About 200 tasks per run instead of 4,245–4,861, so much less scheduling and per-task overhead. Each task then holds about 25 times more data, so the spill and GC figures (memory) are part of the result. L1 spilled 229 MB on I1 with 200 partitions. Fewer partitions don't fix the skew: the hot key still lands in one partition.
- **K1, 8 buckets.** 2 buckets per core spread a hot key over all 4 cores. The lookup tables are small (the largest, the dates, has 1,583 rows; 12,664 after copying), so copying them 8 times costs nothing measurable.

**Salting in the code** (`pipeline.salt_buckets`, D11): only used with the early join, and ignored with the late join.
- In `pipeline_spark.py`, the bookings get a column `salt = floor(rand() * N)`.
- Each lookup table is built with N copies of each row, `salt` = 0..N−1. It is built in Python, where the rows already are.
- The 4 joins use (key, `salt`).
- Each booking matches exactly one copy, so the result is the same as without salting. That holds even when, with the cache off, a step recomputes the bookings and draws other salts. Check 4 confirms it.
- The registry (`run_settings.py`, group Joins) gets the new setting, an int from 1 to 64 with default 1. The Advanced Settings page and the run snapshot then show it like the others.

**Expected size of the effect.**
- P1 should be clearly faster than L1: the 200-partition shuffles are where L1's time goes ("Where the time goes" in result-benchmark.md).
- K1's gain is limited by how long the skewed tasks last: 0.4–1.9 s per join in L1, so perhaps 3–8 s on a 60–80 s run, which is within L1's noise. The clearest evidence is therefore inside Spark: per join stage, the largest task's share of the rows and its time against the median (section 7, table F, and the `stages.jpg` screenshot). The run times are still reported, as measured.

## 4. Tooling to build

| File | What |
|---|---|
| `docker-compose.yml` | `spark-worker`: `SPARK_WORKER_CORES=${BR5_WORKER_CORES:-2}` and `cpus: ${BR5_WORKER_CORES:-2}` (D3). |
| `tests/br5/benchmark.py` | The driver, with sub-commands below. Run from the host. |
| `apps/br5/views/promotion_finder.py`, `backend.py` | `?run=<id>`: show that run's result with a note "an earlier run", and set the three choices to that run's (D10). |
| `apps/br5/views/run_history.py` | `?a=<id>` for run A (D10). The compare tables are as tall as their rows: they scrolled inside a fixed height, which cut rows off a screenshot. |
| `Makefile` | `br5-bench BLOCK=S1` and `br5-bench-screens BLOCK=S1`. |
| `docs/guide/br5/config-guide.md` | The `BR5_WORKER_CORES` variable in the restart guide. |
| `jobs/br5_promotion/run_settings.py`, `pipeline_spark.py` | Block X: the `pipeline.salt_buckets` setting and salting in the early join (3.6). |
| `tests/br5/benchmark.py` | Block X: the presets P1 and K1, block `X` (S3 cores, cells L1, P1, K1, 3 runs each), and the figures of table F. These are read from the Spark history server's REST API (`/api/v1/applications/<app>/stages`, task summary with quantiles 0.5 and 1.0), so no new dependency is needed. |

`benchmark.py` sub-commands:
- **`presets`:** saves the four presets of 3.3 and 3.4, and the four new ones of 3.6.
- **`block S1|S2|S3|X`:**
  1. Restarts `spark-worker` with the block's cores if needed, and waits until the Spark master reports a worker with that many cores.
  2. Runs one warm-up: the first selected cell, recorded as `warmup` and left out of the figures.
  3. Runs the cells interleaved (D8) through `scripts/br5.py run --preset ...`, and adds a third run where the two differ by more than 15%.
  4. Appends each run to the manifest `docs/result/br5/benchmark/runs.csv`: block, cell, input, preset, repeat, run id, status and run time.
  5. Ends with `--clickhouse` in S2 for C1.
- **`screens S1|S2|S3`:** takes the screenshots of section 6 for every cell of the block.
- **`report`:** reads the manifest and `br5.run_history` and writes `docs/result/br5/benchmark/tables.md`, with the tables of section 7. It also checks section 8 and prints any failure.
- **`stages X`:** block X's skew figures (table F) for every measured run, from the Spark history server: one row per shuffle stage in `docs/result/br5/benchmark/stages-X.csv`, and per run the room join and the skewed stages.
- **`restore`:** puts the worker back to 2 cores.

## 5. Procedure and time

| Step | What | Estimated machine time |
|---|---|---|
| 0 | `make br5-up`; `scripts/br5.py import --multiplier 10 --yes`; `benchmark.py presets` | 3 min |
| 1 | Smoke test: one run of I1 × L1 on S2 (the heaviest cell at this worker size), to check the driver reader holds 1.19 M rows in 512 MB of driver memory (risk R2) | 2 min |
| 2 | Block S2, including C1; then its screenshots | 25 min + 5 min |
| 3 | Block S1; then its screenshots | 35 min + 5 min |
| 4 | Block S3; then its screenshots; `benchmark.py restore` | 15 min + 5 min |
| 5 | Summary screenshots (section 6), `benchmark.py report`, `docs/result/br5/result-benchmark.md` | 5 min |
| 6 | Block X (3.6): build the salting setting, `benchmark.py presets`, block X, its screenshots, `restore` | 35 min + 5 min |

That is about 1 h 40 min in total. Each block is self-contained, so the blocks can run in separate sessions. S2 goes first because it needs no restart and contains the reference cells.

Machine rules during a block: no Grafana or app page open in a browser, and no other heavy work on the laptop. The event log stays on (the default) because the Spark pages need it; its cost was not measurable (result-monitoring.md).

## 6. Screenshots

Saved under `docs/result/br5/benchmark/<cell>/`, for example `S2-I1-L2/`. They come from the cell's first measured run. Size: 1600 px wide, full page, JPEG.

| File | Page | Shows | Taken for |
|---|---|---|---|
| `finder.jpg` | Promotion Finder `?run=<id>`, run summary (R4) expanded | The cards, low season and the run's figures: run time, rows read and used, settings source | Every cell |
| `compare.jpg` | Run History `?a=<reference>&run=<id>` | Run time and steps side by side, the settings that differ, resource figures, "same result: yes" | L1 and L3 against L2 of the same input and block. S1/S3 L2 against S2 L2. C1 against S2 L2 |
| `resources.jpg` | Grafana BR5 Resource use (kiosk mode), from 2 s before the run to 2 s after (interleaved runs follow each other within seconds, so a wider window shows parts of the neighbours and inflates "CPU seconds used") | CPU and memory of `spark-worker`, `br5-app` and `clickhouse`, with the run shaded and the worker's CPU limit | Every Spark cell (ClickHouse runs are shorter than one 5 s sample) |
| `spark.jpg` | Spark history, the application's Jobs page | Jobs named after the steps, with their durations | Every Spark cell |
| `stages.jpg` | Spark history, the stage page of the room-type join (the most skewed), Summary Metrics table | Duration and shuffle read records at min / median / max over the tasks: the skew, and how K1 spreads it | Block X, L1, P1 and K1 |

Summary screenshots in `docs/result/br5/benchmark/summary/`:
- `live-SX.jpg`: Grafana Run performance for each block's time range, "Compare by setting" = `launch.settings_from`. Shows the median per live preset.
- `startup.jpg`: Run performance with "Compare by setting" = `cluster.worker_cores`, filtered to `bench-default` and I1. Shows the median per worker size.
- `history.jpg`: the Run History table with all benchmark runs.

That is about 110 images. The script logs in to Grafana through its login API. It waits for each page to finish loading: the app's "Running" indicator gone, and Grafana panels without a loading state. It fails loudly instead of saving a half-drawn page.

## 7. Report tables (`benchmark.py report`)

- **A. Input × live setting** (S2): run time per cell. Shows the effect of the input and of the live settings.
- **B. Start-up × live setting**, one table per input: run time. Shows the start-up effect and how it interacts with the live settings (L3 vs L2 on 1 and 4 cores).
- **C. Spark vs ClickHouse**, per input: S2 L2, the best Spark cell, and C1. Totals, and time inside the steps, since Spark's total includes about 11 s of session start.
- **D. Per cell**:
  - run time (each run, and the mean)
  - steps
  - Spark CPU, tasks, shuffle and GC
  - container CPU seconds and peak memory of `spark-worker`
  - ClickHouse rows read
- **E. Results:** cards and low season per input, identical across all cells.
- **F. Block X**, per input:
  - run time of L1, P1 and K1, with P1 and K1 against L1
  - Spark tasks, shuffle, spill and GC time (P1 against L1)
  - per join stage of L1, P1 and K1: tasks, the largest task's share of the rows read, and the largest task's time against the median

`docs/result/br5/result-benchmark.md` gets these tables, the observations for the report, and the screenshot index. It is updated after each block, not only at the end.

## 8. Verification (that the benchmark measured what it claims)

| # | Check | Pass when |
|---|---|---|
| 1 | Each run used its intended start-up configuration | The run's snapshot `cluster.worker_cores` equals the block's cores. Prometheus `container_spec_cpus` for `spark-worker` matches during the block |
| 2 | Each run used its intended live settings | `launch.settings_from` is the cell's preset. For L1, Spark reports AQE off and broadcast -1. For L3, the parallel reader sent 4 read queries whose rows add up to the rows read. ClickHouse's own "rows read" counts the rows it scanned, not the rows returned (the index granules matching the filter, once per slice), so it is not a simple multiple |
| 3 | Inputs read what 3.1 says | Rows read per input equals the table (×10 counts) in every cell |
| 4 | Same input, same result | For each input, all Spark cells and C1 give the same cards and month rows (`same_result`; numbers equal within a relative 1e-9, since sums added in another order differ in their last bits). The settings change speed, never the answer |
| 5 | All runs succeeded; repeats are close | No failed run in the manifest (or the failure is reported as a finding, see R2). Each cell's runs are within 15%, or have a third run |
| 6 | Screenshots complete and readable | One folder per cell with its files, and the summary set. I open a sample from each block to check that the figures are visible and match the manifest |
| 7 | Stack restored | Worker back to 2 cores; `make br5-check-dashboards` passes |
| 8 | Block X used the intended settings | P1: Spark reports `spark.sql.shuffle.partitions` 8 and its shuffle stages have 8 tasks. All three: AQE off, and the plans show `SortMergeJoin` and no `BroadcastHashJoin` |
| 9 | The salting really happened | L1 and P1: the room-join stage has one task with most of the rows. K1: the join keys include `salt`, and the largest task's share drops to about 1/8 of the hot key's rows or less |

## 9. Risks

- **R1 Noise.** Laptop load, heat and Docker can move run times by 15–30%. Mitigation: D7, D8, a quiet machine. The report gives both runs, not only the mean.
- **R2 Driver reader at ×10.** The baseline pulls 1.19 M rows through the 512 MB driver. Step 1 tests this first. If it runs out of memory, that is a finding (the naive reader doesn't scale). In that case the plan is changed to give `bench-baseline` 1 GB of driver memory, so the other baseline cells still run, and this is noted.
- **R3 S1 is slow.** The baseline on 1 core may take 3–4 min per run. It is included because it shows the start-up effect most strongly.
- **R4 4 cores with 512 MB executor memory.** Four tasks share one small heap, so S3 may show more GC or spill. That is a real result: memory per core shrinks. GC and spill figures are recorded with each run.
- **R5 Compose interpolation of `cpus`.** Checked: Compose accepts `${BR5_WORKER_CORES:-2}` there.
- **R6 Screenshot size.** About 110 JPEGs of about 200 KB each. If the total grows past about 40 MB, they move to Git LFS.
- **R7 Block X effects may be small in seconds.** See 3.6: the skewed tasks last at most about 2 s each. The report then leans on the stage figures (table F), and says plainly if run times differ by less than the noise.

## 10. Not covered

- ×1 and ×50: ×1 figures are in result-pipeline.md and result-monitoring.md; ×50 is left out (D1).
- Import benchmarks: the writers are compared in result-data-import.md.
- Single-setting studies for caching and the serializer; several workers; table layouts (C9). Skew handling and shuffle partitions are covered by block X only, on S3 and from the baseline. AQE's skew join is not measured (D13).

## 11. Status

The process was checked on one cell (S3-I3-L3) and works. The measured results are in [result-benchmark.md](../../result/br5/result-benchmark.md), updated after each block. All blocks are done: S1, S2 with C1, S3, and block X (3.6, shuffle partitions and skew, 2026-10-24); 106 runs, all succeeded. Not built: the `report` sub-command and the summary screenshots (section 6); `result-benchmark.md` has the report tables instead.
