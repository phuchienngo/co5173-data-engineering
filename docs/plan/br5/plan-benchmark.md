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

## 3. The matrix

### 3.1 Inputs (the user's choices)

| ID | Hotel / country / room type | Bookings read at ×10 | Used (not excluded) | What the analysis does |
|---|---|---|---|---|
| I1 | All / All / All | 1,193,900 (100%) | about 744,000 | Ranks room type × country targets (573 found), top 10 cards |
| I2 | City Hotel / Portugal / All | 309,600 (26%) | about 106,000 | Ranks the room types for one country |
| I3 | Resort Hotel / United Kingdom / A | 35,360 (3%) | about 31,500 | One exact target, one card |

The choices are pushed down into the ClickHouse query, so the rows read really differ. Counts come from the CSV × 10 and are confirmed by the first runs.

### 3.2 Start-up configurations (need a restart)

| ID | Spark worker | Restart |
|---|---|---|
| S1 | 1 core, 2 GB | `spark-worker` only, done by the benchmark script |
| S2 | 2 cores, 2 GB (the current setup) | – |
| S3 | 4 cores, 2 GB | `spark-worker` only |

ClickHouse (1 CPU, 1 GB) and the app container are the same in all three.

### 3.3 Live settings (presets, applied per run)

| ID | Preset | Settings |
|---|---|---|
| L1 | `bench-baseline` | The built-in `baseline` (driver reader, early join, no cache, broadcast off, AQE off) + cores max 4 |
| L2 | `bench-default` | Defaults + cores max 4 |
| L3 | `bench-read4` | Defaults + cores max 4 + read partitions 4 |

The benchmark script saves these presets, so they appear in the app and in Grafana's "settings from" filter.

### 3.4 ClickHouse engine

| ID | Preset | Runs |
|---|---|---|
| C1 | `bench-clickhouse`: defaults + engine `clickhouse` | I1, I2, I3 in the S2 block, 3 runs each (about 3 s per run) |

### 3.5 Counts

| | Cells | Measured runs | Warm-ups |
|---|---|---|---|
| Spark: 3 inputs × 3 start-up × 3 live | 27 | 54 (+ thirds where needed) | 3 |
| ClickHouse | 3 | 9 | – |
| Import ×10 | 1 | 1 | – |

## 4. Tooling to build

| File | What |
|---|---|
| `docker-compose.yml` | `spark-worker`: `SPARK_WORKER_CORES=${BR5_WORKER_CORES:-2}` and `cpus: ${BR5_WORKER_CORES:-2}` (D3). |
| `tests/br5/benchmark.py` | The driver, with sub-commands below. Run from the host. |
| `apps/br5/views/promotion_finder.py`, `backend.py` | `?run=<id>`: show that run's result with a note "an earlier run", and set the three choices to that run's (D10). |
| `apps/br5/views/run_history.py` | `?a=<id>` for run A (D10). The compare tables are as tall as their rows: they scrolled inside a fixed height, which cut rows off a screenshot. |
| `Makefile` | `br5-bench BLOCK=S1` and `br5-bench-screens BLOCK=S1`. |
| `docs/guide/br5/config-guide.md` | The `BR5_WORKER_CORES` variable in the restart guide. |

`benchmark.py` sub-commands:
- **`presets`:** saves the four presets of 3.3 and 3.4.
- **`block S1|S2|S3`:**
  1. Restarts `spark-worker` with the block's cores if needed, and waits until the Spark master reports a worker with that many cores.
  2. Runs one warm-up: the first selected cell, recorded as `warmup` and left out of the figures.
  3. Runs the cells interleaved (D8) through `scripts/br5.py run --preset ...`, and adds a third run where the two differ by more than 15%.
  4. Appends each run to the manifest `docs/result/br5/benchmark/runs.csv`: block, cell, input, preset, repeat, run id, status and run time.
  5. Ends with `--clickhouse` in S2 for C1.
- **`screens S1|S2|S3`:** takes the screenshots of section 6 for every cell of the block.
- **`report`:** reads the manifest and `br5.run_history` and writes `docs/result/br5/benchmark/tables.md`, with the tables of section 7. It also checks section 8 and prints any failure.
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

`docs/result/br5/result-benchmark.md` gets these tables, the observations for the report, and the screenshot index.

## 8. Verification (that the benchmark measured what it claims)

| # | Check | Pass when |
|---|---|---|
| 1 | Each run used its intended start-up configuration | The run's snapshot `cluster.worker_cores` equals the block's cores. Prometheus `container_spec_cpus` for `spark-worker` matches during the block |
| 2 | Each run used its intended live settings | `launch.settings_from` is the cell's preset. For L1, Spark reports AQE off and broadcast -1. For L3, the parallel reader sent 4 read queries whose rows add up to the rows read. ClickHouse's own "rows read" counts the rows it scanned, not the rows returned (the index granules matching the filter, once per slice), so it is not a simple multiple |
| 3 | Inputs read what 3.1 says | Rows read per input equals the table (×10 counts) in every cell |
| 4 | Same input, same result | For each input, all Spark cells and C1 give the same cards and month rows (`same_result`). The settings change speed, never the answer |
| 5 | All runs succeeded; repeats are close | No failed run in the manifest (or the failure is reported as a finding, see R2). Each cell's runs are within 15%, or have a third run |
| 6 | Screenshots complete and readable | One folder per cell with its files, and the summary set. I open a sample from each block to check that the figures are visible and match the manifest |
| 7 | Stack restored | Worker back to 2 cores; `make br5-check-dashboards` passes |

## 9. Risks

- **R1 Noise.** Laptop load, heat and Docker can move run times by 15–30%. Mitigation: D7, D8, a quiet machine. The report gives both runs, not only the mean.
- **R2 Driver reader at ×10.** The baseline pulls 1.19 M rows through the 512 MB driver. Step 1 tests this first. If it runs out of memory, that is a finding (the naive reader doesn't scale). In that case the plan is changed to give `bench-baseline` 1 GB of driver memory, so the other baseline cells still run, and this is noted.
- **R3 S1 is slow.** The baseline on 1 core may take 3–4 min per run. It is included because it shows the start-up effect most strongly.
- **R4 4 cores with 512 MB executor memory.** Four tasks share one small heap, so S3 may show more GC or spill. That is a real result: memory per core shrinks. GC and spill figures are recorded with each run.
- **R5 Compose interpolation of `cpus`.** Checked: Compose accepts `${BR5_WORKER_CORES:-2}` there.
- **R6 Screenshot size.** About 110 JPEGs of about 200 KB each. If the total grows past about 40 MB, they move to Git LFS.

## 10. Not covered

- ×1 and ×50: ×1 figures are in result-pipeline.md and result-monitoring.md; ×50 is left out (D1).
- Import benchmarks: the writers are compared in result-data-import.md.
- Single-setting studies for caching, the serializer, skew handling and shuffle partitions; several workers; table layouts (C9).

## 11. Status

**Trial cell (2026-10-19): S3-I3-L3**, to check the process before the full matrix:
- **Process:** the worker restart to 4 cores, the warm-up and 2 measured runs all worked. The two runs were 23.4 and 23.7 s, so no third run was needed.
- **Checks 1–4 passed:**
  - The snapshot shows `cluster.worker_cores` 4, cores max 4 and read partitions 4.
  - Prometheus shows a CPU limit of 4.
  - There were 4 read queries, whose rows add up to 35,360.
  - Same result: yes.
- **Screenshots:** the 4 files are 150–300 KB each. A first version showed All / All / All above a run's result, cut rows in the compare tables and part of the next run in the Grafana window; all three are fixed (sections 4 and 6).
- **Not yet built:** the `report` sub-command.
