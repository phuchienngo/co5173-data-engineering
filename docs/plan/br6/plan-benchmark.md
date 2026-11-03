# BR6 Plan: Benchmark Test

Measures how the Route Finder run performs across inputs, start-up configurations and live settings. It also compares the Spark pipeline with the alternative solution, the ClickHouse engine. It follows [BR5's benchmark plan](../br5/plan-benchmark.md) and its lessons, and uses what [plan-monitoring.md](plan-monitoring.md) built: the figures stored with each run, the dashboards and Run History.

As for BR5, the test runs on the dev laptop only, so it is kept to **just enough runs to show each effect with real figures** for the report.

**Status:** planned on 2026-11-02; the open points of section 11 were accepted as proposed. The tooling of section 4 is built. Block H1 is done (2026-11-03); the measured results are in [result-benchmark.md](../../result/br6/result-benchmark.md), updated after each block.

**As built:** `docker-compose.yml` also gets `cpus: 1.0` back for `kafka` and `br5-app`, which BR5's ClickHouse blocks had tied to `BR5_CLICKHOUSE_CPUS` by mistake. The script's `check` compares each run with the first S2 L2 run of its input, or, before S2 has run, with the input's first run.

## 1. What is asked

| # | Ask | How this plan covers it |
|---|---|---|
| 1 | 2–3 inputs: different choices read different amounts of data | 3 routes in 3 cities, from 544,471 check-ins and 42,795 places down to 1,470 and 598 (section 3.1) |
| 2 | For each input, 2–3 start-up configurations | 3 Spark worker sizes, set at start-up (3.2), as in BR5 |
| 3 | For each input and start-up configuration, 2–3 live settings | 3 settings presets per run (3.3) |
| 4 | **ClickHouse is the alternative solution, as in BR5**, so it gets its own start-up configurations and live settings, like Spark | Blocks H1–H3 from the start: 3 ClickHouse sizes × 4 live settings × 3 inputs (3.5). They build in BR5's finding about ClickHouse's threads (D9) |
| 5 | The single-option comparisons the pipeline result left open ("Not yet measured" in [result-pipeline.md](../../result/br6/result-pipeline.md)) | Block V: the matching method, the distance function and the reader, one at a time, 3 runs each (3.4) |
| 6 | Little time and machine load | About 285 runs, about 2 h of machine time in 7 blocks that can run on different days (section 5) |
| 7 | Screenshots of each test, with the real figures, for the report | Saved for every cell by the script (section 6) |

## 2. Decisions taken in this plan

| # | Decision | Why |
|---|---|---|
| D1 | **The data stays at ×1** (the current import, 2,629,541 check-ins). No re-import. | Unlike BR5, a default BR6 run doesn't read the check-ins: it reads one statistics row per place, counted at import. So the multiplier doesn't change its work at all. Even the "statistics every run" variant took 51.7 s at ×1 and 52.7 s at ×5 (result-pipeline.md). At ×5 the driver reader runs out of memory, so the baseline couldn't be measured at all. At ×1 every cell gives a result. The input dimension already spans 370× in check-ins between the cities. The ×5 figures and the out-of-memory failure are already in the pipeline result. |
| D2 | **Start-up configurations change the Spark worker's cores only (1, 2, 4)**, through BR5's variable `BR5_WORKER_CORES`. Every Spark preset sets `spark.cores.max` to 4. | Same reasons as BR5's D2–D4. The variable already exists in `docker-compose.yml` and is used as it is: one variable for the shared worker. |
| D3 | **The third live setting changes one thing from the default: place statistics counted every run** (`pipeline.place_stats = run`, with the parallel reader). | The statistics at import are BR6's main optimization (F4 of the pipeline plan). The baseline changes 8 settings at once, so it can't show this one on its own. With statistics every run, Spark really reads and aggregates the city's check-ins. So this is the cell where the input size and the worker's cores should matter. (BR5's third setting, 4 read slices, has no effect on a default BR6 run, which reads no check-ins.) |
| D4 | **Inputs are three routes already used in the pipeline result**, with fixed venue ids, place types and visitor (3.1). | Their lists and counts are known and were checked, so the first runs also check the setup. |
| D5 | **2 measured runs per Spark cell, a third when the two differ by more than 15%; one warm-up after each restart; runs interleaved across the cells.** | BR5's D7 and D8: two runs in BR5 differed by up to 30%. Interleaving spreads background load over all cells. |
| D6 | **Block V measures single options from the default on S2, with 3 runs per cell and the default measured again in the block.** Inputs I1 and I2 only. | The effects are a few seconds inside a 25–35 s run (result-pipeline.md: one run each), so 3 runs are needed. Beijing has 553 places with a location: matching them takes no measurable time with any method. The clearest evidence is Spark's own figures for the match step (`spark.cpu_ms@match`, `spark.run_ms@match`), which are exact. |
| D7 | **The ClickHouse CPUs come from BR5's variable `BR5_CLICKHOUSE_CPUS`** (1, 2, 4). Memory stays 1 GB. | Already in `docker-compose.yml`. A whole ClickHouse run peaks at a few tens of MB, far below 1 GB. |
| D8 | **ClickHouse cells take 5 measured runs; the report leans on ClickHouse's own query figures** from `system.query_log` (query time, CPU, peak threads, peak memory, rows read). | BR5's D14: a ClickHouse run takes about 2 s, of which 1.2–1.5 s is starting Python and connecting. |
| D9 | **ClickHouse's live settings are a 2 × 2:** statistics from the import or counted every run, and ClickHouse's memory-per-thread limit on (the default) or off. BR5's "1 thread" setting is not repeated. | BR5 found that ClickHouse 26.8 runs a query on one thread unless each thread has 1 GiB of free memory (`max_threads_min_free_memory_per_thread`). In our 1 GB container, the default therefore always runs on one thread, so a "1 thread" setting equals the default. Lifting the limit is what lets ClickHouse use its CPUs. Statistics every run gives ClickHouse real work: an aggregation over the city's check-ins, the counterpart of Spark's L3. |
| D10 | **BR6 gets the same small additions BR5 got for its ClickHouse blocks:** the setting `clickhouse.limit_threads_by_memory`, `clickhouse.peak_threads` in the run figures, and `cluster.clickhouse_max_threads` in the snapshot (section 4). | Without them, the HL2/HL4 cells couldn't be set from a preset, and the check that ClickHouse used its CPUs (check 11) would have no figure. |
| D11 | **The benchmark script is BR6's own, `tests/br6/benchmark.py`. It loads BR5's script as a module for the parts that don't depend on the BR:** worker and ClickHouse restarts, the history server API and the Playwright helpers. BR5's script is not changed. | About half of BR5's script is generic and tested on 247 runs. Copying it would leave two copies to keep in step. The BR6 parts are the inputs, presets, run command, manifest, references and pages. |
| D12 | **Screenshots are taken by the script after each block**, as in BR5 (Playwright, installed Chrome, headless, JPEG, 1600 px wide), and stored with Git LFS. | BR5's D9 and R6: about 220 images of about 250 KB each. |

## 3. The matrix

### 3.1 Inputs (the user's choices)

All with every place type chosen. The figures are at ×1.

| ID | City | Route | Visitor | Check-ins read | Places (with a location) | Places near the route | Why |
|---|---|---|---|---|---|---|---|
| I1 | Istanbul (most check-ins) | Eminönü (2005) → stop Ortaköy (3982) → Fatih Sultan Mehmet Köprüsü (100), 11.4 km, 2 legs | 88647 | 544,471 | 42,795 (30,279) | 4,574 | The heaviest choice: most check-ins, a stop, and a chosen visitor (adds the visitor step) |
| I2 | Jakarta (most places) | Emporium Pluit Mall (1549) → Gandaria City (1373), 13.0 km, 1 leg | General | 412,100 | 71,150 (49,291) | 7,178 | The reference route of every earlier BR6 result: the most places to match |
| I3 | Beijing (thinnest data) | IKEA (40) → Joy City Chaoyang (409) | General | 1,470 | 598 (553) | 61 | The fixed cost: almost no data |

What each input reads depends on the statistics setting:
- **Statistics at import** (default): the city's places with one statistics row each. I2 reads the most (71,150 places), I3 the least (598).
- **Statistics every run** (L1, L3, HL3, HL4): the city's check-ins as well. I1 reads the most (544,471), I3 the least (1,470).

### 3.2 Start-up configurations (need a restart)

As in BR5 (its section 3.2): only the Spark worker's cores change, through `BR5_WORKER_CORES`.

| ID | `BR5_WORKER_CORES` | Worker cores = `spark-worker` `cpus` | What a run gets | Restart |
|---|---|---|---|---|
| S1 | 1 | 1 | 1 executor with 1 core | `spark-worker` only, done by the script |
| S2 | unset (default 2) | 2 | 1 executor with 2 cores | – (the project's normal setup) |
| S3 | 4 | 4 | 1 executor with 4 cores | `spark-worker` only |

The same in all three: worker memory 2G (container 2500m), executor memory 512m plus 384m overhead; `br5-app` (the app and the Spark driver) 1 CPU / 2g, driver memory 512m; `clickhouse` 1 CPU / 1g.

### 3.3 Spark live settings (presets, applied per run)

The script saves these presets in `br6.settings`, so they appear in the app and in Grafana's "settings from" filter.

| ID | Preset | Built from |
|---|---|---|
| L1 | `bench-baseline` | The built-in `baseline` preset (the naive variant) + `spark.cores.max=4` |
| L2 | `bench-default` | All defaults + `spark.cores.max=4` |
| L3 | `bench-stats-run` | All defaults + `spark.cores.max=4` + `pipeline.place_stats=run` (D3) |

**Every setting that differs** (bold = differs from the default):

| Setting | What it controls | Default | L1 | L2 | L3 |
|---|---|---|---|---|---|
| `spark.cores.max` | The most cores one run may take | 2 | **4** | **4** | **4** |
| `pipeline.place_stats` | `import`: read one statistics row per place, counted at import. `run`: count check-ins and different visitors per place from the city's check-ins at every run | import | **run** | import | **run** |
| `pipeline.reader` | How the check-ins are read (statistics every run only). `parallel`: each Spark task reads its own slice from ClickHouse. `driver`: one query on the driver | parallel | **driver** | parallel | parallel |
| `pipeline.matching` | `grid`: compare only the places in the squares near the route. `all`: compare every place of the city | grid | **all** | grid | grid |
| `pipeline.distance` | Native Spark expressions, `pandas_udf`, or a plain Python UDF | native | **python_udf** | native | native |
| `pipeline.cache` | Keep the check-ins read for the clean and visitor steps | true | **false** | true | true |
| `spark.sql.adaptive.enabled` | AQE | true | **false** | true | true |
| `spark.sql.autoBroadcastJoinThreshold` | Broadcast tables under this size; -1: sort-merge joins | 10m | **-1** | 10m | 10m |
| `pipeline.writer` | Import writer; **no effect on a run** | partitions | pandas | partitions | partitions |

**Left at their defaults in every preset:**
- executor and driver memory 512m; `spark.sql.shuffle.partitions` 200; Java serializer
- square size 1 km; read slices 2; ClickHouse `max_threads` and `max_memory_usage` at the server's defaults
- the Spark event log on (needed for the Spark screenshots)
- all analysis rules (maximum detour 1 km, top 10, weights, minimum visitors and check-ins). They change the answer, not the performance, so the answer depends on the input only.

**Cells:** 3 inputs × 3 live settings in each of S1, S2 and S3: 27 cells, 2 runs each (D5).

### 3.4 Block V: single options (S2)

Each cell changes **one** option from its reference, which is measured again in the block. Inputs I1 and I2, 3 runs per cell, interleaved (D6).

| ID | Preset | Change | Reference | What it shows |
|---|---|---|---|---|
| L2 | `bench-default` | – | – | The reference for V1–V4 |
| V1 | `bench-match-all` | `pipeline.matching=all` | L2 | The grid against comparing every place, with native distances |
| V2 | `bench-python-udf` | `pipeline.distance=python_udf` | L2 | A Python UDF against native expressions |
| V3 | `bench-pandas-udf` | `pipeline.distance=pandas_udf` | L2 | A vectorized `pandas_udf` against native expressions |
| V4 | `bench-match-all-python-udf` | `pipeline.matching=all` + `python_udf` | V2 | The grid when each distance is expensive: the case where it should show |
| L3 | `bench-stats-run` | – | – | The reference for V5 |
| V5 | `bench-stats-run-driver` | L3 + `pipeline.reader=driver` | L3 | The driver reader against the parallel reader, when check-ins are read |

All also have `spark.cores.max=4` (it changes nothing on 2 cores, and keeps the presets alike). **Cells:** 2 inputs × 7 = 14 cells, 3 runs each.

### 3.5 ClickHouse engine (the alternative solution)

The ClickHouse engine runs as a Python job in `br5-app` that sends its SQL to ClickHouse; no Spark application is created. So the Spark worker's size and the Spark settings don't apply to it, and its blocks have their own start-up configurations and live settings.

**Start-up configurations (need a ClickHouse restart):**

| ID | `BR5_CLICKHOUSE_CPUS` | `clickhouse` container `cpus` | ClickHouse's default `max_threads` | Restart |
|---|---|---|---|---|
| H1 | unset (default 1) | 1 | `auto(1)` | – |
| H2 | 2 | 2 | `auto(2)` | `clickhouse` only, done by the script |
| H3 | 4 | 4 | `auto(4)` | `clickhouse` only |

The same in all three: memory 1 GB; server settings at their defaults (with `system.metric_log` off, as in the whole project); the Spark worker stays at 2 cores; `br5-app` 1 CPU / 2 GB.

**Live settings (presets, D9):**

| ID | Preset | Statistics | ClickHouse threads | What it shows |
|---|---|---|---|---|
| HL1 | `bench-clickhouse` | import | the default: one thread in a 1 GB container | The alternative as shipped |
| HL2 | `bench-clickhouse-nomemlimit` | import | `clickhouse.limit_threads_by_memory=false` (sends `max_threads_min_free_memory_per_thread = 0`): up to N threads | Whether the CPUs help when the work is small (one row per place) |
| HL3 | `bench-clickhouse-stats-run` | run | the default | ClickHouse counting the statistics from the check-ins, as Spark's L3 does |
| HL4 | `bench-clickhouse-stats-run-nomemlimit` | run | limit off | Whether the CPUs help when there is real work (an aggregation over up to 544,471 check-ins) |

All four set `pipeline.engine=clickhouse` and leave everything else at its default.

**Cells:** 3 inputs × 4 live settings in each of H1, H2, H3: 36 cells, 5 runs each, interleaved, one warm-up after each restart.

**What differs from the Spark blocks** (as in BR5, section 3.4.3):
- **Start-up check:** `docker inspect` shows the CPU limit, and `system.settings` shows `max_threads` = `auto(N)`.
- **Other users:** during H2 and H3 nothing else may use ClickHouse: no Spark run, no Grafana or app page.
- **Query cache:** off by default, so a repeated run re-executes its queries (check 11).
- **Screenshots:** `finder.jpg` and `compare.jpg` only (no Spark application, and a run is shorter than one 5 s Prometheus sample).

### 3.6 Comparisons for the report

- **Live settings:** L1 against L2 (the whole naive variant), L3 against L2 (statistics every run alone), per input and worker size.
- **Start-up:** S1 → S2 → S3 per live setting. Expected: L2 hardly changes (no data work), L1 and L3 gain (they read and aggregate check-ins).
- **Input:** I1 against I3 per live setting. Expected: L2 hardly changes; L1 and L3 grow with the check-ins.
- **Single options (block V):** match step and Spark CPU of V1–V4 against their references; V5 against L3.
- **ClickHouse:** H1 → H2 → H3 for each live setting; HL2 against HL1 and HL4 against HL3 (the thread limit); HL3 against HL1 (statistics every run).
- **Spark against ClickHouse at the same CPUs** (S1/H1, S2/H2, S3/H3), with the same statistics mode: Spark L2 against ClickHouse HL1 (and HL2), and Spark L3 against ClickHouse HL3 (and HL4). Totals, and time inside the steps (leaving out Spark's session start-up and the ClickHouse job's Python start).

### 3.7 Expected size of the effects

From the single runs of result-pipeline.md and result-monitoring.md (route A, S2):
- L2 about 26–30 s, of which about 10 s is Spark start-up and 10 s the read step; L3 about 52 s; L1 about 63–67 s.
- The ClickHouse engine about 1.2–2.3 s.
- The grid saved about 25% of the match step's Spark CPU but no wall-clock time. The Python UDF about doubled the match step's summed task time. Block V measures this with repeats; differences under the noise are reported as such.
- At ×1 the largest city has 544,471 check-ins, so ClickHouse's thread setting may show only in the query figures, as in BR5 (risk R5).

### 3.8 Counts

| Block | Cells | Measured runs | Warm-ups |
|---|---|---|---|
| S1, S2, S3 | 27 | 54 (+ thirds where needed) | 3 |
| V (S2) | 14 | 42 | 1 |
| H1, H2, H3 | 36 | 180 | 3 |
| **Total** | 77 | about 280 | 7 |

## 4. Tooling to build

All changes are additive. BR5's files are not changed.

| File | What |
|---|---|
| `jobs/br6_route/route_settings.py` | The setting `clickhouse.limit_threads_by_memory` (bool, default true, the server's behaviour), with BR5's wording. `clickhouse_query_settings` sends `max_threads_min_free_memory_per_thread = 0` when it is false (D10) |
| `jobs/common/run_figures.py` | `max(peak_threads_usage) AS peak_threads` in the ClickHouse figures query, stored as `clickhouse.peak_threads`. BR5 keeps its own copy (`run_metrics.py`), so only BR6 is affected |
| `jobs/br6_route/route_run.py` | The snapshot records `cluster.clickhouse_max_threads`, ClickHouse's default `max_threads` at the start of the run, for both engines |
| `apps/br5/views/br6_run_history.py` | "ClickHouse query time (s)" and "ClickHouse peak threads" in the resource figures |
| `tests/br6/test_import.py` | A settings test: the new setting off sends the memory setting at 0; on sends nothing |
| `tests/br6/benchmark.py` (new) | The driver (sub-commands below). Loads `tests/br5/benchmark.py` with `importlib` under another name (D11) |
| `Makefile` | `br6-bench BLOCK=S2` and `br6-bench-screens BLOCK=S2` (`CELLS=` and `ARGS=` as for BR5) |
| `.gitattributes` | `docs/result/br6/benchmark/**/*.jpg` in Git LFS |
| `docs/guide/br6/config-guide.md` | The two variables in the restart guide, pointing to BR5's entries |

`benchmark.py` sub-commands:
- **`presets`:** saves the presets of 3.3–3.5 and prints each one's settings that differ from the default.
- **`block S1|S2|S3|V`:**
  1. Restarts `spark-worker` with the block's cores if needed (BR5's `set_worker`).
  2. One warm-up run, recorded as `warmup`.
  3. The cells interleaved through `scripts/br6.py run --city … --start … --end … [--stops …] [--visitor …] --preset …`.
  4. A third run where two differ by more than 15% (not in block V, which has 3).
  5. Each run appended to `docs/result/br6/benchmark/runs.csv`: block, cell, input, preset, repeat, run id, status, run time, times, Spark application, check-ins read, places found, suggested.
- **`block H1|H2|H3`:** refuses while a BR5 or BR6 run is in progress, recreates `clickhouse` with the block's CPUs if needed and checks the size at the start and the end (BR5's `set_clickhouse` and `check_clickhouse`); then a warm-up and the 4 × 3 cells, 5 runs each, interleaved.
- **`screens <block>`:** the screenshots of section 6 for every cell of the block.
- **`check <block>`:** the checks of section 8 that can be read from the stored runs (settings used, rows read, same result, threads), printed per cell.
- **`restore`:** worker back to 2 cores, ClickHouse back to 1 CPU.

`--cells I1-L2,I3-L3` limits a block, its screenshots or its checks to some cells.

## 5. Procedure and time

| Step | What | Estimated machine time |
|---|---|---|
| 0 | Build section 4; `make br6-test`; `benchmark.py presets` | – |
| 1 | Smoke test: one run of I1 × L1 on S2 (the heaviest cell: the driver reader brings Istanbul's 544,471 check-ins through the 512 MB driver, risk R2), and one of I1 × HL4 | 2 min |
| 2 | Block S2; its screenshots | 16 min + 5 min |
| 3 | Block S1; its screenshots | 21 min + 5 min |
| 4 | Block S3; its screenshots; `restore` | 14 min + 5 min |
| 5 | Block V; its screenshots | 27 min + 5 min |
| 6 | Blocks H1, H2, H3; their screenshots; `restore` | 15 min + 5 min |
| 7 | `check` for every block; `docs/result/br6/result-benchmark.md`; `make br6-check-dashboards` | 5 min |

About 2 h 10 min in total. Each block is self-contained, so blocks can run in separate sessions. S2 goes first because it needs no restart and holds the reference cells.

**Machine rules during a block:** no Grafana or app page open in a browser, no BR5 job, and no other heavy work on the laptop. The event log stays on.

## 6. Screenshots

Saved under `docs/result/br6/benchmark/<block>-<cell>/`, for example `S2-I1-L2/`, from the cell's first measured run. 1600 px wide, full page, JPEG.

| File | Page | Shows | Taken for |
|---|---|---|---|
| `finder.jpg` | Route Finder `/route_finder?run=<id>`, "Run summary (for the team)" expanded | The suggestions and the run's figures: run time, steps, check-ins read, used and excluded, settings used | Every cell |
| `compare.jpg` | Run History `/route_run_history?a=<reference>&run=<id>` | Run time and steps side by side, the settings that differ, resource figures, "Same result?" | Every cell that has a reference (below) |
| `resources.jpg` | Grafana BR6 Resource use (kiosk), from 2 s before the run to 2 s after | CPU and memory of `spark-worker`, `br5-app` and `clickhouse`, with the run shaded | Every Spark cell |
| `spark.jpg` | Spark history, the application's Jobs page | Jobs named after the steps, with their durations | Every Spark cell |

**References in `compare.jpg`:**
- S blocks: L1 and L3 against L2 of the same input and block; L2 of S1 and S3 against S2's L2. S2's L2 cells are the references.
- Block V: V1, V2 and V3 against L2 of block V; V4 against V2; V5 against L3 of block V.
- H blocks: HL2 and HL3 against HL1 of the same input and block; HL4 against HL3. HL1 of H2 and H3 against H1's HL1. H1's HL1 against S2's L2 (Spark against ClickHouse).

That is about 220 images. As in BR5, the script waits for each page to finish loading and fails loudly instead of saving a half-drawn page.

## 7. Report tables (`result-benchmark.md`)

- **A. Run time per cell:** input × live setting × worker size, median, and the effect of each dimension (as BR5's "Effect of each dimension").
- **B. Where the time goes:** start-up (time outside the steps), read, clean, match, visitor, per cell.
- **C. Work and resources per cell:** Spark CPU, tasks, shuffle, spill, executor and driver peak heap; ClickHouse rows read; container CPU and peak memory of `spark-worker` and `br5-app`.
- **D. Block V:** run time and match step per cell, `spark.cpu_ms@match` and `spark.run_ms@match`, against the reference; V5's read step and driver figures against L3.
- **E. ClickHouse blocks:** run time and time inside the steps per H block and live setting; from `system.query_log` the query time, CPU, peak threads, peak memory and rows read.
- **F. Spark against ClickHouse at the same CPUs**, per statistics mode (3.6).
- **G. Results:** places found and suggested, and the first suggestion, per input: identical across every cell.
- Observations for the report, noise, issues found, the screenshot index, and every run's time in an appendix.

The file is updated after each block, not only at the end.

## 8. Verification

| # | Check | Pass when |
|---|---|---|
| 1 | Each Spark run used its block's worker | The snapshot's `cluster.worker_cores` equals the block's cores in every run |
| 2 | Each run used its preset | `launch.settings_from` is the cell's preset; for L1, Spark reports AQE off and broadcast -1; L3 and V5 runs read the check-ins (the parallel reader's queries add up to the rows read; the driver reader sends one query) |
| 3 | Inputs read what 3.1 says | Check-ins read 544,471 / 412,100 / 1,470 and places near the route 4,574 / 7,178 / 61 in every run |
| 4 | Same input, same result | Every run of an input gives the same list as S2's L2 run of that input (`route_compare.py`, through the script's `check`): every cell, both engines, all statistics modes, all thread settings |
| 5 | All runs succeeded; repeats close | No failed run (or the failure is reported as a finding, R2); each Spark cell's runs within 15% or with a third run |
| 6 | Screenshots complete and readable | One folder per cell with its files; a sample from each block opened and checked against the manifest |
| 7 | Stack restored | Worker back to 2 cores, ClickHouse to 1 CPU (`auto(1)`); `make br6-check-dashboards` and `make br5-check-dashboards` pass |
| 8 | Block V options applied | The logged plans: V1 and V4 have no grid join; V2 and V4 show `BatchEvalPython`; V3 shows `ArrowEvalPython`; V5 sends one read query |
| 9 | ClickHouse start-up applied | At the start and end of each H block, the CPU limit and `max_threads` = `auto(N)`; every run's snapshot has `cluster.clickhouse_max_threads` = `auto(N)` |
| 10 | ClickHouse live settings applied | HL2 and HL4 queries carry `max_threads_min_free_memory_per_thread` = 0 in `system.query_log`; HL3 and HL4 read the city's check-ins (rows read per input as expected) |
| 11 | ClickHouse threads and cache | HL1 and HL3 queries run on 1 thread (BR5's finding), HL2 and HL4 on more than 1 on H2 and H3 where the query is big enough; no query used the query cache |

## 9. Risks

- **R1 Noise.** Laptop load and heat moved BR5's run times by 15–30%. Mitigation: D5, a quiet machine, and the report gives every run, not only the median.
- **R2 The driver reader at ×1 on Istanbul.** L1 brings 544,471 check-ins through the 512 MB driver; 412,100 (Jakarta) worked, 2.06 million (×5) didn't. Step 1 tests this first. If it fails, that is a finding: `bench-baseline` then gets 1 GB of driver memory so the other baseline cells still run, and the plan and result say so.
- **R3 S1 is slow.** The baseline on 1 core may take about 2 min per run.
- **R4 Most of a default run is fixed cost.** About 10 s of Spark start-up and a read step that collects the places through the driver. Worker size and input may then barely change L2. That is a finding, the same as BR5's ("data size matters little with the defaults").
- **R5 ClickHouse effects are small in seconds.** At ×1 the heaviest ClickHouse query aggregates 544,471 rows. The thread limit may show only in the query figures, and maybe only on I1. The report says plainly when totals don't differ.
- **R6 Restarting ClickHouse affects the other services** for a few seconds. The script restarts it only between runs and refuses during a BR5 or BR6 run; `restore` puts it back to 1 CPU.
- **R7 The shared variables keep their BR5 names** (`BR5_WORKER_CORES`, `BR5_CLICKHOUSE_CPUS`). The config guide says that they apply to the shared containers, and so to both BRs.

## 10. Not covered

- Other multipliers (D1): ×5 figures are in result-pipeline.md. Larger scales would need a much longer import and would break the driver reader.
- Import benchmarks: the writers and import steps are in result-data-import.md.
- Shuffle partitions and salting (BR5's block X): a BR6 run has few shuffles with the defaults. The skew case of BR6 (the busiest places in the every-run statistics) is not measured.
- ClickHouse memory sizes, server settings other than the defaults, other table layouts, and the query cache. Several Spark workers. The square size of the grid.
- The Run performance summary screenshots and a `report` sub-command (BR5 didn't build them either; the result file has the tables).

## 11. Decided with the user (2026-11-02)

All four were accepted as proposed:

1. **Data at ×1, no re-import (D1).** BR5 used ×10, but a default BR6 run doesn't read check-ins, so the multiplier wouldn't change its work, and the baseline fails at ×5. Alternative: ×5, with the baseline given 1 GB of driver memory.
2. **The third Spark live setting is "statistics every run" (D3)**, BR6's main optimization isolated, instead of a variant like BR5's 4 read slices (which does nothing in a default BR6 run).
3. **Block V** (the matching method, the distance function and the reader, one at a time, I1 and I2 on S2): 43 runs, about 30 min with its screenshots. It can be dropped if time is short; the single runs of result-pipeline.md would then stand.
4. **ClickHouse live settings as a 2 × 2 (D9):** statistics at import or every run × the memory-per-thread limit on or off. BR5's "1 thread" setting is left out, because in a 1 GB container it equals the default.
