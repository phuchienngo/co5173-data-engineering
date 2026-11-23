# BR7 + BR8 Plan: Benchmark Test

Measures how the BR7 (tourist arrival alerts) and BR8 (tourist retention) batch analysis performs across inputs, start-up configurations and live settings, and compares Spark with the alternative solution, a ClickHouse SQL engine.

**It copies BR5/BR6's benchmark as closely as possible** ([BR5 plan](../br5/plan-benchmark.md), [BR6 plan](../br6/plan-benchmark.md), `benchmark-overview.pdf`): data imported once into ClickHouse, a run that reads it with a parallel or driver reader, per-BR `pipeline_runs` and `settings` tables, presets, the same matrix, block procedure, checks and screenshots, and a driver that loads `tests/br5/benchmark.py` for the generic parts.

**The one deliberate difference: no Streamlit app.** Everything the app does for the benchmark is done by a CLI launcher (`scripts/brN.py`, like `scripts/br6.py`) and by Grafana dashboards (section 6).

**Status:** 2026-11-07. All open points decided (section 11). BR8's tooling is built (section 12); BR7 follows on the same code. The analysis is always BR7's/BR8's own prepare logic; BR5/BR6's benchmark is the template for the structure only.

## 1. What is asked

| # | Ask | How this plan covers it |
|---|---|---|
| 1 | 2–3 inputs that read different amounts of data | BR7: 13 / 7 / 3 months of taxi trips (50.7 M → 10.7 M rows). BR8: all destinations / Tokyo / Kuala Lumpur (2.76 M → 27.5 k check-ins) (3.1) |
| 2 | 2–3 start-up configurations | Spark worker 1/2/4 cores through `BR5_WORKER_CORES` (3.2) |
| 3 | 2–3 live settings | L1 baseline, L2 default, L3 one change, as presets in `brN.settings` (3.3) |
| 4 | ClickHouse as the alternative, with its own start-up configurations and live settings | H1–H3 through `BR5_CLICKHOUSE_CPUS` × HL1–HL2 thread settings (3.5) |
| 5 | Single-option comparisons | Block V on S2 (3.4) |
| 6 | Same method as BR5/BR6, Grafana instead of the app | Sections 4–7 |

## 2. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **BR7 and BR8 get BR5/BR6's structure: an import and a run.** The import (Spark, once) loads the source files into ClickHouse (`br7.raw_trip`, `br8.raw_checkin`, `br8.place`, built in `brN_staging` and swapped, as BR5). A run (Spark or ClickHouse engine) reads those tables, does the BR's processing and writes its result tables in ClickHouse. Only runs are benchmarked. | Same as BR5/BR6 (O3). The existing `prepare_br7.py` / `prepare_br8.py` → CSV → Kafka path stays for `make demo-br`. |
| D2 | **What a run computes is exactly today's prepare logic.** BR7: quality labels, tourist-zone filter, dedup, zero-filled hourly baseline (mean, p90), replay counts per zone. BR8: attraction filter, daily visits, visitor/place pairs, returning pairs. Same rules, same constants (`br7_rules`, `br8_rules`). | The benchmark measures the real BR, and the answer can be checked against the prepare summary already produced (`summary.json`). |
| D3 | **The import stores only the columns a run needs, typed**, with ORDER BY chosen for the run's filters (BR7 by `source_month`, BR8 by `region_id`). | BR7's 50.7 M rows must fit ClickHouse's 1 GB container during insert and stay small on disk. Ordering lets the input dimension really change the rows read. |
| D4 | **Start-up = Spark worker cores 1/2/4 (`BR5_WORKER_CORES`), ClickHouse CPUs 1/2/4 (`BR5_CLICKHOUSE_CPUS`).** Every Spark preset sets `spark.cores.max=4`. | BR5's D2–D4 and D15; the variables already exist in `docker-compose.yml`. |
| D5 | **Per-BR databases `br7` and `br8`** with `pipeline_runs`, `settings` (current values + presets) and the result tables, built with `jobs/common/run_store.py`, `spark_settings.py`, `run_figures.py` (CLAUDE.md: "reuse for a batch BR with its own database"). | Same tables and columns as `br5`/`br6`, so the same queries, checks and dashboard patterns work. |
| D6 | **Launchers `scripts/br7.py` and `scripts/br8.py`** with `up`, `import`, `run [--input I1] [--preset X]`, `status`, as `scripts/br6.py`. They replace the app's buttons. Runs are started in `spark-local` through `jobs/common/job_launch.py`. | The benchmark driver calls the launcher exactly like BR6's driver calls `br6.py run --preset`. |
| D7 | **Result check as BR5/BR6:** each run stores a fingerprint of its result tables (row count + `groupBitXor(cityHash64(*))`) in `pipeline_runs`; `check` compares every run with the reference (S2 L2 of the same input). A different answer is a failed run. | "Same result or it doesn't count" (PDF page 4). |
| D8 | **2 measured runs per Spark cell (+1 if > 15% apart), 3 in block V, 5 per ClickHouse cell. One warm-up after each restart. Cells interleaved.** Median reported. | BR5/BR6's D5/D8. |
| D9 | **Drivers `tests/br7/benchmark.py` and `tests/br8/benchmark.py`** load `tests/br5/benchmark.py` with `importlib` for restarts, size checks, history server and Playwright. BR5/BR6 files are not changed. | BR6's D11. |
| D10 | **Memory rule:** during a block only ClickHouse, Spark master/worker, `spark-local`, Prometheus, `docker-stats`, `spark-history` run. Kafka, Grafana and stream jobs are stopped; Grafana runs only for screenshots after the block. | WSL has 7.6 GB RAM. |

## 3. The matrix

### 3.1 Inputs (the run's choice)

**BR7** (the 10 tourist zones; last month = replay month, the others train the baseline). One import of all 13 months; the input picks the months.

| ID | Months | Rows read | Why |
|---|---|---|---|
| I1 | 2025-09 → 2026-09 (12 + 1) | 50,745,563 | The full year, the default |
| I2 | 2026-03 → 2026-09 (6 + 1) | 26,454,644 | Half |
| I3 | 2026-07 → 2026-09 (2 + 1) | 10,742,869 | Smallest that keeps ≥ 8 training hours per baseline cell |

**BR8** (`max_places = 0`). One import of all travel check-ins (exploded, 2,764,448 rows) and places (687,173).

| ID | Region | Check-ins read | Daily visits | Why |
|---|---|---|---|---|
| I1 | ALL | 2,764,448 | 107,912 | The default |
| I2 | Q1490 Tokyo | 164,969 | 5,416 | Biggest region |
| I3 | Q1865 Kuala Lumpur | 27,544 | (measured) | The fixed cost |

### 3.2 Start-up configurations

| ID | `BR5_WORKER_CORES` | What a run gets | Restart |
|---|---|---|---|
| S1 | 1 | 1 executor × 1 core | `spark-worker`, by the script |
| S2 | unset (2) | 1 executor × 2 cores | – |
| S3 | 4 | 1 executor × 4 cores | `spark-worker` |

Worker 2G, executor 512m + overhead, driver 512m (BR5/BR6 values; see O5 for BR7).

### 3.3 Spark live settings (presets in `brN.settings`)

| Setting | What it controls | L1 `bench-baseline` | L2 `bench-default` | L3 `bench-read-4` |
|---|---|---|---|---|
| `spark.cores.max` | most cores per run | **4** | **4** | **4** |
| `pipeline.reader` | `parallel`: each task reads its own slice from ClickHouse as Arrow. `driver`: one query on the driver | **driver** | parallel | parallel |
| `pipeline.read_partitions` | slices for the parallel reader | 2 | 2 | **4** |
| `pipeline.udf` | BR7 quality label + hour of week, BR8 attraction regex: `native`, `pandas`, `python` | **python** | native | native |
| `pipeline.cache` | keep the cleaned rows reused by several outputs | **false** | true | true |
| `spark.sql.adaptive.enabled` | AQE | **false** | true | true |
| `spark.sql.autoBroadcastJoinThreshold` | BR7 zone grid, BR8 places join | **-1** | 10m | 10m |
| `pipeline.writer` | result writer: `partitions` (`write_partitions`) or `pandas` (`toPandas` → `insert_df`, the naive baseline) | **pandas** | partitions | partitions |

L3 = BR5's third setting (4 read slices): one change, the reader's parallelism. **Cells:** 3 inputs × 3 presets × S1/S2/S3 = 27 per BR, 2 runs each.

### 3.4 Block V: single options (S2, inputs I1 and I2, 3 runs)

| ID | Change from L2 | Shows |
|---|---|---|
| L2 | – | reference |
| V1 | `pipeline.udf=python` | row Python UDF vs native |
| V2 | `pipeline.udf=pandas` | vectorized `pandas_udf` vs native |
| V3 | `pipeline.cache=false` | recomputing the cleaned rows |
| V4 | `spark.sql.autoBroadcastJoinThreshold=-1` | broadcast vs sort-merge |
| V5 | `spark.sql.adaptive.enabled=false` | AQE |
| V6 | `pipeline.writer=pandas` | naive writer vs parallel writer |

2 inputs × 7 cells × 3 runs = 42 per BR.

### 3.5 ClickHouse engine

A Python job in `spark-local` runs the BR's processing as SQL on the imported tables and writes the same result tables. No Spark application.

| ID | `BR5_CLICKHOUSE_CPUS` | Restart |
|---|---|---|
| H1 | unset (1) | – |
| H2 | 2 | `clickhouse` |
| H3 | 4 | `clickhouse` |

| ID | Preset | Threads |
|---|---|---|
| HL1 | `bench-clickhouse` | server default (1 thread in a 1 GB container, BR5's finding) |
| HL2 | `bench-clickhouse-nomemlimit` | `max_threads_min_free_memory_per_thread = 0` (`clickhouse.limit_threads_by_memory=false`) |

3 inputs × 2 × H1–H3 = 18 cells per BR, 5 runs each. Figures from `system.query_log`, queries tagged with the run id.

### 3.6 Counts and time

| Block | Runs per BR | BR7 time | BR8 time |
|---|---|---|---|
| S1, S2, S3 | 54 + thirds + 3 warm-ups | ≈ 2.5 h | ≈ 45 min |
| V | 42 + 1 | ≈ 1 h | ≈ 20 min |
| H1–H3 | 90 + 3 | ≈ 40 min | ≈ 10 min |
| Import (once, not timed in the matrix) | 1 | ≈ 5 min | ≈ 2 min |
| **Total** | ≈ 190 per BR | ≈ 4 h | ≈ 1.3 h |

Estimates from the first real prepare (BR7 1 min 47 s on 2 cores from parquet); a run from ClickHouse adds the read step. Measured in the smoke test (section 5).

## 4. Tooling to build

All additive; BR1–BR6 untouched.

| File | What |
|---|---|
| `sql/clickhouse/ddl_br7.sql`, `ddl_br8.sql` | Append `br7`/`br8` databases: raw tables, result tables (keyed by `run_id`), `pipeline_runs`, `settings`, Grafana views (as `ddl_br6.sql`) |
| `jobs/br7_tourist_arrival_alerts/bench_import.py`, `bench_run.py`, `bench_spark.py`, `bench_sql.py`, `bench_settings.py` | Import, run entry, Spark engine, ClickHouse engine, BR7 settings catalog on `spark_settings.py`. Module names prefixed so they don't clash on one Python path |
| `jobs/br8_tourist_retention/…` | The same for BR8 (`br8_bench_*` names) |
| `scripts/br7.py`, `scripts/br8.py` | Launchers (D6) |
| `tests/br7/benchmark.py`, `tests/br8/benchmark.py` | Drivers: `presets`, `block`, `check`, `screens`, `restore`; `--cells`, `--repeats`, `--no-warmup` |
| `tests/br7/test_bench.py`, `tests/br8/test_bench.py` | Presets, settings → Spark conf, fingerprint SQL; Spark and SQL engines agree on a tiny fixture |
| `grafana/dashboards/br7-benchmark.json`, `br8-benchmark.json` | Section 6 |
| `Makefile` | `br7-up`, `br7-import`, `br7-run`, `br7-status`, `br7-bench BLOCK=`, `br7-bench-screens`; same for BR8 |
| `.gitattributes` | `docs/result/br7/benchmark/**/*.jpg`, `docs/result/br8/benchmark/**/*.jpg` in LFS |

## 5. Procedure

Step 0: build section 4; unit tests; `brN.py import`; `benchmark.py presets`.
Step 1: smoke test: the heaviest cell (I1 × L1 on S2) and I1 × HL2, plus a fingerprint match between Spark L2 and ClickHouse HL1 and the prepare `summary.json`.
Then per block (as the PDF, page 4): restart at the block's size → warm-up → round 1 → round 2 → third runs → size check → `check`. Order: S2, S1, S3, V, H1, H2, H3, then `restore`.

## 6. Grafana instead of the app

| App page (BR5/BR6) | Replaced by |
|---|---|
| Finder (choose input, run, read result) | `brN.py run --input I1 --preset X` + Grafana "Run result" panel (result tables filtered by run id) |
| Advanced Settings (edit settings, presets) | Presets saved by `benchmark.py presets`; current values via `brN.py settings set/load`; Grafana "Settings" table (read-only) |
| Data (import, status) | `brN.py import` / `brN.py status`; Grafana "Data" stat panels |
| Run History (list, two runs side by side) | Grafana dashboard `brN-benchmark`: run list, compare `run_a` vs `run_b` (time, steps, settings that differ, Spark and ClickHouse figures, same result ✓/✗) |

Dashboard rows: Overview (median per worker size × Spark baseline/default/ClickHouse, as PDF page 7) · Matrix (median, min, max, runs, ✓/✗ per cell) · Where the time goes (steps + outside steps) · Compare two runs · Resources (Prometheus `docker-stats`) · ClickHouse queries · Run list.

## 7. Screenshots

`docs/result/brN/benchmark/<block>-<cell>/`, first measured run, Playwright 1600 px JPEG: `result.jpg` (Grafana run result), `compare.jpg` (Grafana compare vs reference), `resources.jpg` (Spark cells), `spark.jpg` (Spark history Jobs page, Spark cells).

## 8. Report (`docs/result/brN/result-benchmark.md`)

As BR6 section 7: A run time per cell and effect of each dimension; B where the time goes; C work and resources; D block V; E ClickHouse blocks; F Spark vs ClickHouse at the same CPUs; G identical results; appendix of every run.

## 9. Risks

- **R1 BR7 driver reader (L1) on 50.7 M rows** will not fit a 512 MB driver (BR6 hit this at ×5 with far fewer rows). See O5.
- **R2 BR7 import into a 1 GB ClickHouse.** Mitigation D3 (needed columns only), `write_partitions` in bounded batches.
- **R3 Memory (7.6 GB WSL).** D10.
- **R4 Spark and ClickHouse p90.** Spark `percentile` and ClickHouse quantile must match after rounding; checked in the smoke test.
- **R5 BR7 L1 on S1** may run 10+ min. Kept; shows the effect most strongly.
- **R6 Noise.** 15% rule, interleaving, quiet machine.

## 10. Not covered

The Kafka stream (O2); data multipliers (BR7 already 50.7 M rows); a second Spark worker.

## 11. Open points

| # | Question | Proposal |
|---|---|---|
| O1 | Merge `main` first? | **Decided: yes** (2026-11-07) |
| O2 | Stream benchmark? | **Decided: batch only** |
| O3 | ClickHouse reads parquet, or pre-loaded tables? | **Decided: pre-loaded, as BR5/BR6** |
| O4 | One plan for both BRs? | **Decided: yes** (dashboards, drivers and databases stay per BR, as BR5/BR6) |
| O5 | BR7 L1 driver reader on I1/I2 (R1) | **Decided:** `reader=driver` in L1 as BR5/BR6, `bench-baseline` gets 2 GB driver memory; an out-of-memory failure is reported as a finding |
| O6 | Effort | **Decided:** BR8 first, then BR7 on the same code |

## 12. As built: BR8 (2026-11-07)

| File | What |
|---|---|
| `sql/clickhouse/ddl_br8.sql` (appended) | `br8.region`, `br8.place`, `br8.raw_checkin`, `br8.pipeline_runs`, `br8.settings`, `br8.run_visit`, `br8.run_place`, view `br8.run_history`. No `fact_` names, so `start.py` never truncates them; the stream tables are unchanged |
| `jobs/br8_tourist_retention/br8_bench_settings.py` | Catalog on `spark_settings.py`: `pipeline.udf` (native / pandas / python), `cache`, `reader`, `read_partitions`, `writer`, `engine`, ClickHouse thread settings, `rules.max_places` (0). Built-in `baseline` = section 3.3 L1 |
| `br8_bench_runs.py` | Names on `run_store.py`, inputs, region filter, busy check over BR5–BR8, result fingerprint and summary |
| `br8_bench_import.py` | Spark: the three TripWorld files `prepare_br8.py` downloads → `br8_staging` → checked (row counts, `visit_time` range, unique place ids) → swapped |
| `br8_bench_spark.py`, `br8_bench_sql.py` | The two engines; steps `read`, `clean`, `match`, `aggregate`, `write`, same meaning as `prepare_br8.py` `build()` |
| `br8_bench_run.py`, `br8_bench_launch.py`, `br8_bench_status.py` | Run entry (records `pipeline_runs`, figures, fingerprint after `finish`), launcher in `spark-local`, status |
| `scripts/br8.py` | `up`, `import`, `run --input I1|I2|I3 [--preset] [--tag]`, `status`, `settings` |
| `tests/br8/benchmark.py` | Driver: `presets`, `block`, `check`, `screens`, `restore`; `EXPECTED` rows read and fingerprints per input |
| `tests/br8/test_bench.py` | 31 unit tests, no Docker (`make br8-test`) |
| `grafana/dashboards/br8-benchmark.json`, `br8-benchmark-run.json` | Overview (medians per block, matrix, steps, Spark and ClickHouse figures, every run) and one run against a reference (result, steps, settings that differ, figures, CPU/memory) |
| `Makefile`, `.gitattributes` | `br8-*` targets; BR8 screenshots in LFS |

Deviations from sections 3–7:
- **The places join is always sort-merge** (`hint("merge")`). Rows read through `mapInArrow` have no size estimate, so Spark would otherwise broadcast all 687,173 places through the 512 MB driver; `prepare_br8.py` on parquet plans a sort-merge join too. V4 (broadcast off) therefore only changes the small joins (regions, per-place figures).
- **Screenshots:** `run.jpg` (Grafana run dashboard: result, compare with the reference, resources) and `spark.jpg`, instead of separate `result/compare/resources` images.
- **The ClickHouse engine's read step only builds the queries** (as BR6's); its `aggregate` step writes `run_visit` and `write` writes `run_place`.

Verified before any Docker run (embedded ClickHouse 26.9 on the real TripWorld files):
- The ClickHouse engine's rows equal an independent pandas version of `prepare_br8.py` row by row (visits with sha256 event ids, per-place visitors / returning / visits, region, category, city) for Tokyo, Tokyo top 20 (2,995 visits) and Kuala Lumpur; on all data it gives 2,764,448 check-ins read, 108,262 used, **107,912 visits, 10,639 places**, the figures of `prepare_br8.py`.
- Every Grafana panel query runs on the DDL.
- On the Docker stack (docs/result/br8/result-benchmark-smoke.md): the import (62 s) and Spark and ClickHouse runs on Tokyo and all data give the expected fingerprints; the baseline is 2.9× slower than the default on all data.

Changes after the smoke test:
- The import no longer caches and repartitions on the slice key (a round-robin repartition ran the 512 MB executor out of memory).
- `br8.run_history` carries readable names (`what`, `input_name`, `variant`, `hardware`, `label`) and `correct` (✓ when the fingerprint equals the expected one of the input; the same constants as the driver's `EXPECTED`, kept equal by a unit test). The dashboards show only these: status counters, latest runs (click to open), Spark against ClickHouse, cells, steps, work done; the run page has drop-downs instead of run ids.

Benchmark run (2026-11-08): all BR8 blocks done, 188 measured runs, every one with the expected fingerprint; figures in docs/result/br8/result-benchmark.md. The first S1 attempt is void (the laptop slept mid-run; its runs are tagged `S1-void-sleep`). For BR7: keep Windows awake during blocks, and with a 512 MB executor expect occasional executor restarts in the cached clean step (BR8 I1 on 2–4 cores: `OutOfMemoryError`, the task is retried, the run succeeds but is slower); check the Spark logs of every run for `Lost executor`.

## 13. As built: BR7 (2026-11-08)

Same structure as BR8 (section 12), files `jobs/br7_tourist_arrival_alerts/br7_bench_*.py`, `scripts/br7.py`, `tests/br7/`, `grafana/dashboards/br7-benchmark*.json`, `make br7-*`, tables appended to `sql/clickhouse/ddl_br7.sql` (database `br7`; the stream tables are unchanged).

| File | What |
|---|---|
| `br7_bench_import.py` | The 13 files read with `prepare.py`'s own `load_month()` (same columns and casts, quality label dropped) → `br7.raw_trip` (50,745,563 rows, ORDER BY `source_month, source_row`); `taxi_zone_lookup.csv` → `br7.zone`. Checked per month: rows, drop-off range, a pick-up checksum |
| `br7_bench_spark.py` | `prepare.py main()`'s steps on the imported trips, `prepare.py`'s helpers imported (`month_bounds`, `local_hour_of_week`, limits): `quality` (every trip labelled, counts per month and label), `clean` (tourist zones, label ok, New York → UTC, `dropDuplicates` on the natural key, sha256 event id), `baseline` (zero-filled hours, mean, p90, ≥ 8 hours), `replay`, `write` |
| `br7_bench_sql.py` | The same steps as ClickHouse SQL; follows Spark where ClickHouse differs (to_utc_timestamp in the DST gap and overlap, Java's double-to-string in the event id, concat_ws skipping nulls) |
| `br7_bench_quality.py` | The quality rule for the Python UDF, stdlib only (unit-tested against `prepare.py`'s labels and limits) |
| Result tables | `run_quality` (summary.json `rows_by_quality`), `run_month` (trips per month before/after duplicates), `run_baseline` (prepared/baseline), `run_replay` (prepared/replay), `run_zone` (prepared/zones + replay counts) |

Deviations from sections 3–7:
- **No driver reader (R1 decided):** 50.7 M trips as one Arrow table on the driver need ~3 GB outside the JVM heap; on the 7.4 GB WSL host that crashes the machine, not just the run. The baseline reads with `pipeline.reader=single`: one connection and one partition, as Spark's JDBC reader without partitioning options. The parallel reader is BR7's own streamed copy of `clickhouse_read.read_parallel` (a 25 M-row slice can't be one Arrow table either).
- **`pipeline.udf` is the quality label** (50.7 M rows); the hour of week is computed on 87,600 grid rows only and stays native.
- **`pipeline.cache` caches the deduplicated trips**, as `prepare.py` does; the labelled trips are never cached (as `prepare.py`: a year of trips doesn't fit).
- **The replay's `passenger_count` is not in the fingerprint.** In the TLC files 7,608 duplicate groups (same natural key) disagree on `passenger_count`; `dropDuplicates` keeps any one row, so this column can change between two runs of `prepare.py` itself. Everything else (event ids, zones, times, counts, baseline values) is compared.
- **Results are checked row by row against `prepare.py`'s own output** (`data/br7/prepared`, loaded into a scratch database `br7_ref`), not only by counts.

Benchmark run (2026-11-08 to 2026-11-09): all BR7 blocks done, 186 measured runs, every one with the expected fingerprint; figures in docs/result/br7/result-benchmark.md. One run (block V, I2-V1, Python UDF) ran the 512 MB executor out of memory in the deduplication's sort and was run again; its row is in `docs/result/br7/benchmark/runs-failed.csv`. The chain was paused between S1 and S3 (laptop heat), at a block boundary.
