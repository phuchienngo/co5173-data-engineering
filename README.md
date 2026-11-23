# CO5173 Data Engineering: Tourism

Course project for CO5173 Data Engineering (HCMUT). The domain is **tourism**, and the focus is **big data processing optimization**. Each business requirement (BR) is a data pipeline built on the same stack, with a naive baseline and an optimized variant. The variants are benchmarked against each other and against an alternative solution, ClickHouse SQL.

**Stack:** Kafka → Spark 4.0 (PySpark, batch and Structured Streaming) → ClickHouse 26.8 → Grafana and a Streamlit app. Everything runs in Docker Compose.

## Business requirements

| BR | What it does | Mode | Serving |
|---|---|---|---|
| BR3 Customer sentiment | Classifies review texts as positive, neutral, negative or non-English (VADER) and computes satisfaction per location | Stream (Kafka topic `reviews`) | Grafana dashboard "BR3 Data and Results" |
| BR4 Revenue monitoring | Computes revenue per booking, splits bookings into low / medium / high, and shows revenue by location and room type | Stream (Kafka topic `bookings`) | Grafana dashboard "BR4 Data and Results" |
| BR5 Promotion in the low season | From 119,390 hotel bookings: finds each hotel's low-season months and ranks the room type × guest country pairs worth a promotion | Batch | **Promotion Finder** app + Grafana dashboards "BR5 Run performance", "BR5 Resource use", "BR5 Data and result" |
| BR6 Route suggestions | From 2.63 million check-ins in 15 cities (Massive-STEPS): suggests stops along a route from A to B, ranked by popularity, the visitor's taste and the detour | Batch | **Route Finder** app + Grafana dashboards "BR6 Run performance", "BR6 Resource use", "BR6 Data and result" |

BR5 and BR6 are the fully built ones. Each has:
- its own ClickHouse database
- run history with every setting used
- a settings store with presets
- a ClickHouse-only engine as the alternative solution
- monitoring dashboards
- a measured benchmark

The business intent of each BR is in `docs/intent/brN/intent.md`.

## Requirements

- Docker with Compose; give it at least 8 GB of memory.
- Python 3 on the host, to run the launch scripts.
- Git LFS: the raw data (`data/**`) and the benchmark screenshots are stored with it. Run `git lfs pull` after cloning.
- `make` is optional: every target calls a Python script that can be run directly (see below).
- Playwright, only to take the benchmark screenshots.

## Quick start

BR5 and BR6 share one app (`br5-app`, http://localhost:8501).

```bash
make br6-fresh        # BR6: import the check-ins, then start the stack and the app
make br6-up           # later: start again with the stored data
make br5-fresh        # BR5: the same for the hotel bookings
make down             # stop everything; data volumes are kept
```

Without `make`, for example on Windows:

```bash
python scripts/br6.py fresh
python scripts/br6.py up
python scripts/br6.py run --city jakarta --start 1549 --end 1373
```

Streaming demo for BR3 and BR4, which starts Kafka, submits the job and then the producer:

```bash
make demo-br BR=3,4
```

`make help` lists every target with its options: import multiplier, writer, preset, cities, and benchmark blocks.

## Addresses

| What | Address |
|---|---|
| App: Promotion Finder (BR5), Route Finder (BR6), Advanced Settings, Data, Run History | http://localhost:8501 |
| Grafana (admin / admin), folder CO5173 | http://localhost:3000 |
| Spark master UI / Spark history | http://localhost:8082 / http://localhost:18080 |
| Prometheus | http://localhost:9090 |
| ClickHouse (user `default`, password `clickhouse`) | HTTP 8123, native 9000 |
| Kafka | `localhost:9092` from the host, `kafka:29092` from containers |

### Grafana dashboards

All are in the CO5173 folder. BR5 and BR6 each have the same three dashboards:

| Dashboard | Shows |
|---|---|
| **BR3 / BR4 Data and Results** | Processed reviews, positive rate and sentiment distribution (BR3); bookings, revenue and active properties (BR4) |
| **BRn Run performance** | Imports and runs: count, median, fastest and slowest run time, run time over time, the median per value of a chosen setting, and each step of the latest runs |
| **BRn Resource use** | CPU and memory of `spark-worker` (executors), `br5-app` (app and Spark driver) and `clickhouse`, with each run marked; the cores and memory Spark gave to runs; ClickHouse's running queries and memory; CPU seconds and peak memory per container |
| **BRn Data and result** | What is stored: import status and time, multiplier, rows per table (BR6: check-ins per city), rows excluded by the cleaning rules, and the result of the latest run |

BR1 and BR2 run their business queries in Grafana: **BR1 Tour finder** (`/d/br1-finder`) and **BR2 Pricing advisor** (`/d/br2-advisor`). The inputs are dashboard variables, and the panels query ClickHouse directly. See [BR1 Tour finder](docs/br1/tour-route.md) and [BR2 Pricing advisor](docs/br2/dynamic-pricing.md).

The [monitoring guides](docs/guide/br6/monitoring-guide.md) explain each panel and how to read them.

## Repository layout

```
docker-compose.yml        the whole stack (profiles: full, monitoring, br5, br6)
docker/                   Spark image, docker-stats exporter (per-container CPU and memory for Prometheus)
sql/clickhouse/ddl_brN.sql  ClickHouse schema per BR; a BR is discovered from this file
jobs/brN_<name>/          Spark jobs (job.py is the spark-submit target) and the ClickHouse engine
jobs/common/              shared helpers: Spark session, ClickHouse client, parallel writer, run store, settings
ingestion/producers/      Kafka producers for the stream BRs
apps/br5/                 Streamlit app shared by BR5 and BR6
scripts/                  start.py (demo launcher), br5.py and br6.py (up, fresh, import, run, status)
grafana/                  provisioned dashboards (dashboards-br5/, dashboards-br6/ per BR)
tests/brN/                unit tests, dashboard query checks, benchmark drivers
data/brN/                 raw data (Git LFS)
docs/                     documents, see below
```

### Adding a BR

A new BR plugs in by file name:
- `sql/clickhouse/ddl_brN.sql`
- `jobs/brN_<name>/job.py`, with `MODE = "batch"` or `"stream"`
- for a stream BR, `ingestion/producers/produce_brN.py` with `TOPIC = "..."`

`CLAUDE.md` describes the convention and which shared code to reuse.

## Documents

Documents are grouped by type first, then by BR, as `docs/<type>/brN/`:

| Type | Content |
|---|---|
| `intent/` | The business requirement |
| `spec/` | Detailed specification (`spec.md` is the entry point) |
| `plan/` | Implementation plans, one per part: data import, data model, pipeline, monitoring, benchmark |
| `result/` | Measured results of each part's end-to-end test, with the figures for the report |
| `guide/` | How-to guides: configuration and monitoring |

The course requirements are in `docs/DE_Presentation_and_Project_Descriptions_and_Requirements.md`, and the technology choice is in `docs/Main_Technology_Solution.md`.

## Optimizations and findings

Each Spark pipeline has a naive baseline and an optimized default. The settings can be changed per run in the app or through presets. They cover:
- the reader: parallel reads from ClickHouse, or one read through the driver
- native expressions, `pandas_udf` or Python UDFs
- grid matching (BR6)
- place statistics computed at import (BR6)
- AQE and skew handling, broadcast joins, shuffle partitions, salting
- the ClickHouse writer: one insert per partition, or `toPandas()`

The benchmarks run every combination of input, live setting and cluster size, plus ClickHouse at 1, 2 and 4 CPUs. Every run is checked to give the same result.

- **BR5** ([result-benchmark.md](docs/result/br5/result-benchmark.md)): the live settings matter far more than the input or the cluster size. On the same CPU, the ClickHouse engine is 14–20× faster than Spark's defaults.
- **BR6** ([result-benchmark.md](docs/result/br6/result-benchmark.md)):
  - Computing the place statistics at import makes a run about 40% faster and independent of the data size.
  - Going from 1 to 2 Spark cores pays off; going from 2 to 4 hardly does.
  - ClickHouse returns the same suggestions in about 2 s, against 27–44 s for Spark, whose start-up alone takes 10–14 s.

## Tests

The stack must be running (`make br5-up` or `make br6-up`).

```bash
make br5-test                 # unit tests (run inside the app container)
make br6-test
make br6-check-dashboards     # runs every Grafana panel query and reports errors
```

## Running the benchmarks

The benchmark of each BR is a matrix of **cells**. A cell is an input × a live setting, run on a given container size. The cells are grouped into **blocks**, one block per container size. A driver script runs a block from the host:

- it resizes the container
- it runs one warm-up
- it runs the cells interleaved, and appends every run to `docs/result/brN/benchmark/runs.csv`

The plans (`docs/plan/brN/plan-benchmark.md`) define every cell, and the results (`docs/result/brN/result-benchmark.md`) hold the measured figures.

### Configurations

**Container sizes** are fixed when a container starts, so they change per block. The driver sets them through two variables in `docker-compose.yml`:

| Variable | Container | Default | Blocks |
|---|---|---|---|
| `BR5_WORKER_CORES` | `spark-worker`: cores for the Spark executor | 2 | S1 = 1, S2 = 2, S3 = 4 |
| `BR5_CLICKHOUSE_CPUS` | `clickhouse`: CPUs | 1 | H1 = 1, H2 = 2, H3 = 4 |

**Live settings** change per run, through presets the driver saves in the settings store. They then appear in the app and in Grafana's "settings from" filter. Each block's live settings, BR5 first, then BR6:

| Block | BR5 | BR6 |
|---|---|---|
| S1–S3 (Spark) | L1 baseline, L2 default, L3 4 read slices | L1 baseline, L2 default, L3 place statistics counted every run |
| X / V (BR5 / BR6) | Baseline with 8 shuffle partitions, baseline with salting (4 cores) | One option at a time on 2 cores: match every place, Python UDF, `pandas_udf`, both, driver reader |
| H1–H3 (ClickHouse engine) | Default, 1 thread, memory-per-thread limit off | Default and limit off, each with statistics from the import or every run |

**Inputs:**
- BR5: all hotels; City Hotel × Portugal; Resort Hotel × UK × room A.
- BR6: Istanbul with a stop and a chosen visitor; Jakarta; Beijing.

### Commands

`make` targets call `tests/brN/benchmark.py`, which you can also run directly with `python`:

```bash
python tests/br6/benchmark.py presets            # save the benchmark presets (once)
python tests/br6/benchmark.py block S2           # run a block: S1, S2, S3, V, H1, H2, H3
python tests/br6/benchmark.py check S2           # check the stored runs of a block
python tests/br6/benchmark.py screens S2         # screenshots of the block's cells (needs Playwright)
python tests/br6/benchmark.py restore            # worker back to 2 cores, ClickHouse to 1 CPU
```

BR5's driver (`tests/br5/benchmark.py`, blocks S1, S2, S3, X, H1, H2, H3) has the same commands. Its `stages` command takes the place of `check`: it reads each run's Spark stages, to show skew.

The same through `make`:

```bash
make br6-bench BLOCK=S2
make br6-bench BLOCK=H1 CELLS=I1-HL1,I3-HL4
make br6-bench-screens BLOCK=S2
```

| Option | Meaning |
|---|---|
| `--cells I1-L2,I3-L3` (`make`: `CELLS=`) | Run, check or screenshot only these cells |
| `--repeats N` (`make`: `ARGS="--repeats N"`) | Measured runs per cell. The defaults are 2 for the Spark blocks, with a third run when the two differ by more than 15%; 3 for BR5 X and BR6 V; and 5 for the H blocks |
| `--no-warmup` | Skip the warm-up run |
| `--clickhouse` (BR5 only) | Also run the ClickHouse engine cells in a Spark block |

The `check` command reads the stored runs of a block and checks, for each run:
- the preset and container size used
- the rows read
- that the result is the same as the reference run's
- for the H blocks, ClickHouse's threads

The screenshots go to `docs/result/brN/benchmark/<block>-<cell>/`, stored with Git LFS:
- the result page
- a comparison with the reference run
- Grafana's resource use
- the Spark jobs

**While a block runs**, keep other heavy work off the machine and the app and Grafana pages closed. The figures are timings.
