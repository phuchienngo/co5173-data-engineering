# CLAUDE.md

## Project context

- Course project for CO5173 Data Engineering (HCMUT). Domain: **Tourism**. Presentation topic, which is also the focus of the implementation: **Big Data Processing Optimization**.
- Requirements and grading: `docs/DE_Presentation_and_Project_Descriptions_and_Requirements.md`. Each business requirement (BR) is graded on data correctness, DE performance, and how well the app lets users exploit the data. Alternative solutions are benchmarked against the main one.
- **Per-BR documents live in `docs/brN/`, one folder per business requirement (BR).** Every document about a BR goes in its own folder with a fixed file name, for example:
  - `docs/brN/intent.md`: the business requirement / application intent. Read it whenever you need business context for a BR.
  - Later documents (spec, plan, ...) follow the same rule: `docs/brN/spec.md`, `docs/brN/plan.md`, and so on.
  - Project-wide documents (course requirements, tech solution) stay directly in `docs/`.
- Raw data for each BR: `data/brN/`, stored with Git LFS (`data/**`).
- Tech solution (`docs/Main_Technology_Solution.md`): Kafka → Spark (PySpark, batch and Structured Streaming) → ClickHouse → Grafana or a web app.

## Stack (`docker-compose.yml`)

- **Kafka** `cp-kafka:8.3.1`, KRaft mode. Brokers: `localhost:9092` from the host, `kafka:29092` from containers.
- **Spark** 4.0 (`bitnamilegacy/spark:4.0`) standalone cluster: 1 master and 1 worker (2 cores, 2G). Jobs are submitted from the `spark-local` container (client mode), which mounts `/opt/jobs`, `/opt/data`, `/opt/ingestion` and `/opt/logs`.
- **ClickHouse** `26.8`, login `default` / `clickhouse`, ports 8123 and 9000.
- **Prometheus** scrapes ClickHouse and Spark.
- **Grafana** `13.2` at `:3000` (admin/admin). Dashboards are provisioned from `grafana/dashboards/*.json`.
- Run everything with `make demo` or `make demo-br BR=3,4`, which call `scripts/start.py`. Logs go to `logs/demo/brN-*.log`. The Makefile and `start.py` use `python3`, `bash` and `tail`; the dev machine is Windows.

## BR plug-in convention (discovered by filename)

```
sql/clickhouse/ddl_brN.sql            the BR is discovered from this file; start.py truncates its fact_* tables before each run
jobs/brN_<name>/job.py (+ helpers)    spark-submit target; jobs/common/*.py and sibling .py files are shipped as --py-files
ingestion/producers/produce_brN.py    stream BRs only; must define TOPIC = "..." (scripts/topics.py reads it with ast)
```

### Job mode

Declare the mode with a top-level constant in `job.py`. `start.py` reads it with `ast` and does not import the job.

```python
MODE = "batch"   # or "stream" (the default when MODE is absent)
```

- **`stream`**: `start.py` submits the job and waits for `ResolveWriteToStream` in the log. It then starts `produce_brN.py`, and the job keeps running until Ctrl+C.
- **`batch`**: `start.py` submits the job and waits for it to exit. It prints the end of the log and fails if the exit code is non-zero. No producer is started, so the job reads its input from `/opt/data/brN/` itself.
- When batch and stream BRs are selected together, all batch jobs run first, one at a time, and then the stream jobs start.

## Guidance for new BRs: reuse vs. write our own

**Reuse as-is:**
- Compose stack and Dockerfile
- BR plug-in convention and job modes
- `scripts/topics.py`
- `jobs/common` helpers: `get_client`, `get_producer`, `get_spark_session`
- Grafana provisioning (add a new dashboard JSON per BR)

**Extend shared code** (`scripts/`, `jobs/common/`, compose). **Keep changes additive and don't break BR3/BR4.** Planned extensions:
- A parallel ClickHouse writer (JDBC/Spark connector) next to the current `toPandas()` → `insert_df` writer. **Keep `toPandas` as the naive baseline for benchmarks.**
- A configurable Kafka partition count.
- A common producer helper with a message key, typed fields and an event time.
- Rejected-row counting instead of dropping rows silently. The shared session sets `spark.sql.ansi.enabled=false`, so a bad `.cast()` returns null.

**Write our own for each BR:**
- Job logic, DDL and dashboard.
- ClickHouse tables with real keys and event time: `ReplacingMergeTree`, PARTITION BY / ORDER BY chosen for the queries, pre-aggregated tables where useful.
- `checkpointLocation` for any stream job, so a restart doesn't duplicate rows.
- Evaluation output: a `pipeline_runs` table (BR, variant, rows in/out/rejected, duration) plus Grafana panels backed by Prometheus.

**Patterns to copy:** broadcast joins with small dimension tables, `explain()` output in the logs (for the report), and `maxOffsetsPerTrigger` for backpressure.

**Optimization focus:** every BR should show a baseline vs. optimized variant with measured results: native expressions or `pandas_udf` vs. Python UDFs, partitioning and caching, AQE and skew handling, Parquet staging, writer parallelism.
