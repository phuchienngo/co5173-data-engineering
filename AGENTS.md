# AGENTS.md

Rulebook for adding or changing a business requirement (BR) in this repo. Follow the existing pattern; do not invent a new one.

## 1. Purpose & stack

CO5173 data-engineering course project. Each BR is a streaming pipeline: a Python producer replays CSV/parquet rows into Kafka (KRaft, `cp-kafka:8.3.1`), a Spark 4 Structured Streaming job (standalone cluster, image `co5173-spark-demo:local` built from `docker/spark/Dockerfile` on `bitnamilegacy/spark:4.0`) consumes them and writes serving tables to ClickHouse (`clickhouse-server:26.8`), and Grafana (`grafana:13.2`) + Prometheus (`prometheus:v3.14.0`) plus an nginx tourist map (`tour-map`) read the results. Everything runs under `docker-compose.yml`, driven by `scripts/start.py` through `make`. Python deps are pinned in `requirements.txt`; the Spark runtime itself comes from the base image (pyspark lines are stripped at image build). Data files under `data/` are tracked with Git LFS (`.gitattributes`).

## 2. Repo layout

```
Makefile                      make demo / make demo-br BR=N / make logs / make down
docker-compose.yml            services + profiles: full (Spark), monitoring (Prometheus, Grafana), ui (kafka-ui), tunnel
docker/spark/Dockerfile       spark image; pip installs requirements.txt minus pyspark
requirements.txt              pinned Python deps (clickhouse-connect, kafka-python, pandas, pyarrow, pyspark, ...)
scripts/start.py              orchestrator: infra up -> topics -> DDL -> spark-submit per BR -> producers
scripts/topics.py             creates Kafka topics from TOPIC constants in ingestion/producers/produce_br*.py
scripts/download_tlc.sh       downloads NYC TLC parquet + zone lookup into data/br2/raw
scripts/fetch_osm_landmarks.py  writes data/br1/landmarks.csv (--out, --cities)
jobs/common/                  shared helpers, shipped to Spark via --py-files and to producers via PYTHONPATH
  spark_session.py            get_spark_session(app_name)
  kafka_io.py                 get_producer() (JSON value serializer, KAFKA_BOOTSTRAP_SERVERS, default localhost:9092)
  clickhouse_io.py            get_client(), write_dataframe(df, table), truncate_table(table), read_query(query)
jobs/brN_<name>/job.py        the Spark job (exactly one per BR)
jobs/brN_<name>/*.py          sibling helper modules, shipped with the job (e.g. br2 pricing.py, br4 bucket.py)
ingestion/producers/produce_brN.py   Kafka producer for BR N; module-level TOPIC constant
sql/clickhouse/ddl_brN.sql    ClickHouse DDL for BR N; discovery key for start.py
data/brN/                     seed CSV(s) + README.md (text). raw/, lake/, checkpoints/ are local-only (gitignored)
grafana/dashboards/brN-<name>.json   provisioned dashboard
docs/                          README.md index; one folder per BR: docs/br1/ (tour-route.md, tour-route.html, architecture.svg/.html), docs/br2/ (dynamic-pricing.md/.html)
serving/                      static tourist UI (index.html), served read-only by the tour-map nginx service on :8090
logs/demo/                    brN-spark.log, brN-producer.log (gitignored)
```

Current BR state: BR1 tour route (`tour.*` DB), BR2 dynamic pricing (`pricing.*` DB), BR3 sentiment (`fact_reviews`, unqualified), BR4 revenue monitoring (`fact_bookings`, unqualified).

## 3. The BR contract (what start.py expects)

- **Discovery:** `discover_brs()` globs `sql/clickhouse/ddl_br*.sql`; the BR number is the filename stem after `ddl_br`. No DDL file = the BR does not exist.
- **Job lookup:** `job_path()` globs `jobs/br{N}_*/job.py`. Zero matches or more than one = exit. Use the same `N` everywhere.
- **No CLI args from start.py.** The job is run with spark-submit options only. Read config from env with defaults (`os.environ.get(...)`); argparse with env-backed defaults is fine (BR1, BR2).
- **Env forwarding:** `passthrough_env()` forwards host env keys matching `^(BR\d+_|MAX_OFFSETS$)` into `spark-local` via `docker exec -e`. Anything else must already exist in the `spark-local` service env in `docker-compose.yml` (`KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, `CLICKHOUSE_HOST=clickhouse`, `CLICKHOUSE_PASSWORD`, `SPARK_MASTER_URL`). Name your knobs `BRN_*`.
- **spark-submit:** `--master spark://spark-master:7077`; `--driver-memory` / `--executor-memory` = `SPARK_DRIVER_MEMORY` / `SPARK_EXECUTOR_MEMORY` (default `512m`); `spark.cores.max` = `SPARK_CORES_MAX` (default `1`); `spark.sql.shuffle.partitions` = `SPARK_SHUFFLE_PARTITIONS` (default `4`); `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:3.9.0`. Do not add other packages without updating this list.
- **Imports:** `--py-files` = all `jobs/common/*.py` plus every `*.py` sibling in the job's folder, so import them as flat modules: `from kafka_io import ...`, `from spark_session import get_spark_session`, `from pricing import ...`. Do not import from another BR's folder.
- **Readiness gate:** start.py polls the spark-submit log for the text `ResolveWriteToStream` in its last 80 lines. The timer (`SPARK_READY_TIMEOUT`, default `180` seconds) starts when spark-submit launches, so package resolution and any work before the query starts all count. The job MUST start a Structured Streaming query. Heavy batch warm-up goes after the query is started, or inside `foreachBatch`, or must be small. BR1 and BR2 currently run batch work before `.start()`; do not copy that without measuring it.
- **If the Spark job exits** before ready, start.py exits. If it exits while producers run, start.py stops producers and exits.
- **Producer CLI** (`produce_brN.py` runs in `spark-local` with `PYTHONPATH=/opt/jobs/common`): must accept `--interval` (float, seconds between rows), `--limit` (int, stop after N rows), `--loop` (repeat forever). start.py always passes `--interval`; passes `--limit N` when `--events`/`PRODUCER_LIMIT` is set, `--loop` when `--loop-producers` is set, and neither otherwise (full CSV once).
- **Topics:** `discover_topics()` in topics.py parses each `produce_br*.py` with `ast` and takes the first module-level `TOPIC = "<string literal>"`. Computed or env-driven values are ignored, so write a plain literal. Topics are created with 1 partition, replication factor 1, `--if-not-exists`. start.py runs `topics.py --reset` on every run, which deletes all discovered topics (all BRs) before recreating them. Topic backlog from earlier runs is wiped each run.
- **DDL:** each run applies `ddl_brN.sql` with `clickhouse-client --multiquery`. Statements must be idempotent (`IF NOT EXISTS`, `CREATE OR REPLACE VIEW`).
- **fact_* truncation:** after the DDL, start.py runs `TRUNCATE TABLE IF EXISTS` for each `CREATE TABLE [IF NOT EXISTS] fact_*` it finds. The regex only matches unqualified names: `fact_x` is truncated, `db.fact_x` is not (the captured group is `db`). For qualified fact tables, truncate inside the job (BR1 does this in its warm-up: `TRUNCATE TABLE` for `tour.fact_tour`).
- **ClickHouse from a job:**
  - `clickhouse_io`: `write_dataframe(pdf, table)` for pandas frames, e.g. BR4 `write_batch` inside `foreachBatch`. Its defaults are `localhost:8123`; `spark-local` sets `CLICKHOUSE_HOST=clickhouse`.
  - Large frames: BR1 `jobs/br1_tour_route/job.py` has `ch_post`, `insert_partition` (JSONEachRow over HTTP, 20k-row chunks) and `insert_rows(df, table, ch)` (`df.foreachPartition`). It reads `CLICKHOUSE_URL` (default `http://clickhouse:8123`). BR2 `pricing.py` has `ch_insert(json_lines)` doing the same.
- **Kafka bootstrap:** `KAFKA_BOOTSTRAP_SERVERS`, default `kafka:29092` in job code (BR1, BR2, BR4). Producers use `kafka_io.get_producer()` with the same env name, default `localhost:9092`.
- **Observability:** every streaming query MUST set `.queryName("<name>")`, because Prometheus metric names derive from it (BR1 `tour_requests`, BR2 `demand_ratio`, BR3 `reviews`, BR4 `bookings`). `start.py` injects these Prometheus confs into every BR's spark-submit: `spark.ui.prometheus.enabled=true`, `spark.sql.streaming.metricsEnabled=true`, `spark.metrics.namespace=br<N>`, and the PrometheusServlet sink (`spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet`, path `/metrics/prometheus`). Prometheus scrapes the driver and executor UI endpoints on `spark-local:4040`–`4043` (`spark-driver` job at `/metrics/prometheus/`, `spark-executors` job at `/metrics/executors/prometheus/`); one port per running BR, so unused ports show as down, which is expected. Each BR dashboard should include the "Spark streaming (Prometheus)" row: copy the 4 panels from `grafana/dashboards/br1-tour-route.json` and replace the `br1` namespace, the `tour_requests` query name and the `application_name`.

## 4. Boilerplate

Derived from `jobs/br4_revenue_monitoring/job.py`, `jobs/br2_dynamic_pricing/job.py`, `ingestion/producers/produce_br4.py`. Replace `brN`, `<name>`, `<topic>`.

(a) `jobs/brN_<name>/job.py`

```python
"""BRN <name>: Kafka <topic> -> ClickHouse fact_<x>."""
import os

from pyspark.sql import functions as F

from clickhouse_io import write_dataframe
from spark_session import get_spark_session

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "<topic>"  # must equal the TOPIC in produce_brN.py
SCHEMA = "id string, value double"  # DDL string for from_json
MAX_OFFSETS = os.environ.get("BRN_MAX_OFFSETS", "500")


def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        write_dataframe(batch_df.toPandas(), "<db>.fact_<x>")


def main():
    spark = get_spark_session("brN-<name>")
    spark.sparkContext.setLogLevel("WARN")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )
    events = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
    # Optional batch warm-up: after the query is started, or keep it tiny (see contract).
    query = events.writeStream.foreachBatch(write_batch).trigger(processingTime="5 seconds").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
```

(b) `ingestion/producers/produce_brN.py`

```python
"""BRN producer: rows from data/brN/<file>.csv -> Kafka <topic>."""
import argparse
import csv
import sys
import time
from pathlib import Path

from kafka_io import get_producer

SAMPLE_PATH = Path(__file__).resolve().parents[2] / "data" / "brN" / "<file>.csv"  # /opt/data/... in container
TOPIC = "<topic>"

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=float, default=0)
parser.add_argument("--loop", action="store_true")
parser.add_argument("--limit", type=int)
args = parser.parse_args()


def rows():
    if not SAMPLE_PATH.exists():
        sys.exit(f"{SAMPLE_PATH} not found — see data/brN/README.md")
    with SAMPLE_PATH.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield {"id": row["id"], "value": row["value"]}


producer = get_producer()
sent = 0
while True:
    for row in rows():
        producer.send(TOPIC, row)
        sent += 1
        if sent % 500 == 0:
            print(f"sent {sent} rows", flush=True)
        if args.limit and sent >= args.limit:
            producer.flush()
            print(f"finished sending {sent} rows")
            sys.exit(0)
        time.sleep(args.interval)
    if not args.loop:
        break
producer.flush()
print(f"finished sending {sent} rows")
```

(c) `sql/clickhouse/ddl_brN.sql`

```sql
-- BRN <name>: serving tables for <dashboard / map>.
-- Applied by scripts/start.py on every run (clickhouse-client --multiquery): keep it idempotent.
CREATE DATABASE IF NOT EXISTS <db>;

CREATE TABLE IF NOT EXISTS <db>.fact_<x>
(
    id           String,
    value        Float64,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (id, ingested_at);
```

(d) Grafana dashboard rule: file `grafana/dashboards/brN-<name>.json`; top-level `uid` `brN-<short>` (existing: `br1-tour`, `br2-pricing`); datasource uid `clickhouse` (BR1 uses it; BR2 uses `clickhouse-pricing`, which points at DB `pricing`); always use fully-qualified table names (`<db>.fact_<x>`) so the default database does not matter. Provisioned by the `CO5173` provider from `/var/lib/grafana/dashboards` (`allowUiUpdates: true`, reloads every 30s), so edits in the UI are not written back to the repo.

## 5. Coding conventions

- Names: `snake_case` for functions, variables, modules; `ALL_CAPS` for module constants.
- Config: `os.environ.get("NAME", "default")` at call time or module level. Document new knobs in the job docstring.
- Output: `print()` (with `flush=True` in long loops), not `logging`.
- No type-annotation sprawl; annotate only where the file already does.
- No abstraction used once. Copy a helper before extracting a shared one. Shared helpers go in `jobs/common/` only when two or more BRs use them.
- Self-check: a `--self-test` flag or `assert`s under `if __name__ == "__main__":`. No pytest. BR1's `solve_order` check is the model: `python3 jobs/br1_tour_route/job.py --self-test` (stdlib-only imports at module top).
- A deliberate shortcut gets a comment `# ponytail: <shortcut>, <upgrade path>`. Example: `# ponytail: flat tariff, swap for per-city tariff table when needed`.
- Keep diffs minimal. Do not reformat files you do not need to change.
- Never edit Git LFS pointer files (`version https://git-lfs.github.com/spec/v1` ...). Fix the data source, not the pointer.
- Never commit data under `data/*/raw`, `data/*/lake`, `data/*/checkpoints`, or `*.log`.

## 6. Run & verify

- Full stack + one BR: `make demo-br BR=1` (BR accepts `1`, `1,4`, or `all`; default `all` for `make demo`).
- Limit or pace producers: `make demo-br BR=4 PRODUCER_LIMIT=500 PRODUCER_INTERVAL=0.01` (`PRODUCER_LIMIT` becomes `--events`, then `--limit`).
- Env knobs pass through: `BR1_BENCH_REQUESTS=5000 make demo-br BR=1`. Host-side knobs: `SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY`, `SPARK_CORES_MAX`, `SPARK_SHUFFLE_PARTITIONS`, `SPARK_READY_TIMEOUT`.
- Logs: `logs/demo/brN-spark.log` and `logs/demo/brN-producer.log`. `make logs` tails all; `make logs SVC=grafana` tails one container.
- Check ClickHouse rows (single-quoted so `$CLICKHOUSE_PASSWORD` expands inside the container):
  `docker exec clickhouse sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --query "SELECT count() FROM tour.fact_tour"'`
- Grafana: http://localhost:3000 (admin / admin per `docker-compose.yml`). Tourist map: http://localhost:8090. Spark master UI: http://localhost:8082.
- Kafka UI (optional): `docker compose --profile ui up -d kafka-ui`, then http://localhost:8080.
- BR2 prerequisite: `bash scripts/download_tlc.sh yellow 2025-07 2026-07` before `make demo-br BR=2`.
- BR4 prerequisite: `data/br4/*.csv` are Git LFS pointers in this checkout (~130-byte `version https://git-lfs.github.com/spec/v1` files). Run `git lfs pull` first, or the producer finds no rows.
- Stop: `make down` (or Ctrl+C for `make demo*`).

## 7. Checklist for adding BR N

- [ ] Pick the next free N (`ls sql/clickhouse/ddl_br*.sql`); pick a short `<name>`.
- [ ] Write `sql/clickhouse/ddl_brN.sql`: `CREATE DATABASE IF NOT EXISTS`, idempotent tables, `fact_*` names for tables that should auto-truncate.
- [ ] Write `jobs/brN_<name>/job.py` (one file) plus sibling helpers; start the streaming query; add `BRN_*` env knobs.
- [ ] Write `ingestion/producers/produce_brN.py` with module-level `TOPIC = "..."`, `--interval`, `--limit`, `--loop`.
- [ ] Add seed data `data/brN/` with `README.md` (text); add `.gitignore` lines for `data/brN/raw/`, `data/brN/lake/`, `data/brN/checkpoints/`, `data/brN/*.log`.
- [ ] Add `grafana/dashboards/brN-<name>.json` with uid `brN-<short>` and fully-qualified tables.
- [ ] Prometheus streaming row on the dashboard (4 panels, see section 3 Observability); `.queryName("<name>")` set on every streaming query.
- [ ] Add `docs/brN/README.md` and `docs/brN/index.html` (diagrams beside them).
- [ ] Run `make demo-br BR=N`; confirm `ResolveWriteToStream` in `logs/demo/brN-spark.log` and rows in ClickHouse.
- [ ] Check Grafana and the tourist map if the BR has one.
- [ ] Update the layout tree (section 2) and the BR state line.

## 8. Git

- Agents never run `git add`, `git commit`, or `git push`. Leave changes in the working tree; the human stages and commits.
- At the end, report changed paths and propose a commit message in the repo style. Observed style (`git log --oneline -5`): `5cca6c0 sync up boilderplate from master`, `ae6269c implement dynamic pricing pipeline`, `095945c init`. Lowercase, imperative or plain descriptive subject, no prefix.
- Read-only git (`status`, `diff`, `log`, `show`, `branch`) is fine.
