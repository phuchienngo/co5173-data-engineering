# BR4 Plan: Benchmark Test

Compares the main solution (Spark Structured Streaming, ETL) with the alternative (ClickHouse Kafka engine + Materialized View, ELT) on revenue and bucket. One script runs BR3 and BR4 together: `tests/br3/benchmark.py`. Figures: [result-benchmark.md](../../result/br4/result-benchmark.md).

## 1. What was asked

| # | Ask | How this plan covers it |
|---|---|---|
| 1 | Data correctness | Join both outputs on the id and compare every row |
| 2 | DE performance | Backlog drain and a steady 1,000 msg/s stream |
| 3 | Alternative solution benchmarked against the main one | ClickHouse MV in `tests/br4/clickhouse_br4.sql` |

## 2. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Two scenarios**: drain a backlog already in Kafka, and a 1,000 msg/s stream for 60 s. | Backlog shows throughput; the stream shows latency. |
| D2 | **3 repetitions, median [min - max].** | Enough to see noise on one laptop; not a statistical test. |
| D3 | **Same resources as the repo**: Docker 8 GB / 8 CPU, ClickHouse 1 GB, Spark worker 2 cores, `spark.cores.max=1`. | Results describe the real stack. |
| D4 | **`MAX_OFFSETS_PER_TRIGGER=10000`** in the benchmark (demo keeps 500). | Matches ClickHouse's 10,000-row blocks (`kafka_max_block_size`). |
| D5 | **Latency = `ingested_at` − `event_time`** (Kafka timestamp). | One definition for both engines. |
| D6 | **The script waits for Spark master to have no active app** before each submit. | A leftover driver held all cores and starved the benchmark run. |

## 3. Metrics

| Metric | How |
|---|---|
| Backlog drain time, rows/s | Time from start until all expected rows exist |
| Latency p50 / p95 / p99 | Steady stream, per row |
| Peak RAM, mean CPU | `docker stats` |
| Correctness | Row-by-row comparison on the id |

## 4. Run

```bash
python3 scripts/start.py --events 3000 3 4   # build image, create tables, load dimensions; Ctrl+C when done
pip install clickhouse-connect kafka-python
make br3-bench REPS=3                        # about 40 minutes
```

Raw output: `docs/result/br3/benchmark/` (`benchmark_runs.csv`, `summary.md`, `benchmark.log`).

## 5. Not done

- No screenshots of the dashboards for each run, and the figures are not stored in a `pipeline_runs` table.
