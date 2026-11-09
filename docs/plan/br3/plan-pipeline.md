# BR3 Plan: Data Processing Pipeline

Implements [spec.md](../../spec/br3/spec.md). Main solution: ETL with Spark Structured Streaming. The alternative (ClickHouse Materialized View) is in [plan-benchmark.md](plan-benchmark.md).

## 1. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Stream job** (default mode, no `MODE` constant): Kafka `reviews` → Spark → ClickHouse. | Reviews arrive over time; the dashboard should update as they do. |
| D2 | **Sentiment with VADER** in a Python UDF, with a `non_english` label when English stop words are below 20% of the words. | VADER only scores English; without the check, other languages are scored wrongly (German "die" read as negative). |
| D3 | **Keywords with `RegexTokenizer` + `StopWordsRemover`** (Spark's 181 English stop words). | Native Spark ML stages, no UDF. |
| D4 | **Listings join as a broadcast join** with `dim_airbnb_listing`. | The dimension is small (31,069 rows). |
| D5 | **Dimension read with Python's `csv` module**, not Spark. | Spark's multiLine CSV reader misaligns columns after free text and dropped about 1.6k listings. |
| D6 | **Write with `toPandas()` → `write_dataframe`** inside `foreachBatch`. | Repo convention: the naive writer is the baseline. Trigger every 5 s, `maxOffsetsPerTrigger` 500 (demo). |
| D7 | **`fact_reviews` and `dim_airbnb_listing` are truncated at start.** | `insert_df` only appends; a restart would duplicate rows. |

## 2. Flow

1. `produce_br3.py` streams `reviews.csv.gz` to topic `reviews`.
2. `job.py` loads listings into ClickHouse and broadcasts their ids.
3. Per micro-batch: parse JSON → tokenize and remove stop words → VADER label → inner join listings → insert into `fact_reviews`.
4. Grafana reads `fact_reviews` and `dim_airbnb_listing`.

## 3. Files

| File | Role |
|---|---|
| `ingestion/producers/produce_br3.py` | Producer (`TOPIC = "reviews"`) |
| `jobs/br3_sentiment_analysis/job.py` | Spark streaming job |
| `sql/clickhouse/ddl_br3.sql` | `fact_reviews`, `dim_airbnb_listing` (MergeTree) |
| `grafana/dashboards-br3/br3-data-result.json` | Dashboard |
| `tests/br3/benchmark.py`, `tests/br3/clickhouse_br3.sql` | Benchmark and the alternative solution |

## 4. Tables

| Table | Key columns | Order by |
|---|---|---|
| `fact_reviews` | `review_id`, `listing_id`, `sentiment_label`, `keywords`, `event_time`, `ingested_at` | `(listing_id, ingested_at)` |
| `dim_airbnb_listing` | `listing_id`, `name`, `neighbourhood`, `latitude`, `longitude`, `room_type` | `listing_id` |

## 5. Run

```bash
make demo-br BR=3
```

## 6. Verification

- Row count in `fact_reviews` equals the reviews whose listing exists in the dimension.
- Same output from the alternative pipeline: [result-benchmark.md](../../result/br3/result-benchmark.md).

## 7. Not done

- No `pipeline_runs` table or run history (BR5/BR6 have one). The benchmark script keeps its figures in `docs/result/br3/benchmark/`.
