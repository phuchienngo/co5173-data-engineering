# BR4 Plan: Data Processing Pipeline

Implements [spec.md](../../spec/br4/spec.md). Main solution: ETL with Spark Structured Streaming. The alternative (ClickHouse Materialized View) is in [plan-benchmark.md](plan-benchmark.md).

## 1. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Stream job**: Kafka `bookings` → Spark → ClickHouse. | Bookings arrive over time. |
| D2 | **Revenue = event revenue, else `price × nights_booked`** (1 night if missing). | Some events carry no revenue. |
| D3 | **Bucket in a Python UDF** (`bucket.py`): low < 2,000 ≤ medium < 8,000 ≤ high. | Simple rule; the alternative does it with `multiIf`. |
| D4 | **Broadcast join** with `dim_listing_pricing` (from `Dim_Property.csv`). | 1,500 rows. |
| D5 | **Write with `toPandas()` → `write_dataframe`** in `foreachBatch`; trigger 5 s, `maxOffsetsPerTrigger` 500. | Repo convention: naive writer as the baseline. |
| D6 | **Tables truncated at start.** | `insert_df` only appends. |

## 2. Flow

1. `produce_br4.py` streams `Fact_Bookings.csv` to topic `bookings`.
2. `job.py` loads `Dim_Property.csv` into ClickHouse and broadcasts it.
3. Per micro-batch: parse JSON → inner join property → revenue → bucket → insert into `fact_bookings`.
4. Grafana reads `fact_bookings` and `dim_listing_pricing`.

## 3. Files

| File | Role |
|---|---|
| `ingestion/producers/produce_br4.py` | Producer (`TOPIC = "bookings"`) |
| `jobs/br4_revenue_monitoring/job.py`, `bucket.py` | Spark streaming job and bucket rule |
| `sql/clickhouse/ddl_br4.sql` | `fact_bookings`, `dim_listing_pricing` (MergeTree) |
| `grafana/dashboards-br4/br4-data-result.json` | Dashboard |
| `tests/br4/clickhouse_br4.sql` | Alternative solution (benchmark script: `tests/br3/benchmark.py`) |

## 4. Tables

| Table | Key columns | Order by |
|---|---|---|
| `fact_bookings` | `booking_id`, `listing_id`, `revenue`, `revenue_bucket`, `event_time`, `ingested_at` | `(listing_id, ingested_at)` |
| `dim_listing_pricing` | `listing_id`, `neighbourhood`, `room_type`, `price`, `minimum_nights` | `listing_id` |

## 5. Run

```bash
make demo-br BR=4
```

## 6. Verification

- Same output from the alternative pipeline on every booking: [result-benchmark.md](../../result/br4/result-benchmark.md).

## 7. Not done

- No `pipeline_runs` table or run history.
