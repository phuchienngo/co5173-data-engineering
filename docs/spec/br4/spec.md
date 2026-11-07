# BR4 Spec: Revenue Monitoring

Business context: see [intent.md](../../intent/br4/intent.md).

Priorities: **Must** = needed for the demo and the report, **Should** = wanted, **Could** = only if time allows.

## 1. Key value: watch booking revenue as it arrives

> A property manager sees revenue per booking, how it splits into low / medium / high bookings, and where and in which room types it comes from.

Users:
- **Property manager:** reads the Grafana dashboard.
- **Project team (us):** compares the main pipeline with the alternative and reports the figures.

## 2. What the user sees (Grafana dashboard `br4-data-result`)

| # | Item | Priority |
|---|---|---|
| R1 | Bookings, total revenue, average revenue, active properties | Must |
| R2 | Revenue bucket distribution (low / medium / high) | Must |
| R3 | Revenue by room type, bookings and revenue over time | Should |
| R4 | Top locations by revenue, highest-revenue bookings | Should |

## 3. Data

| Source | Content | Size |
|---|---|---|
| `data/br4/Fact_Bookings.csv` | Bookings: `id`, `listing_id`, `revenue`, `nights_booked` | 50,000 bookings |
| `data/br4/Dim_Property.csv` | Property dimension: city, room type, price per night | 1,500 properties |

Bookings arrive as a stream (Kafka topic `bookings`). Properties are static.

## 4. Definitions

- **Revenue:** the revenue in the event; if missing, `price × nights_booked` (1 night if missing too).
- **Bucket:** low < 2,000 ≤ medium < 8,000 ≤ high.
- **Valid booking:** its `listing_id` exists in `dim_listing_pricing`; others are dropped by the inner join.

## 5. Non-functional requirements

- **Correctness:** the main and the alternative pipeline give the same revenue and bucket for every booking.
- **Speed (measured in the benchmark):** backlog drain time, throughput, latency under a steady stream, memory and CPU.
- **Runs locally** on the existing Docker stack.

## 6. Out of scope

Revenue forecasting, currency conversion, refunds and cancellations.

## 7. Acceptance checklist

- [ ] `make demo-br BR=4` fills `fact_bookings`, and the dashboard shows R1–R4.
- [ ] A restart does not duplicate rows.
- [ ] The benchmark compares Spark with the ClickHouse Materialized View; the two outputs match on every booking ([plan-benchmark.md](../../plan/br4/plan-benchmark.md)).

## 8. Open decisions

None.
