| Engine | Rows | end_to_end_s | first_row_s | rows_per_s | peak_mem_mib | mean_cpu_pct | p50_ms | p95_ms | p99_ms | live_peak_mem_mib | live_mean_cpu_pct |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark | 44332/200000 | 96.99 | 17.17 | 2062 | 2078 | 88.2 | 6913 | 10822 | 11028 | 2217 | 49.2 |
| clickhouse | 44332/200000 | 11.1 | 4.43 | 18023 | 345 | 23.7 | 4300 | 7591 | 7746 | 374 | 13 |

Median of 3 runs. Spark maxOffsetsPerTrigger = 20000; live: 1000 trips/s for 60 s, latency per hourly window = ingested_at - newest Kafka timestamp.
rows = landmark trips written, topic_rows = Kafka messages consumed; rows_per_s uses topic_rows (Rows column shows landmark/topic).
ClickHouse Kafka engine flushes blocks every stream_flush_interval_ms (7.5 s default); Spark trigger is 10 s.

Correctness (backlog outputs joined on location and hour, Spark vs ClickHouse):
- 684/684 windows identical in every column; active_bookings 684/684, avg_fare 684/684, capacity 684/684, suggested_fare 684/684, price_action 684/684
- Window counts: {'spark': 684, 'clickhouse': 684}
