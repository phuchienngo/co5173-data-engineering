| BR | Engine | Rows | end_to_end_s | first_row_s | rows_per_s | peak_mem_mib | mean_cpu_pct | p50_ms | p95_ms | p99_ms | live_peak_mem_mib | live_mean_cpu_pct |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| BR3 | spark | 200000 | 114.58 | 21.3 | 1745 | 3007 | 129.5 | 5044 | 7418 | 8085 | 2920 | 91.8 |
| BR3 | clickhouse | 200000 | 20.01 | 5.16 | 9997 | 788 | 80.2 | 4334 | 7857 | 8214 | 626 | 14.4 |
| BR4 | spark | 49964 | 29.25 | 15.03 | 1708 | 2576 | 171.4 | 3252 | 5501 | 5865 | 2672 | 48.3 |
| BR4 | clickhouse | 49964 | 4.13 | 4.13 | 12097 | 858 | 12 | 3878 | 7286 | 7595 | 864 | 13.1 |

Median of 3 runs. Spark maxOffsetsPerTrigger = 10000; live: 1000 msg/s for 60 s, latency = ingested_at - Kafka timestamp.

Correctness (backlog outputs joined on id, Spark vs ClickHouse):
- BR3: 199998/200000 rows with identical keywords; 199999/200000 with the same English/non-English decision
- BR4: 49964/49964 rows with identical revenue and bucket
- Row counts (count, distinct ids): {'fact_reviews': (200000, 200000), 'fact_reviews_ch': (200000, 200000), 'fact_bookings': (49964, 49964), 'fact_bookings_ch': (49964, 49964)}
