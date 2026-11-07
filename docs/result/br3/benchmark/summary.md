| BR | Engine | Rows | end_to_end_s | first_row_s | rows_per_s | peak_mem_mib | mean_cpu_pct | p50_ms | p95_ms | p99_ms | live_peak_mem_mib | live_mean_cpu_pct |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| BR3 | spark | 200000 | 106.22 | 16.92 | 1883 | 3268 | 115 | 4626 | 6934 | 7242 | 2760 | 77.8 |
| BR3 | clickhouse | 200000 | 18.48 | 5.19 | 10822 | 973 | 80.2 | 4185 | 7790 | 8108 | 930 | 15.6 |
| BR4 | spark | 49964 | 27.31 | 11.28 | 1830 | 2688 | 135.2 | 3087 | 5450 | 5770 | 2751 | 46.2 |
| BR4 | clickhouse | 49964 | 4.16 | 4.16 | 12024 | 898 | 9.5 | 3984 | 7342 | 7641 | 958 | 15.6 |

Median of 3 runs. Spark maxOffsetsPerTrigger = 10000; live: 1000 msg/s for 60 s, latency = ingested_at - Kafka timestamp.

Correctness (backlog outputs joined on id, Spark vs ClickHouse):
- BR3: 199998/200000 rows with identical keywords; 199999/200000 with the same English/non-English decision
- BR4: 49964/49964 rows with identical revenue and bucket
- Row counts (count, distinct ids): {'fact_reviews': (200000, 200000), 'fact_reviews_ch': (200000, 200000), 'fact_bookings': (49964, 49964), 'fact_bookings_ch': (49964, 49964)}
