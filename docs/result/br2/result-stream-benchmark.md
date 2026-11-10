# Benchmark BR2 - Dynamic Pricing (stream)

Phần batch (raw vs lake) nằm ở `docs/result/br2/result-benchmark.md`. File này chỉ so sánh phần stream.

## So sánh gì
| | Giải pháp chính | Giải pháp thay thế |
|---|---|---|
| Kiểu | ETL: Kafka → Spark Structured Streaming → ClickHouse | ELT: ClickHouse tự đọc Kafka (`ENGINE = Kafka`) + Materialized View |
| Code | `jobs/br2_dynamic_pricing/job.py` (`run_stream`) | `tests/br2/clickhouse_br2.sql` |
| Gom nhóm | `from_json` → lọc 16 landmark zone → window 1 h theo location, watermark 2 h, `count` + `avg(fare)` | MV nhóm theo `(location_id, toStartOfHour(pickup_datetime))` vào `AggregatingMergeTree` (`SimpleAggregateFunction` sum/max) |
| Join capacity | broadcast join capacity (P95) | view `v_demand_ratio_ch` join `dim_capacity_ch` (768 slot) và `dim_landmark_ch` (16) |
| Luật giá | `demand_ratio_pct = bookings * 100 / capacity`; trên 80% thì fare × 1.15 | cùng công thức và cùng ngưỡng trong view |
| Ghi | `pricing.demand_ratio_hourly` (`ReplacingMergeTree(updated_at)`, `outputMode update`, trigger 10 s) | `pricing.demand_ratio_hourly_ch` |

Như BR4, BR2 làm lại được 100% bằng SQL (aggregate + join + luật giá) nên đây là phép so sánh tương đương; khác BR3, không có logic nào ClickHouse không biểu diễn được.

## Dữ liệu và cấu hình
- 200.000 chuyến yellow taxi đầu tiên của 2026-07 (sắp theo giờ đón) từ `data/br2/lake/fact_trips`; 44.332 chuyến kết thúc ở một trong 16 landmark zone (topic `bench_br2_200000`).
- Live: 1.000 chuyến/s trong 60 s (60.000 chuyến); các dòng gửi bắt đầu sau các dòng warm-up ít nhất 3 giờ; mỗi lần có 14.656 chuyến landmark trên 217 window theo giờ.
- Spark submit giống `scripts/start.py` (driver 512 MB, executor 512 MB, `spark.cores.max=1`), trigger 10 s; benchmark đặt `maxOffsetsPerTrigger=20000` (`BENCH_MAX_OFFSETS`).
- Cluster Spark: 1 worker, 2 core. ClickHouse Kafka engine: `kafka_max_block_size = 10000`, flush mặc định (`stream_flush_interval_ms` 7,5 s).

## Tiêu chí
| Tiêu chí (theo đề) | Đo bằng |
|---|---|
| Đúng dữ liệu | chạy backlog "check" cho mỗi engine, join 2 output theo `(location_id, toUnixTimestamp(window_start))`: so `active_bookings`, `avg_fare`, `capacity`, `suggested_fare`, `price_action`; số window mỗi bên |
| Hiệu năng - xả backlog | 200.000 message có sẵn trong Kafka: thời gian đến khi `sum(active_bookings)` (`FINAL`) ≥ 44.332; rows/s = 200.000 / end-to-end (số message Kafka đã đọc mỗi giây); RAM đỉnh và CPU trung bình từ `docker stats` (Spark: spark-local + spark-worker + spark-master + clickhouse; ClickHouse: chỉ clickhouse) |
| Hiệu năng - chạy liên tục | 1.000 chuyến/s trong 60 s: độ trễ mỗi window giờ = `ingested_at` (Spark: `updated_at`) − timestamp Kafka mới nhất trong window, p50/p95/p99 |
| Hiệu quả khai thác | cả hai ghi vào ClickHouse `pricing.*`, dashboard Grafana `br2-advisor` |

Mỗi phép đo lặp 3 lần, báo median [min - max].

## Chạy lại
```bash
docker compose --profile full up -d clickhouse kafka spark-master spark-worker spark-local
python3 -m venv .venv && .venv/bin/pip install clickhouse-connect==1.8.0 kafka-python==3.0.11 pyarrow==22.0.0
.venv/bin/python tests/br2/benchmark_stream.py 3
```
- Cần lake (`data/br2/lake/fact_trips`, `dim_capacity`) dựng sẵn bằng `make demo-br BR=2` một lần, và dừng demo stream BR2.
- Homebrew Python chặn `pip install` (PEP 668) nên dùng venv; `make br2-bench-stream` giả định `python3` đã có đủ thư viện.
- Biến môi trường: `BENCH_TRIPS` (200000), `BENCH_MONTH` (2026-07), `BENCH_RATE` (1000), `BENCH_DURATION` (60), `BENCH_MAX_OFFSETS` (20000).
- Kết quả thô: `docs/result/br2/benchmark-stream/benchmark_runs.csv`, `docs/result/br2/benchmark-stream/summary.md`.

## Kết quả (median [min - max] của 3 lần)

Xả backlog 200.000 message (44.332 chuyến landmark, 684 window sau khi gom):

| Engine | End-to-end (s) | Dòng đầu tiên (s) | rows/s | RAM đỉnh (MiB) | CPU TB (%) |
|---|---|---|---|---|---|
| Spark | 96.99 [92.16 - 101.58] | 17.17 [15.72 - 17.36] | 2,062 [1,969 - 2,170] | 2,078 [2,045 - 2,108] | 88.2 [75.1 - 90.5] |
| ClickHouse MV | 11.1 [11.07 - 11.7] | 4.43 [4.43 - 4.49] | 18,023 [17,087 - 18,068] | 345 [303 - 426] | 23.7 [22.4 - 24.1] |

Chạy liên tục 1.000 chuyến/s trong 60 s (14.656 chuyến landmark, 217 window mỗi lần, cả hai engine, cả 3 lần):

| Engine | p50 (ms) | p95 (ms) | p99 (ms) | RAM đỉnh (MiB) | CPU TB (%) |
|---|---|---|---|---|---|
| Spark | 6,913 [5,835 - 6,922] | 10,822 [10,000 - 11,139] | 11,028 [10,311 - 11,355] | 2,217 [1,965 - 2,257] | 49.2 [33.6 - 51.8] |
| ClickHouse MV | 4,300 [4,174 - 4,395] | 7,591 [7,544 - 7,618] | 7,746 [7,746 - 7,801] | 374 [329 - 424] | 13.0 [12.8 - 19.2] |

Độ đúng: 684/684 window giống hệt ở cả 5 cột (`active_bookings`, `avg_fare`, `capacity`, `suggested_fare`, `price_action`); mỗi bên 684 window.

## Nhận xét
- Kết quả giống nhau 100%: aggregate theo giờ + join capacity + luật 80% / +15% chuyển sang SQL của ClickHouse không mất gì.
- Xả backlog: ClickHouse MV nhanh hơn 8.7x (end-to-end 96.99 / 11.1 s; rows/s 18,023 / 2,062 = 8.7x), dùng RAM ít hơn 6.0x (2,078 / 345 MiB) và CPU ít hơn khoảng 3.7x (88.2 / 23.7). Dòng đầu tiên: 4.43 s so với 17.17 s (3.9x).
- Chạy liên tục: ClickHouse cũng thấp hơn ở mọi phân vị: p50 4,300 so với 6,913 ms (1.6x), p95 1.4x, p99 1.4x. Khác BR4, ở đây Spark không thắng độ trễ.
- Hai độ trễ đều chứa chu kỳ cấu hình mặc định, chưa chỉnh: p50 của Spark gồm trigger 10 s, p50 của ClickHouse gồm `stream_flush_interval_ms` 7,5 s. Dùng đúng mặc định như BR3/BR4.
- RAM của Spark tính cả 3 container JVM (spark-local, spark-worker, spark-master) cộng ClickHouse; RAM của ClickHouse chỉ tính một container.
- Đánh đổi: với phép biến đổi thuần SQL như BR2, ClickHouse MV nhanh và rẻ hơn rõ rệt. Spark vẫn có chỗ đứng ở: baseline batch capacity một lần trên 48,950,921 dòng (so raw vs lake ở `docs/result/br2/result-benchmark.md`), ngữ nghĩa watermark / dữ liệu đến muộn, và một codebase chung cho batch và stream.

## Nhật ký
| Lần | Vấn đề | Xử lý |
|---|---|---|
| Smoke (`BENCH_TRIPS=40000`, `BENCH_DURATION=10`, 1 lần lặp) | Không lỗi. Spark 21.33 s so với ClickHouse 7.32 s, 205/205 window giống hệt | Chạy đủ cấu hình |
| 1 | Lần chạy chết cùng phiên Claude sau Spark rep 0 (94.32 s) | Chạy lại từ đầu |
| 2 | Va chạm với một instance harness còn sót, instance đó xóa `pricing.demand_ratio_hourly_ch` giữa lúc poll: `Code: 60 ... Unknown table expression identifier 'pricing.demand_ratio_hourly_ch'` | Kill process còn sót và job Spark của nó (`docker exec spark-local pkill -f jobs/br2_`), chạy lại |
| 3 | Chạy đủ 3 lần lặp, EXIT 0 | Dùng số liệu lần này |

## Hạn chế
- Một máy; cluster Spark chỉ 1 worker 2 core; kết quả phụ thuộc giới hạn RAM/CPU Docker.
- 3 lần lặp chưa đủ để kiểm định thống kê; khoảng min-max không chồng nhau ở end-to-end, rows/s và RAM.
- Không đo CPU/RAM của Kafka broker; RAM của ClickHouse không gồm chi phí phía broker của consumer thread.
- Độ trễ live tính theo window giờ (217 window mỗi lần), không phải từng chuyến.
- Chỉ dùng dữ liệu 2026-07.
