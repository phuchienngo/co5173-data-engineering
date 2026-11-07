# Benchmark BR3 - Customer Sentiment Analysis

## So sánh gì
| | Giải pháp chính | Giải pháp thay thế |
|---|---|---|
| Kiểu | ETL: Kafka → Spark Structured Streaming → ClickHouse | ELT: ClickHouse tự đọc Kafka (`ENGINE = Kafka`) + Materialized View |
| Code | `jobs/br3_sentiment_analysis/job.py` | `tests/br3/clickhouse_br3.sql` |
| Tách từ khóa | `RegexTokenizer` `[a-z]{3,}` + `StopWordsRemover` (181 stop word tiếng Anh của Spark) | `extractAll(lower(comments), '[a-z]{3,}')` + cùng 181 stop word (bảng `en_stopwords`, lấy từ jar `spark-mllib`) |
| Lọc tiếng Anh | tỷ lệ stop word tiếng Anh >= 20%, nếu không thì `non_english` | cùng quy tắc (`arrayCount`) |
| Cảm xúc | VADER (Python UDF) | KHÔNG có: ClickHouse không chạy được VADER bằng SQL |
| Join dim | broadcast join `dim_airbnb_listing` | `WHERE listing_id IN (SELECT listing_id FROM dim_airbnb_listing)` |

Lưu ý công bằng: Spark làm thêm phần VADER, ClickHouse chỉ làm phần từ khóa + lọc ngôn ngữ.

## Dữ liệu và cấu hình
- Inside Airbnb Bangkok, snapshot 2026-06-29: `data/br3/reviews.csv.gz` (693.647 review thật, Git LFS), `data/br3/listings.csv` (31.069 listing). Benchmark dùng 200.000 review đầu.
- Docker Desktop 8 GB, 8 CPU; giới hạn theo `docker-compose.yml` của repo: ClickHouse 1 GB; Spark worker 2.500 MB, 2 core; driver trong `spark-local`.
- Spark submit giống `scripts/start.py`: driver 512 MB, executor 512 MB, `spark.cores.max=1`, trigger 5 s. Riêng benchmark đặt `MAX_OFFSETS_PER_TRIGGER=10000` (demo giữ 500) để ngang với khối 10.000 dòng của ClickHouse.
- ClickHouse Kafka engine: `kafka_max_block_size = 10000`, flush theo mặc định (~7,5 s).

## Tiêu chí
| Tiêu chí (theo đề) | Đo bằng |
|---|---|
| Đúng dữ liệu | nối 2 output theo `review_id`: số dòng `keywords` giống hệt; số dòng cùng quyết định tiếng Anh |
| Hiệu năng - xả backlog | 200.000 message có sẵn trong Kafka: thời gian từ lúc khởi động đến khi đủ dòng; rows/s; RAM đỉnh; CPU trung bình (`docker stats`) |
| Hiệu năng - chạy liên tục | 1.000 msg/s trong 60 s: độ trễ từng dòng = `ingested_at` − `event_time` (timestamp Kafka), p50/p95/p99 |
| Hiệu quả khai thác | cả hai ghi vào ClickHouse, dashboard Grafana `br3-data-result` |

Mỗi phép đo lặp 3 lần, báo median [min - max].

## Chạy lại
```bash
python3 scripts/start.py --events 3000 3 4   # build image, tạo bảng, nạp dim; Ctrl+C khi xong
pip install clickhouse-connect kafka-python
make br3-bench REPS=3        # khoảng 40 phút
```
Kết quả thô: `docs/result/br3/benchmark/benchmark_runs.csv`, `docs/result/br3/benchmark/summary.md`.

## Kết quả (median [min - max] của 3 lần, nền `main`)

Xả backlog 200.000 review:

| Engine | End-to-end (s) | Dòng đầu tiên (s) | rows/s | RAM đỉnh (MiB) | CPU TB (%) |
|---|---|---|---|---|---|
| Spark | 106,2 [106,0 - 107,3] | 16,9 | 1.883 [1.863 - 1.887] | 3.268 [2.874 - 3.302] | 115 [114 - 115] |
| ClickHouse MV | 18,5 [18,1 - 19,0] | 5,2 | 10.822 [10.523 - 11.070] | 973 [883 - 1.024] | 80 [80 - 82] |

Chạy liên tục 1.000 msg/s trong 60 s (60.000/60.000 dòng tới đủ ở cả hai, cả 3 lần):

| Engine | p50 (ms) | p95 (ms) | p99 (ms) | RAM đỉnh (MiB) | CPU TB (%) |
|---|---|---|---|---|---|
| Spark | 4.626 [4.619 - 4.718] | 6.934 [6.925 - 6.954] | 7.242 [7.241 - 7.332] | 2.760 | 78 |
| ClickHouse MV | 4.185 [4.068 - 4.247] | 7.790 [7.693 - 7.897] | 8.108 [8.071 - 8.227] | 930 | 16 |

Độ đúng (200.000 dòng, nối theo `review_id`; mỗi bên 200.000 dòng, không trùng id):
- `keywords` giống hệt: 199.998/200.000 (99,999%); quyết định tiếng Anh giống: 199.999/200.000.
- 3 dòng lệch đều là review tiếng Thổ Nhĩ Kỳ có chữ `İ`: Java/Python hạ thành `i` + dấu chấm kết hợp nên tách thêm được chữ `i`; `lower()` của ClickHouse chỉ hạ chữ ASCII nên giữ `İ`.

## Nhận xét
- Xả backlog: ClickHouse MV nhanh hơn khoảng 5,7 lần, dùng khoảng 1/3,4 RAM. Spark tốn ~17 s khởi động (JVM, nạp 31k listing, kết nối cluster) rồi xử lý ~2.200 dòng/s với 1 core (`spark.cores.max=1` theo cấu hình của repo) vì phải chạy VADER bằng Python UDF.
- Chạy liên tục: độ trễ hai bên cùng cỡ 4-8 s; ClickHouse thấp hơn ở p50, Spark thấp hơn ở p95/p99. Độ trễ chủ yếu do chu kỳ gom mặc định (Spark trigger 5 s, ClickHouse flush ~7,5 s), chưa phải tốc độ xử lý. ClickHouse dùng CPU ít hơn khoảng 5 lần khi chạy liên tục.
- So sánh BR3 không hoàn toàn tương đương: Spark làm thêm phân loại cảm xúc VADER, ClickHouse không làm được bằng SQL. Vì vậy giữ Spark làm giải pháp chính cho BR3; ClickHouse MV hợp cho phần tách từ khóa nếu cần tiết kiệm tài nguyên.
- So với lần đo trên nền cũ (Spark 51,6 s với 2 core, executor 640 MB), Spark chậm hơn vì repo hiện giới hạn 1 core và executor 512 MB.

## Nhật ký
| Lần | Vấn đề | Xử lý |
|---|---|---|
| 1 (nền cũ `320140e`) | Kafka engine mặc định gom ~1 triệu dòng một khối; 200k review dài vượt 1 GB RAM, `MEMORY_LIMIT_EXCEEDED` 105 lần, xả mất 202 s | `kafka_max_block_size = 10000` |
| 2 (nền cũ) | Đo lại: ClickHouse 18,4 s, Spark 51,6 s; 199.998/200.000 dòng trùng `keywords` | Chuyển sang nền `main` và đo lại (lần 3) |
| 3 (nền `main`) | Spark không ghi được dòng nào sau 20 phút (timeout). Nguyên nhân: driver JVM của lần chạy demo trước vẫn sống và giữ đủ 2/2 core trên Spark master (`pkill -f spark-submit` chỉ giết launcher, không giết `org.apache.spark.deploy.SparkSubmit`), app benchmark không được cấp executor | Dừng driver bằng `pkill -f /opt/jobs/br`; script chờ master không còn app active trước mỗi lần submit và ghi log Spark vào `docs/result/br3/benchmark/logs/` |
| 4 (nền `main`) | Chạy đủ 3 lần. ClickHouse ghi 4 lỗi `MEMORY_LIMIT_EXCEEDED` lúc 17:12 UTC (lần xả BR3 đầu), nhưng thời gian lần đó (18,1 s) ngang các lần khác | Dùng số liệu lần này; RAM đỉnh BR3 ClickHouse chạm 1.024 MiB, nên tăng giới hạn nếu xả backlog lớn hơn |

## Hạn chế
- Một máy, 8 GB; kết quả phụ thuộc giới hạn RAM/CPU trong `docker-compose.yml`.
- Phần cảm xúc không so được vì ClickHouse không có VADER.
- Độ trễ dùng đồng hồ host (timestamp Kafka) và đồng hồ VM Docker (ClickHouse); nếu lệch thì lệch như nhau cho hai engine.
- 3 lần lặp chưa đủ để kiểm định thống kê.
