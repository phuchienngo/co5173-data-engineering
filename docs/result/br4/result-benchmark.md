# Benchmark BR4 - Revenue Monitoring

## So sánh gì
| | Giải pháp chính | Giải pháp thay thế |
|---|---|---|
| Kiểu | ETL: Kafka → Spark Structured Streaming → ClickHouse | ELT: ClickHouse tự đọc Kafka (`ENGINE = Kafka`) + Materialized View |
| Code | `jobs/br4_revenue_monitoring/job.py` (+ `bucket.py`) | `tests/br4/clickhouse_br4.sql` |
| Join dim | broadcast join `dim_listing_pricing` (từ `Dim_Property.csv`) | `INNER JOIN dim_listing_pricing` trong MV |
| Doanh thu | `coalesce(revenue của event, price * coalesce(nights_booked, 1))` | cùng công thức (`toFloat64OrNull`, `toInt32OrNull`) |
| Nhóm | `bucket_revenue`: low < 2000 ≤ medium < 8000 ≤ high (Python UDF) | `multiIf(rev < 2000, 'low', rev < 8000, 'medium', 'high')` |

BR4 làm lại được 100% bằng SQL nên đây là phép so sánh tương đương.

## Dữ liệu và cấu hình
- `data/br4/Fact_Bookings.csv` (50.000 booking) và `Dim_Property.csv` (1.500 property), Git LFS. Benchmark dùng toàn bộ 50.000 booking.
- Docker Desktop 8 GB, 8 CPU; giới hạn theo `docker-compose.yml`: ClickHouse 1 GB; Spark worker 2.500 MB, 2 core.
- Spark submit giống `scripts/start.py`: driver 512 MB, executor 512 MB, `spark.cores.max=1`, trigger 5 s; benchmark đặt `MAX_OFFSETS_PER_TRIGGER=10000` (demo giữ 500).
- ClickHouse Kafka engine: `kafka_max_block_size = 10000`, flush mặc định (~7,5 s).

## Tiêu chí
| Tiêu chí (theo đề) | Đo bằng |
|---|---|
| Đúng dữ liệu | nối 2 output theo `booking_id`: số dòng có `revenue` (sai số < 1e-6) và `revenue_bucket` giống hệt; số dòng mỗi bên |
| Hiệu năng - xả backlog | 50.000 message có sẵn trong Kafka: thời gian đến khi đủ dòng (sau inner join); rows/s; RAM đỉnh; CPU trung bình |
| Hiệu năng - chạy liên tục | 1.000 msg/s trong 60 s: độ trễ `ingested_at` − `event_time`, p50/p95/p99 |
| Hiệu quả khai thác | cả hai ghi vào ClickHouse, dashboard Grafana `br4-data-result` |

Mỗi phép đo lặp 3 lần, báo median [min - max].

## Chạy lại
Xem `docs/result/br3/result-benchmark.md` (một lệnh chạy cả hai BR).

## Kết quả (median [min - max] của 3 lần, nền `main`)

Xả backlog 50.000 booking (49.964 dòng sau inner join với `dim_listing_pricing`; 36 booking có `Property_ID` không có trong dim):

| Engine | End-to-end (s) | Dòng đầu tiên (s) | rows/s | RAM đỉnh (MiB) | CPU TB (%) |
|---|---|---|---|---|---|
| Spark | 27,3 [27,3 - 28,9] | 11,3 | 1.830 [1.732 - 1.833] | 2.688 [2.541 - 2.750] | 135 [132 - 138] |
| ClickHouse MV | 4,2 [4,2 - 4,2] | 4,2 | 12.024 [11.947 - 12.047] | 898 [621 - 919] | 10 [8 - 28] |

Chạy liên tục 1.000 msg/s trong 60 s (59.958/59.958 dòng tới đủ ở cả hai, cả 3 lần):

| Engine | p50 (ms) | p95 (ms) | p99 (ms) | RAM đỉnh (MiB) | CPU TB (%) |
|---|---|---|---|---|---|
| Spark | 3.087 [3.029 - 3.093] | 5.450 [5.403 - 5.456] | 5.770 [5.720 - 5.889] | 2.751 | 46 |
| ClickHouse MV | 3.984 [3.953 - 4.065] | 7.342 [7.274 - 7.346] | 7.641 [7.581 - 7.641] | 958 | 16 |

Độ đúng: 49.964/49.964 dòng có `revenue` và `revenue_bucket` giống hệt; mỗi bên 49.964 dòng, không trùng `booking_id`.

## Nhận xét
- Kết quả giống nhau 100%: logic BR4 (join + công thức doanh thu + chia nhóm) chuyển sang SQL của ClickHouse không mất gì.
- Xả backlog: ClickHouse MV nhanh hơn khoảng 6,6 lần, dùng khoảng 1/3 RAM và rất ít CPU. ClickHouse ghi cả 50k dòng trong một lần flush (dòng đầu và dòng cuối cùng lúc 4,2 s); Spark tốn ~11 s khởi động và chạy 1 core.
- Chạy liên tục: Spark có độ trễ thấp hơn ở mọi phân vị (p95 5,5 s so với 7,3 s), do trigger 5 s ngắn hơn chu kỳ flush mặc định ~7,5 s của ClickHouse. Hai giá trị này chỉnh được (Spark `trigger`, ClickHouse `kafka_flush_interval_ms`) nhưng chưa thử.
- Đánh đổi: ClickHouse MV gọn, rẻ tài nguyên, phù hợp logic SQL như BR4; Spark linh hoạt hơn (Python, ML, mở rộng nhiều worker) và có độ trễ đuôi thấp hơn ở cấu hình mặc định.

## Nhật ký
| Lần | Vấn đề | Xử lý |
|---|---|---|
| 1-2 (nền cũ `320140e`) | BR4 lúc đó dùng dữ liệu Airbnb (doanh thu ước tính); đo được ClickHouse 10,7 s vs Spark 21,8 s, kết quả trùng 100% | `main` đổi BR4 sang `Fact_Bookings.csv`, viết lại `clickhouse_br4.sql` theo công thức mới và đo lại |
| 3 (nền `main`) | Spark không ghi được dòng nào sau 20 phút (timeout). Nguyên nhân: driver JVM của lần chạy demo trước vẫn sống và giữ đủ 2/2 core trên Spark master (`pkill -f spark-submit` chỉ giết launcher, không giết `org.apache.spark.deploy.SparkSubmit`), app benchmark không được cấp executor | Dừng driver bằng `pkill -f /opt/jobs/br`; script chờ master không còn app active trước mỗi lần submit và ghi log Spark vào `docs/result/br3/benchmark/logs/` |
| 4 (nền `main`) | Chạy đủ 3 lần, không lỗi ở BR4 | Dùng số liệu lần này |

## Hạn chế
- Một máy, 8 GB; kết quả phụ thuộc giới hạn RAM/CPU.
- 50.000 booking là dữ liệu mẫu; phép đo live lặp lại các booking (cùng `booking_id`) để đủ 60.000 message.
- Độ trễ dùng đồng hồ host và VM Docker; lệch như nhau cho hai engine.
- 3 lần lặp chưa đủ để kiểm định thống kê; khoảng min-max của hai engine không chồng nhau ở mọi chỉ số chính.
