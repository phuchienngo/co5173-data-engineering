# BR2 result: rows dropped

Measured with `make br2-count` (`tests/br2/count_rows.py`, spark-submit on the standalone cluster, 1 core / 512m). The script reads `data/br2/raw` directly and reuses the filters of `jobs/br2_dynamic_pricing/pricing.py` (`read_trips`). It writes nothing to ClickHouse or to the lake.

## Row counts per step

| Step | Rows | Dropped at this step | Kind |
|---|---:|---:|---|
| Raw parquet (13 files, 2025-07 to 2026-07) | 51,005,857 | | |
| Pickup date outside the file's month | | 208 | data quality |
| `fare_amount` <= 0 | | 1,699,669 | data quality |
| `fare_amount` >= 500 | | 1,187 | data quality |
| `pu_location_id` null or not in 1..263 | | 88,453 | data quality |
| `do_location_id` null or not in 1..263 | | 265,419 | data quality |
| **Lake `fact_trips` (after normalization)** | **48,950,921** | | |
| Month outside the baseline range (2026-07 file) | | 3,485,604 | scope |
| `do_location_id` not in the 16 landmark zones | | 36,160,013 | scope |
| **Used for the hourly table (bookings)** | **9,305,304** | | |

Steps are applied in this order and counted sequentially, so the drops add up to the total. Null `pickup_datetime` and null `fare_amount` were both 0 rows. There is no `dropDuplicates` and no row-dropping join before the hourly stage.

## Totals

| | Rows | Share of raw |
|---|---:|---:|
| Raw | 51,005,857 | 100.00% |
| (a) Dropped for data quality | 2,054,936 | 4.03% |
| (b) Dropped by scope filtering (not bad data) | 39,645,617 | 77.73% |
| Total dropped (raw − used) | 41,700,553 | 81.76% |
| Used for hourly | 9,305,304 | 18.24% |

Cross-check: the in-memory lake count equals `read_trips().count()` = 48,950,921. The hourly output stays 134,462 zone-hours and 9,305,304 bookings.

## Where rows are dropped in the code

- `pricing.py:76-81`: the casts of pickup, PU, DO and fare are non-ANSI, so a bad type becomes null and is caught by the filters below.
- `pricing.py:82-83`: `date_format(pickup_datetime, "yyyy-MM") == month` drops a null pickup and a pickup outside the file's own month.
- `pricing.py:84-85`: `fare_amount > 0 AND fare_amount < 500`.
- `pricing.py:86-87`: `pu_location_id` and `do_location_id` between 1 and 263.
- `job.py:60-62` (also 94-95 and 98-100 in the benchmark path): `pickup_month` between `baseline_from` and `baseline_to`, and `do_location_id` in the 16 landmark zones. `--baseline-to` defaults to `2026-06` (`job.py:169`).

## Notes for the report (Table 3)

- **48,950,921 is the lake `fact_trips` count after normalization, not the raw count.** The raw count is 51,005,857. The difference, 2,054,936, is exactly the data-quality drops.
- The 9,305,304 bookings cover 12 months (2025-07 to 2026-06), because the 2026-07 file is outside the default baseline range.

Cell text if "rows in" is changed to the raw count 51.005.857:

> 41.700.553 dòng (2.054.936 lỗi dữ liệu: 1.699.669 fare ≤ 0; 265.419 DO zone không hợp lệ; 88.453 PU zone không hợp lệ; 1.187 fare ≥ 500; 208 sai tháng; 39.645.617 ngoài phạm vi: 36.160.013 ngoài 16 zone, 3.485.604 thuộc tháng 2026-07 không dùng cho baseline)

Cell text if "rows in" stays 48.950.921 and is relabelled as lake rows after normalization:

> 0 dòng lỗi; 39.645.617 dòng ngoài phạm vi (36.160.013 ngoài 16 zone; 3.485.604 thuộc tháng 2026-07). Trước đó 2.054.936 dòng lỗi đã bị loại khi chuẩn hóa từ 51.005.857 dòng raw.
