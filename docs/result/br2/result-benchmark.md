# Benchmark BR2 - Dynamic Pricing

Full report with charts and the product-owner walkthrough: [../br1-br2-benchmark-report.html](../br1-br2-benchmark-report.html).

## Purpose
Measure the cost of the capacity baseline when it reads the raw monthly TLC parquet files versus the normalized lake, and check that both give the same result.

## What is compared
The baseline: 12 months (2025-07 to 2026-06), 16 landmark zones, P95 of hourly bookings per zone, day type and hour.

| | Raw files | Partitioned lake |
|---|---|---|
| Input | Raw monthly TLC parquet files | Normalized lake, partitioned by `service` and `pickup_month` |
| Code | `jobs/br2_dynamic_pricing/job.py`, `run_bench`, variant `raw` | same, variant `lake` |

## Setup
- 2026-11-08. macOS host, OrbStack Docker 29.8, 8 CPUs, 8 GB.
- Spark 4.0 standalone, 1 worker (2 cores, 2 GB). The job gets 1 core, 512 MB driver, 512 MB executor, `spark.sql.shuffle.partitions=4`.
- ClickHouse 26.8 (1 GB), Grafana 13.2.3.
- Nothing else ran on the Spark cluster. 3 repetitions; medians with min-max.
- Order alternates raw,lake / lake,raw / raw,lake to limit OS page-cache bias.
- Results go to `pricing.job_run`. The benchmark does not touch serving data. `--bench-lake-build` rewrites the lake partitions (same data), so do not run it while the replay producer is reading.

## Results
Median seconds (min-max) of 3 repetitions:

| Stage | Raw files | Lake | Speedup |
|---|---|---|---|
| hourly | 13.66 (11.47-14.45) | 6.71 (6.65-7.07) | 2.04x |
| capacity | 0.61 (0.42-0.81) | 0.35 (0.27-0.77) | |
| **total** | **14.08** (12.08-15.26) | **7.42** (6.98-7.42) | **1.90x** |

Speedup column = ratio of medians; headline = median of per-rep ratios.

Per repetition, total:

| Rep | Raw (s) | Lake (s) | Speedup |
|---|---|---|---|
| 1 | 12.08 | 6.98 | 1.73x |
| 2 | 14.08 | 7.42 | 1.90x |
| 3 | 15.26 | 7.42 | 2.06x |

Lake times were stable (6.98-7.42 s) while raw times varied more (12.08-15.26 s); the cause of the raw variation was not isolated (raw ran first in reps 1 and 3, so it is not a simple cache warm-up effect). So the lake is also more predictable.

## Why the lake is faster (`explain()`, `logs/bench/br2-rep1.log`)
- Lake plan: `PartitionFilters: [(service = yellow), (pickup_month >= 2025-07), (pickup_month <= 2026-06)]`.
- Lake plan: `PushedFilters: [In(do_location_id, [16 zone ids])]`. It reads only `[pickup_datetime, do_location_id, fare_amount, service, pickup_month]`.
- Raw plan: 12 separate FileScans, one per monthly file. Pushed filters are only `IsNotNull(...)`. No partition pruning; the zone filter is not pushed down.

## Correctness
Identical in all 6 runs:
- Hourly aggregate: 134,462 zone-hours, 9,305,304 bookings.
- Capacity: 768 slots, checksum 75,940.

## Storage and build cost
- Raw: 873 MB in 13 files. Lake: 460 MB in 26 parquet files (53% of raw).
- One-off lake build (rep 1): 44.53 s, 48,950,921 trip rows (13 months).
- Break-even if the lake were built only for this benchmark: 44.53 / (14.08 - 7.42) = about 7 rebuilds.
- Pipeline nuance: the real pipeline writes this lake anyway, because the replay producer reads it. The lake is therefore not an extra cost there.

## Business query latency
Grafana panel queries on `br2-advisor`, each run 10 times against ClickHouse (ms):

| Panel | p50 | p95 | max | Rows |
|---|---|---|---|---|
| Surge zone-hours | 18.8 | 20.9 | 21.7 | 1 |
| Share of hours in surge | 15.6 | 18.4 | 18.8 | 1 |
| Extra fare revenue | 18.4 | 19.9 | 20.1 | 1 |
| Average demand ratio | 17.0 | 22.7 | 25.6 | 1 |
| Price suggestion per landmark | 22.7 | 33.3 | 40.3 | 16 |
| Demand ratio by hour of day | 24.2 | 26.7 | 26.9 | 16 |
| Best hours to raise prices | 22.6 | 24.0 | 24.5 | 1 |
| Bookings vs capacity | 30.5 | 35.1 | 35.1 | 9,503 |

Every question answers in well under 0.1 s at the median (p50 16-31 ms).

## Streaming observation
From the live demo, not a controlled benchmark:
- Average input 1,496 rows/s (producer-bound); processing capacity 21,627 rows/s, about 14x headroom.
- Average batch latency 530 ms.
- The July 2026 replay produced 11,534 zone-hour rows on top of 134,462 history rows.

## Conclusions
- The lake makes the baseline 1.90x faster (hourly 2.04x), with identical output, 47% less storage and steadier times.
- The gain comes from partition pruning, filter push-down and column pruning, which the raw files cannot use.
- The streaming job has about 14x headroom over the replay rate.

## Reproduce
```bash
bash scripts/download_tlc.sh yellow 2025-07 2026-07
make demo-br BR=2        # data
make br2-bench           # tests/br2/benchmark.py, REPS=3 default
python3 tests/br5/check_dashboards.py --br 2
```
Raw data: [benchmark/runs.csv](benchmark/runs.csv), [benchmark/queries.csv](benchmark/queries.csv). Logs: `logs/bench/`.

## Screenshots
- [Advisor, default rule](screenshots/br2-advisor-default.jpg)
- [Weekend rule, 60% / 25%, history](screenshots/br2-advisor-weekend-60-25-history.jpg)
- [Benchmark panels (`/d/br2-pricing`, bottom)](screenshots/br2-benchmark-panels.jpg)

## Threats to validity
- 3 repetitions on one laptop.
- OS page cache; reduced by alternating the order.
- p95 comes from 10 samples.
- Streaming figures come from the demo, not a controlled run.
- The revenue what-if assumes no demand loss after a price rise.
