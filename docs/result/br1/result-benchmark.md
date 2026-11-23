# Benchmark BR1 - Tour Route Optimization

Full report with charts and the product-owner walkthrough: [../br1-br2-benchmark-report.html](../br1-br2-benchmark-report.html).

## Purpose
Measure how much the optimized solver saves over the naive one on the same 20,000 tour requests, and check that both give the same tours.

## What is compared
| | Naive | Optimized |
|---|---|---|
| Per-request solve | Python row UDF | `mapInPandas` with numpy haversine |
| Distances | Shuffle join with the `dist_matrix` parquet | Coordinates broadcast, computed locally |
| Request building | Shuffle joins | Broadcast joins |
| Spark settings | AQE off, 200 shuffle partitions | AQE on, 8 shuffle partitions, Arrow |

Details: `docs/br1/tour-route.md`, section 6. Code: `jobs/br1_tour_route/job.py` (`--bench-only`).

## Setup
- 2026-11-08. macOS host, OrbStack Docker 29.8, 8 CPUs, 8 GB.
- Spark 4.0 standalone, 1 worker (2 cores, 2 GB). The job gets 1 core, 512 MB driver, 512 MB executor, `spark.sql.shuffle.partitions=4`.
- ClickHouse 26.8 (1 GB), Grafana 13.2.3.
- Nothing else ran on the Spark cluster. 3 repetitions; medians with min-max.
- The benchmark never touches tour.fact_tour / tour.fact_tour_leg (the tours the Tour finder reads). Its timed sink_clickhouse stage truncates and reloads tour.dim_landmark and tour.dist_matrix with identical data, so for about 18 s per repetition the landmark lookups and map can be empty - run it when nobody is using the dashboards.

## Results
Median seconds (min-max) of 3 repetitions, 20,000 requests (4-8 stops, 3 cities):

| Stage | Naive | Optimized | Speedup |
|---|---|---|---|
| requests | 9.407 (9.286-10.152) | 0.750 (0.683-0.759) | 12.5x |
| solve | 41.914 (41.520-43.460) | 2.351 (2.305-2.430) | 17.8x |
| **total** | **52.066** (50.806-52.867) | **3.064** (3.034-3.180) | **17.0x** |

Speedup column = ratio of medians (17.0x = 52.066 / 3.064); median of per-rep ratios = 16.75x.

Per repetition, total:

| Rep | Naive (s) | Optimized (s) | Speedup |
|---|---|---|---|
| 1 | 50.806 | 3.034 | 16.75x |
| 2 | 52.867 | 3.180 | 16.62x |
| 3 | 52.066 | 3.064 | 16.99x |

One repetition took 85.8, 85.8 and 85.6 s of wall time.

Optimized-only warm-up (median, min-max). It is not in "total", which covers requests and solve only:

| Stage | Seconds | Rows |
|---|---|---|
| landmarks | 4.420 (4.078-4.610) | 1,954 |
| dist_matrix | 3.098 (2.777-3.208) | 1,746,644 pairs |
| sink_clickhouse | 18.094 (17.756-18.467) | 1,748,598 written |

## Correctness
Naive and optimized outputs compared row by row from the lake (`data/br1/lake/fact_tour/mode=naive` and `mode=optimized`):
- 20,000 tours in both.
- `total_cost_vnd` equal on all 20,000 tours; sum 9,444,273,402 VND in both.
- `total_km`: maximum absolute difference 9e-13.

## Isolation matters
Two same-day runs while BR2 shared the 2-core worker gave naive 79.5 s and 72.2 s, optimized 3.35 s and 5.20 s (speedups 23.8x and 13.9x). Isolated runs give 16.6-17.0x. Report the isolated figure.

## Business query latency
Grafana panel queries on `br1-finder`, each run 10 times against ClickHouse (ms):

| Panel | p50 | p95 | max | Rows |
|---|---|---|---|---|
| Matching tours | 9.5 | 69.7 | 95.0 | 1 |
| Cheapest tour | 10.9 | 12.4 | 13.1 | 1 |
| Median tour cost | 11.1 | 23.9 | 31.0 | 1 |
| Shortest tour | 8.7 | 49.7 | 54.9 | 1 |
| Tours within budget | 15.7 | 52.3 | 54.2 | 100 |
| Cheapest tour per number of stops | 12.2 | 16.9 | 19.5 | 5 |
| Leg-by-leg cost | 6.2 | 8.1 | 8.4 | 4 |
| Route on the map | 7.4 | 11.8 | 12.1 | 4 |
| Average cost split | 10.9 | 12.6 | 13.5 | 5 |

Every question answers in well under 0.1 s at the median (p50 6-16 ms). The p95 spikes up to 95 ms are most likely first-call and Grafana overhead.

## Streaming observation
From the live demo, not a controlled benchmark: peak 2,743 requests/s, average micro-batch latency 399 ms.

## Conclusions
- Optimized is faster in total by a median of the per-rep speedups 16.75x (range 16.62-16.99x); ratio of medians: solve 17.8x, requests 12.5x. It gives the same 20,000 tours.
- The ClickHouse sink (18.1 s) now takes about 6 times the whole optimized compute (3.06 s). Next target: the repo's parallel `write_partitions` writer or native-format inserts.
- Business questions are interactive: p50 at most 16 ms.

## Reproduce
```bash
make demo-br BR=1        # data
make br1-bench           # tests/br1/benchmark.py, 3 reps
python3 tests/br5/check_dashboards.py --br 1
```
Raw data: [benchmark/runs.csv](benchmark/runs.csv), [benchmark/queries.csv](benchmark/queries.csv). Logs: `logs/bench/`.

## Screenshots
- [Tour finder, HCMC](screenshots/br1-finder-hcmc.jpg)
- [Hanoi, 5 stops, up to 500,000 VND](screenshots/br1-finder-hanoi-5stops-500k.jpg)
- [Must-see stop](screenshots/br1-finder-must-visit.jpg)
- [Benchmark panels (`/d/br1-tour`, bottom)](screenshots/br1-benchmark-panels.jpg)

## Threats to validity
- 3 repetitions on one laptop.
- "Total" excludes the optimized warm-up stages.
- p95 comes from 10 samples.
- Streaming figures come from the demo, not a controlled run.
- Speedup depends on isolation (see above).
