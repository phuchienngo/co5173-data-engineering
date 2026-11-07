# BR5 Result: Data Processing Pipeline

End-to-end test of [plan-pipeline.md](../../plan/br5/plan-pipeline.md) on 2026-10-18. Figures come from `br5.pipeline_runs`, `br5.run_month` and `br5.run_card`. This is the correctness check with a few timings; the full benchmark matrix (repeated runs, resource use) is left for the monitoring step.

**Setup:** as in [result-data-import.md](result-data-import.md): Spark 1 worker (2 cores, 2 GB), ClickHouse (1 CPU, 1 GB). Default settings unless a preset is named.

**Timing scope:** run time starts when the job's Python process starts. It includes creating the Spark session, but not the `spark-submit` launch before it.

## What the data says (All hotels, all countries, all room types, ×1)

| Figure | Value |
|---|---|
| Bookings read | 119,390 |
| Excluded | canceled 44,224 · zero nights 680 · zero guests 97 · duplicates 0 (kept by default) |
| Used | 74,389 (read = used + excluded, exactly) |
| Kept but noted | country unknown 416 · price 0 or below (left out of average price) 1,001 |
| Countries / room types offered (≥ 100 valid bookings) | 33 of 177 countries · 8 of 10 room types |
| Overall low season (60% threshold) | **Dec – Jan: demand is 52% of peak**. Peak is May (11,796 room-nights per month); Nov is just above the threshold at 62% |
| Room type × country targets | 573 found, 68 with enough data and a low season, top 10 shown |
| Top card | Room A · United Kingdom: Aug and Nov – Feb low; potential 1,036 room-nights per month; early-bird (book ≥ 50 days ahead) + strong discount |

## Run time (seconds)

| Run | Total | read | clean | aggregate | profile | other steps |
|---|---|---|---|---|---|---|
| Spark, ×1 (3 runs) | 31.6 – 35.2 | 10.5 – 11.9 | 1.6 – 1.7 | 2.5 – 2.7 | 5.5 – 6.8 | < 0.3 |
| Spark, ×10 | 38.2 | 12.8 | 1.7 | 2.9 | 8.0 | 0.2 |
| ClickHouse SQL engine, ×1 (2 runs) | 2.1 – 2.4 | 0 | 0.1 – 0.2 | 0.1 | 0.1 | 0.4 |
| ClickHouse SQL engine, ×10 | 2.6 | 0 | 0.2 | 0.2 | 0.4 | 0.1 |
| City Hotel · Portugal · room A, Spark default | 30.9 | 11.0 | 1.3 | 2.1 | 5.6 | 0.1 |
| Same choice, preset **baseline** | **78.1** | 5.2 | 25.2 | 12.6 | 23.7 | 0.1 |

The steps don't add up to the total: the remaining 10–12 s of each Spark run is creating the Spark session (the application and its executor) before the first step.

## Import time with the new settings

The import now uses the per-run settings too. With Spark's default of 200 shuffle partitions (the import used a fixed 4 before), it got slower:

| Import | Before (4 partitions) | Now (200, AQE on) | Step that grew most |
|---|---|---|---|
| ×1 | 62 – 73 s | 93 s | write_dims: 4 – 6 → 23 s |
| ×10 | 110 – 112 s | 153 s | write_dims: 5 → 27 s |

## Observations for the report

- **The ClickHouse engine is about 15× faster than Spark here** (2.1 s vs 31.8 s at ×1, 2.6 s vs 38.2 s at ×10). Spark's fixed cost dominates: about 11 s to create the session and about 10 s for the first read with caching. This is the "pushdown to the warehouse" alternative: for aggregations over data that already sits in a column store, sending the query to the data wins at this size.
- **×10 data costs Spark only 6.6 s more** (31.8 → 38.2 s), and ClickHouse 0.5 s more. Neither engine is near its limits at 1.2 M rows; ×50 is the next point to measure.
- **The naive baseline preset is 2.5× slower** (78.1 vs 30.9 s on the same choice). The baseline uses the driver reader, joins the lookup tables before aggregating, has broadcast and AQE off, and doesn't cache. Every lookup join becomes a sort-merge join (4 in the aggregate plan, 5 in the profile plan), and clean, aggregate and profile each read the data again, because nothing is cached.
- **Spark's default of 200 shuffle partitions slows the import by about 40%.** It mostly hits the small `distinct()` queries that build the dimensions (write_dims, 5 → 23 s). This is a ready-made tuning example for the report: set shuffle partitions to 4–8, or rely on AQE coalescing, and measure again.
- **Synthetic data doesn't change the answer.** ×10 gives exactly the same cards and monthly figures as ×1, because figures are divided by the multiplier.

## Behaviour verified

| Check | Result |
|---|---|
| Unit tests (rules and settings) | 43 pass (`make br5-test`) |
| Spark engine vs ClickHouse engine, All / All / All | Same 10 cards and 132 month rows (`compare_runs.py`) |
| Same run twice | Identical results |
| ×1 vs ×10 | Identical results |
| Default vs baseline on City Hotel · Portugal · A | Identical results |
| Settings take effect | The snapshot shows the baseline values. Spark itself reports broadcast -1 and AQE off. The logged plans use sort-merge joins for all 9 joins. With defaults there is a single join (in the profile step), and it is a broadcast join |
| Validation | Executor memory 4g is blocked with "is more than the worker has (2g)". "abc" for total cores is rejected and not saved |
| Presets | Save, load (inputs update), delete; Reset per setting and Reset all |
| Thin target (Resort Hotel · Austria · room G) | "Not enough data to recommend: 11 valid bookings for this choice (minimum 100)" |
| Busy lock | A CLI run during an app run is refused as busy |
| Stop | The run ends in 1–4 s as "Stopped by the user". The lock clears at once, and no Spark application or process is left |
| CLI | `scripts/br5.py run` and `import` go through the same launcher as the app (including a ×10 and a ×1 re-import) |

## Not yet measured

- ×50, the parallel vs driver reader on its own, cache on/off on its own, and skew handling. These belong in the benchmark matrix with resource figures (monitoring plan).
- BR3/BR4 were not run end to end. The only shared change is the optional `conf` argument of `get_spark_session`, and `start.py` was not changed.
