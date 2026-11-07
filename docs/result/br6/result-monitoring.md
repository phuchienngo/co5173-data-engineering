# BR6 Result: Monitoring and Run History

End-to-end test of [plan-monitoring.md](../../plan/br6/plan-monitoring.md) on 2026-10-24. Same setup as [result-pipeline.md](result-pipeline.md): Windows 11 laptop, Rancher Desktop (WSL2), 1 Spark worker (2 cores, 2 GB), ClickHouse 1 GB, all 15 cities at ×1. Figures come from `br6.run_history`, Prometheus and the Spark history server.

## The three runs and the import (plan row 3)

Route A: Jakarta, Emporium Pluit Mall → Gandaria City, General visitor, all place types. Run one after the other, with no Grafana page open.

| | Import | Spark, default | Spark, baseline preset | ClickHouse engine |
|---|---|---|---|---|
| Run time | 256.5 s | 26.1 s | 63.0 s | 1.9 s |
| Steps (s) | read 57.0, check 29.7, derive 28.4, write_raw 40.3, write_dims 47.3, write_fact 33.5, verify 1.3, place_stats 9.2, swap 0.1 | read 9.6, clean 1.6, match 5.1, score 0.0, write 0.3; start-up and other 9.5 | read 4.7, clean 15.3, match 33.3, score 0.0, write 0.2; start-up 9.4 | read 0.0, clean 0.1, match 0.2, score 0.0, write 0.2; other 1.4 |
| Spark CPU time | 185.8 s | 3.1 s | 20.1 s | – |
| Spark tasks | 6,047 | 19 | 1,014 | – |
| Shuffle written | 857 MB | 0.4 MB | 13.6 MB | – |
| Spilled (memory) | 2,917 MB | 0 | 0 | – |
| Executor / driver peak heap | 403 / 234 MB | 114 / 149 MB | 259 / 391 MB | – |
| ClickHouse rows read | 31.8 million | 213,762 | 486,598 | 427,524 |
| ClickHouse rows written | 6.0 million | 0 | 0 | 0 |
| ClickHouse CPU / peak query memory | 7.5 s / 91 MB | 0.04 s / 11 MB | 0.05 s / 12 MB | 0.18 s / 37 MB |
| **Container CPU** (Prometheus): spark-worker / br5-app / clickhouse | 428 / 79 / 43 s | 22.9 / 25.3 / 4.4 s | 100.0 / 36.3 / 11.3 s | 0.0 / 3.4 / 3.2 s |
| **Container peak memory**: spark-worker / br5-app / clickhouse | 1,486 / 963 / 724 MB | 695 / 832 / 642 MB | 1,087 / 1,181 / 631 MB | 251 / 275 / 623 MB |
| spark-worker samples in the window | 52 | 6 | 13 | 2 |

All three runs gave the same list (`route_compare.py`: SAME for default vs baseline and default vs ClickHouse engine): 7,178 places near the route, 10 suggested.

Container figures cover the run's window plus 5 s, so short runs include a little of what came before and after.

**Checked:**
- **Spark runs:** `spark_app_id` is set and CPU time > 0.
- **Per-step figures add up:** for runs, the per-step figures (read, clean, match) add up exactly to the totals (CPU, task time, tasks). For the import, they add up exactly over its seven Spark steps.
- **The baseline shuffles more** than the default: 13.6 vs 0.4 MB, and 1,014 vs 19 tasks.
- **The ClickHouse engine run** has ClickHouse figures only, and no Spark page.
- **The stored totals match the history server:** for the default run, the history server's stage figures give 3,113 ms CPU, 19 tasks and 8,661 ms task time, the same as the stored figures.

## Observations for the report

- **Spark's own CPU figure is a small part of a Spark run's cost.** The default run used 3.1 s of executor JVM CPU, but `spark-worker` used 22.9 s of CPU and `br5-app` 25.3 s. The difference is:
  - the executor's Python workers (the parallel reader)
  - JVM start-up for the executor
  - in the app container, the driver's start-up and the job's own Python
  
  The ClickHouse engine's whole run cost about 3 s of CPU in each of `br5-app` and `clickhouse`. Container CPU is the fair figure for comparing the engines; `spark.cpu_ms` is the one for comparing Spark settings.
- **The baseline uses about 4× the worker CPU of the default** (100 vs 23 s) and 2.4× the run time, for the same answer. Its driver peak heap is higher (391 vs 149 MB) because the driver reader brings 412,100 check-ins into the driver.
- **The import is the only memory-heavy job:** `spark-worker` reached 1.49 GB of its 2 GB, and the executors spilled 2.9 GB to memory during the import's shuffles. No run comes close.
- **Short runs:** the ClickHouse engine run fell in 2 samples of the 5 s scrape, as expected (plan H8). Its stored figures are exact.

## Checks (plan section 7)

| # | Check | Result |
|---|---|---|
| 1 | Unit tests | 28 passed (3 new: `settings_diff`). The view's parsing was checked by query on all 25 succeeded runs: `found` and `after_filters` equal `result_summary`, the counts per status add up to `found`, and `suggested` equals the stored result count |
| 2 | `check_dashboards.py --br 6`; the same without `--br` | BR6: 43 queries, 0 failed, 0 empty. BR5: 37 queries, 0 failed; 2 empty (no BR5 run in that dashboard's default time range) |
| 3 | Three runs and an import | Above |
| 4 | M7 during a Spark run | The ×5 default run of the pipeline test (dashboard zoomed to its window): `spark-worker` CPU rises inside its shaded region, memory stays under the 2 GB limit, `br5-app` memory rises with the driver |
| 5 | Compare by `pipeline.matching` and `pipeline.distance` (runs, last 7 days) | Grafana: all 7, grid 22; native 21, pandas_udf 1, python_udf 7. A direct query on `pipeline_runs` gives the same counts |
| 6 | M8 against the app | Latest run (the ClickHouse engine run): excluded 78,909 no location, 5 far from the city, 119,715 not offered; 7,178 near the route; first suggestion Central Park. The Route Finder shows the same |
| 7 | Run History comparisons | Default vs baseline: the settings that differ are exactly the baseline preset's 8 keys, plus Spark's 2 effective values and "settings from"; Same result: yes. Two default runs: no setting differences; Same result: yes (warned "they read different imports"). Spark vs ClickHouse engine: Same result: yes. Two runs either side of the grouping reload test: the grouping warning, the grouping keys among the settings that differ, and the first differences |
| 8 | Links | Sidebar on BR6 pages: three Grafana dashboards and Spark history. The selection pages show both BRs' groups. The Route Finder result links to Resource use (its time window) and to Run History. Grafana run tables link to `/route_run_history?run=<id>`. The history server lists "BR6 import" and "BR6 route run" next to `br5_analysis`. BR5 pages return as before |
| 9 | `docker compose … down`, then `make br6-up` | All 42 rows of run history, the Spark history pages, the Prometheus series from before the restart, and all dashboard queries are still there |
