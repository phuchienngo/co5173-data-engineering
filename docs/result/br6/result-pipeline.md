# BR6 Result: Route Finder Pipeline and Settings

End-to-end test of [plan-pipeline.md](../../plan/br6/plan-pipeline.md) on 2026-10-24. Every figure below comes from `br6.pipeline_runs`, `br6.run_place`, `system.part_log` and the job logs.

**Setup:**
- Windows 11 laptop, Rancher Desktop (WSL2).
- Spark standalone: 1 worker (2 cores, 2 GB). Default settings unless stated: 512 MB driver and executor, AQE on, grid matching (1 km squares), native distances, place statistics from the import.
- ClickHouse 26.8 (1 GB container), with `system.metric_log` turned off (see "ClickHouse memory" below).
- Data: all 15 cities, ×1 (2,629,541 check-ins) unless stated; ×5 = 13,147,705.

Run time includes Spark start-up (about 12–15 s of each Spark run), as the user sees it.

## Test routes

| Route | City | Points | Places near the route (1 km) |
|---|---|---|---|
| A | Jakarta (most places with a location: 49,291) | Emporium Pluit Mall (venue 1549) → Gandaria City (1373), 13.0 km | 7,178 |
| B | Istanbul (most check-ins: 544,471) | Eminönü (2005) → stop Ortaköy (3982) → Fatih Sultan Mehmet Köprüsü (100), 11.4 km | 4,574 |
| C | Beijing (thinnest data) | IKEA (40) → Joy City Chaoyang (409) | 61 |

## Correctness

| Check | Result |
|---|---|
| Counts add up (route A) | 412,100 read = 213,471 used + 78,909 no location + 5 far from the city + 119,715 Not offered type. Read = Jakarta's check-ins |
| Counts add up (route B) | 544,471 read = 306,019 used + 62,621 + 5 + 175,826 |
| Grid vs every place (C9) | Same list (`route_compare.py`: same places, statuses, ranks, distances, scores and counts) |
| Native vs `pandas_udf` vs Python UDF (C9) | Same list, all three, with the grid and without it |
| Statistics at import vs every run (F4) | Same list and the same check-in counts |
| Parallel vs driver reader | Same list |
| Spark vs ClickHouse engine (C10) | Same list, with either statistics mode |
| Same run twice | Same list |
| ×1 vs ×5 data | Same list and counts (default settings, statistics every run, and both ClickHouse modes) |
| Chosen visitor vs General visitor (route B, visitor 88647: 40 of 87 used check-ins outdoors) | Different rankings: 6 of 10 suggestions are Nature & outdoors for the visitor, 3 for General visitor. Fit values match a hand calculation (Food & drink 28/40 = 0.7, Shopping 5/40 = 0.125); earlier visits are shown but don't change the score |
| Few places (route C) | 4 suggestions; 33 places nearby have fewer than 3 visitors. With only *Sights & culture*: "No places within the allowed detour for these choices. 37 places of other place types are near the route: try choosing more place types." |
| Grouping reload (D13, F9) | Moving *Shopping Mall* to Not offered in the file: the next run reloaded it, the snapshot's grouping hash changed, and the 32 malls near route A became "not offered". After restoring the file, the next run reloaded it back and gave the original list |

## Run time per option (route A, seconds)

One run each, so differences under about 3 s are noise.

| Settings | ×1 total | read | clean | match | Spark CPU | ×5 total | ×5 read |
|---|---|---|---|---|---|---|---|
| **default** (grid, native, statistics from the import, parallel reader) | 30.5 | 11.0 | 1.5 | 6.9 | 2.9 | 31.6 | 12.0 |
| every place (no grid) | 26.6 | 9.9 | 1.9 | 4.8 | 2.9 | | |
| grid, `pandas_udf` | 35.0 | 12.6 | 2.1 | 8.2 | 3.9 | | |
| grid, Python UDF | 33.5 | 10.9 | 2.0 | 7.8 | 3.8 | | |
| every place, Python UDF | 32.9 | 11.2 | 1.5 | 7.0 | 4.3 | | |
| statistics every run (parallel reader) | 51.7 | 25.0 | 4.4 | 10.2 | 14.3 | 52.7 | 29.0 |
| statistics every run, driver reader | 52.7 | 26.6 | 4.1 | 8.6 | 12.9 | failed: driver out of memory | |
| **ClickHouse engine** (statistics from the import) | 1.7 | 0.0 | 0.1 | 0.2 | – | 1.8 | |
| ClickHouse engine, statistics every run | 2.3 | 0.0 | 0.1 | 0.3 | – | 2.2 | |
| **baseline preset** (every place, Python UDF, statistics every run, driver reader, no cache, broadcast off, AQE off) | 67.2 | 5.5 | 16.2 | 34.0 | 20.4 | failed: driver out of memory | |

Routes B and C with the defaults: 25.3–26.8 s and 22.7–26.1 s. The visitor step adds 0.7–1.0 s.

**The match step in Spark's own figures** (executor CPU / summed task time, ms):

| | native | Python UDF | `pandas_udf` |
|---|---|---|---|
| grid | 885 / 4,570 | 1,490 / 8,442 | 1,619 / 9,506 |
| every place | 1,137 / 4,101 | 1,978 / 8,834 | |

**Import:** the new `place_stats` step takes 9.2 s at ×1 and 10.2 s at ×5 (whole import 257.5 s and 408.5 s). It saves about 20 s on every run that would otherwise count the statistics (route A: 51.7 → 30.5 s), so it pays off from the first run.

## Observations for the report

- **Pre-computing the place statistics at import is the optimization that counts.** It makes a run 40% faster (51.7 → 30.5 s at ×1, 52.7 → 31.6 s at ×5), cuts executor CPU by 5× (14.3 → 2.9 s) and makes the run independent of the data size: at ×5 the default run reads 205k ClickHouse rows instead of 4.2 million. The cost is 9–10 s once per import.
- **Spark start-up dominates a Route Finder run.** With the defaults, Spark's own work is under 3 s of CPU in a 30 s run. The ClickHouse engine answers the same question in 1.2–2.3 s, 15–25× faster, including at ×5. As in BR5, Spark only earns its start-up cost when the data per run is large.
- **The grid saves about a quarter of the matching CPU** (885 vs 1,137 ms native; 1,490 vs 1,978 ms with the Python UDF), but not wall-clock time: at 49k places and one leg, the match step's 5–8 s is mostly Spark job scheduling. This is what the plan expected for native distances (F6). It also holds with the Python UDF, which the plan expected to make the grid visible. The grid would matter with far more places per city than this data has.
- **Python UDFs about double the executor time of the match step** (summed task time 4.1–4.6 s native vs 8.4–9.5 s with either UDF). `pandas_udf` is no faster than the plain Python UDF here, because each batch is small (a few thousand places per task).
- **The driver reader can't handle ×5:** reading Jakarta's 2.06 million check-ins (×5) through the 512 MB driver ran out of Java heap, in the "statistics every run, driver reader" variant and in the baseline preset. Both runs were recorded as failed with the error, and the stored data was not touched. The parallel reader handled the same read.
- **The baseline preset is 2.2× slower at ×1** (67.2 vs 30.5 s). Most of the difference is the clean and match steps (16.2 + 34.0 s vs 1.5 + 6.9 s): with AQE off, each aggregation runs 200 shuffle partitions for a few thousand rows, and the sort-merge joins and the Python UDF add to that.

## ClickHouse memory: a stack problem found during the test

The first three re-imports failed their verify step: `raw_checkin`, `dim_place` or `fact_checkin` had too many rows (+450,000, +1,250,000, +950,000, always whole 50,000-row batches). The cause:
- An insert from the parallel writer failed with "(total) memory limit exceeded: would use 923 MiB". ClickHouse keeps itself under 90% of its 1 GB container. Spark then retried the task, and the retry inserted the batches the failed attempt had already written again.
- The inserts themselves used 10–26 MiB. The memory was taken by `system.metric_log`, ClickHouse's own metrics history: it has 2,110 columns, and each of its background merges peaked at 730 MiB, 10–18 times a minute. This was already so the day before (635 merges, 728 MiB peaks). The earlier imports passed only because no merge happened to coincide with an insert.
- **Fix:** `docker-compose.yml` turns `metric_log` off (config `clickhouse-system-logs-xml`). Nothing in the project reads it: Grafana scrapes ClickHouse's Prometheus endpoint, and the run figures read `system.query_log`. After the change, three imports in a row succeeded (×1, ×5, ×1).
- **Effect on BR5's benchmark figures:** BR5's reference cell (S2, All/All/All, `bench-default`) and the ClickHouse engine were re-run with the log off: 3 interleaved runs each after a warm-up, no app page open. The ClickHouse engine took 2.2–2.4 s, as stored (2.1–2.5 s). ClickHouse's time inside a Spark run was a little lower (query time 0.6–0.7 s vs 0.8–1.4 s, the same ClickHouse CPU). The Spark runs took 34.3–36.0 s vs 30.7–33.3 s stored, but the difference is in Spark's own executor CPU (8.9–10.0 s vs 8.0–9.0 s) and start-up, which the log can't affect, so it is laptop variation between sessions. Comparisons should use runs from the same session. These runs are in `br5.pipeline_runs` (2026-10-24) but not in the benchmark manifest.
- **For the report:** the import's verify step did its job. Each broken import was marked failed, and the stored data stayed as it was. The writer itself is still not idempotent: a retried task re-inserts its rows. Making it idempotent (insert deduplication tokens) is a possible improvement for both BRs.

## Behaviour verified

- **App:** the Route Finder pre-selects the latest run's city and shows its result. The search boxes find places (39 matches for "monas", more than 50 for "senayan city"). Stops can be added and removed; a removed stop's later stops move up. The visitor list shows check-ins and favourite types. **Find stops** ran route Monas → Grand Indonesia → Senayan City with visitor 42779 in 34.6 s, with progress per step, the live Spark link and Stop. The cards, both expanders and the CSV button render with no errors.
- **Advanced Settings (BR6):** all groups, defaults and current values, the read-only category table and the "Needs restart" values. An out-of-range square size (0.04) is rejected with a message; 0.5 is saved to `br6.settings` only (BR5's settings unchanged) and marked changed; Reset restores it.
- **Settings checks (C4):** weights adding up to 110% and executor memory 4g are both refused before Spark starts, with a clear message.
- **Busy lock and Stop (G7, F10):** a second run during a run is refused with "Busy: a BR6 run is in progress (step: read)". Stop (the app's `route_launch.stop`) records "Stopped by the user" and the lock clears at once.
- **Snapshot (C6):** the baseline run's snapshot holds its values and Spark's effective ones (broadcast −1, AQE false), the preset name and the grouping hash. Its logged plans show a `SortMergeJoin` and the Python UDF (`BatchEvalPython`).
- **Run History and Data:** runs are listed next to imports; the Data screen shows the place statistics line.
- **BR5:** one analysis run (City Hotel, PRT, room A) succeeded as before.
- **Unit tests:** `make br6-test`, 25 tests passed (13 new rules tests, among them the grid's never-loses-a-candidate check on 40 random routes).

## Not yet measured

- Repeated runs (3 or more per option) for medians, and route B and C per option: with the monitoring plan's benchmark.
- The baseline preset at ×5 with the parallel reader, to separate the driver's memory limit from the other baseline choices.
