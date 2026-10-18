# BR5 Result: Data Import

End-to-end test of [plan-data-import.md](../../plan/br5/plan-data-import.md) on 2026-10-18. Every figure below comes from `br5.pipeline_runs` and `system.parts`.

**Setup:**
- Windows 11 laptop, Rancher Desktop (WSL2).
- Spark standalone: 1 worker (2 cores, 2 GB); the import job uses `spark.cores.max=2`, 512 MB driver and executor, 2 write partitions.
- ClickHouse 26.8 (1 CPU, 1 GB).

Job time excludes `spark-submit` start-up (about 15–30 s).

## Data

| Figure | Value |
|---|---|
| Source file | `hotel_bookings.csv`, 16.9 MB, 119,390 rows, 32 columns |
| Empty values kept ("NULL"/"NA") | country 488, children 4, agent 16,340, company 112,593 |
| Exact duplicate rows (flagged, not removed) | 31,994 (26.8%) |
| Dimension rows | date 1,583 · hotel 2 · country 178 (incl. Unknown) · room type 12 · channel 26 · booking profile 108 |
| Checks | raw = fact = rows × multiplier; 0 orphan keys; `sum(adr)` and nights equal in raw and fact; values match pandas on the CSV |

## Import time (seconds)

| Run | Runs | Total | read | check | derive | write raw | write dims | write fact |
|---|---|---|---|---|---|---|---|---|
| ×1, parallel writer | 4 | 62–73 (avg 69) | 9–15 | 4–7 | 17–22 | 5.5–6.4 | 4–6 | 6.7–7.8 |
| ×1, pandas writer (baseline) | 1 | 68 | 13 | 5 | 20 | 6.9 | 4.4 | 7.9 |
| ×10, parallel writer (1,193,900 rows) | 2 | 110–112 | 12–14 | 5 | 22 | 36–38 | 5–5.5 | 20–21 |

Verify and swap take under 1 s at both sizes.

## Storage in ClickHouse (×1)

| Table | Rows | Compressed | Uncompressed |
|---|---|---|---|
| raw_booking | 119,390 | 2.14 MiB | 7.02 MiB |
| fact_booking | 119,390 | 2.61 MiB | 7.62 MiB |
| all 6 dimensions | 1,909 | 15 KiB | 30 KiB |

## Observations for the report

- **At ×1, fixed costs dominate.** Read, check and derive take about 40 s, and writing only about 13 s. Writer comparisons need the multiplier to show a real difference: at ×1, pandas and parallel are within about 1 s.
- **×10 data took only about 1.6× the time** (69 → 111 s). The derive step barely grows (about 20 → 22 s), because duplicates and dimensions are computed once on the original rows, before multiplying.
- **The fact table writes faster than the raw table at ×10** (21 vs 37 s for the same rows). The fact table's columns are mostly small integer keys, while the raw table has many text columns.
- **The star schema isn't smaller on disk here.** The fact table compresses to 2.61 MiB vs 2.14 MiB for raw. ClickHouse's `LowCardinality` text already compresses repeated values well, and the fact table adds 3 date keys. Its benefit has to be shown in query speed, which the analysis step will measure.
- **Compression:** the 16.9 MB CSV is stored as about 2.1 MiB in ClickHouse (about 8× smaller).

## Behaviour verified

- **Persistence:** data survives stopping and restarting the whole stack.
- **Fresh mode:** drops only BR5 data; other databases and the run history are kept.
- **Busy lock:** a second import is refused while one runs, whether started from the command line or the app.
- **Bad input:** a file with a bad value fails at the check step with the column named ("lead_time: 1"), and the stored data is untouched.
- **App:** the Data page shows status, asks for confirmation before replacing, and shows write progress (for example 450,000 of 1,193,900 rows).

## Not yet measured

- The pandas writer at ×10 and ×50, where it is expected to run out of driver memory.
- Grafana resource panels (monitoring plan).
