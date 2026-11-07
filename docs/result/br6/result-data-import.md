# BR6 Result: Data Import

End-to-end test of [plan-data-import.md](../../plan/br6/plan-data-import.md) and the shared app skeleton ([plan-shared-app.md](../../plan/br6/plan-shared-app.md)) on 2026-10-23. Every figure below comes from `br6.pipeline_runs`, `system.parts` and the job logs.

**Setup:**
- Windows 11 laptop, Rancher Desktop (WSL2).
- Spark standalone: 1 worker (2 cores, 2 GB). The import used the default settings: `spark.cores.max=2`, 512 MB driver and executor, 2 write partitions, AQE on.
- ClickHouse 26.8.

Job time excludes `spark-submit` start-up (about 15–30 s).

## Data

| Figure | Value |
|---|---|
| Source files | 15 × `<city>_checkins.csv`, 470 MB, 2,629,541 rows, 15 columns |
| Empty values kept | latitude, longitude and name 348,592 (13.3%); address 597,610 |
| Dimension rows | city 15 · place 338,660 (85,216 without location) · visitor 80,299 · category 764 · place type 6 · grouping 764 · area 482 · date 2,392 |
| Places with more than one category over time | 4,556 (main category chosen as planned) |
| Categories not in the grouping file | 0 |
| Checks | raw = rows read; fact = rows × multiplier, also per city; 0 orphan keys; places and visitors equal the distinct pairs in raw; check-ins without location equal in raw and fact; check-ins per category equal in raw and fact |
| Spark read vs. Python read | Row counts and the first and last row of every file are identical |

Timestamps are stored as UTC exactly as in the files (for example Beijing row 1: `2012-04-08 04:55:00`), and the date key is the city's local date.

## Import time (seconds)

| Run | Total | read | check | derive | write raw | write dims | write fact | verify |
|---|---|---|---|---|---|---|---|---|
| 2 cities (Beijing, Tokyo; 15,309 rows), ×1 | 77.7 | 18.9 | 11.2 | 5.4 | 2.6 | 24.6 | 4.6 | 0.2 |
| 15 cities, ×1, parallel writer (2 runs) | 251–278 (avg 264) | 50–51 | 31–32 | 29–36 | 42–51 | 54–56 | 35–40 | 1.2–2.4 |
| 15 cities, ×2, parallel writer (5,259,082 fact rows) | 365.4 | 60.5 | 33.8 | 31.4 | 47.6 | 78.6 | 99.7 | 1.9 |
| 15 cities, ×1, pandas writer (baseline) | failed at 115 s | 47.2 | 27.2 | 25.4 | out of memory | | | |

The swap takes under 0.1 s.

**Spark figures per run** (15 cities): ×1 used 179–200 s of executor CPU and wrote 828 MB of shuffle data, with a 409 MB executor peak heap. ×2 used 243 s of CPU and wrote 946 MB of shuffle.

## Storage in ClickHouse

| Table | Rows | Compressed | Uncompressed |
|---|---|---|---|
| raw_checkin | 2,629,541 | 119.2 MiB | 370.6 MiB |
| fact_checkin (×2) | 5,259,082 | 97.1 MiB | 314.9 MiB |
| dim_place | 338,660 | 15.0 MiB | 25.0 MiB |
| all other dimensions | 84,722 | 0.7 MiB | 0.8 MiB |

## Observations for the report

- **The pandas baseline can't import BR6 at all with the default 512 MB driver.** `toPandas()` of the 2.6 million raw rows failed with `java.lang.OutOfMemoryError: Java heap space` in the driver. The run was marked failed and the stored data stayed as it was. The parallel writer handled the same data, and twice as much, without memory trouble (executor peak heap 409–466 MB). (BR5's 119k rows fit through the driver at ×1.)
- **Joins with the large dimensions are sort-merge joins.** In the fact build, `dim_place` (339k rows) and `dim_visitor` (80k) are above the 10 MB broadcast threshold, so Spark shuffles both sides. The small dimensions (city, category, area) are broadcast. This is the join tuning case the data model expected; BR5's dimensions were all tiny.
- **Building the dimensions is the slowest write step** (54–56 s at ×1). Most of it is the window functions: main category per place and key numbering per city. They work on the original rows, so they shouldn't grow with the multiplier. The single ×2 run still took 79 s for this step, so a repeat is needed to tell that apart from noise.
- **×2 data took 1.4× the time** (264 → 365 s). Most of the extra time is the fact write (about 38 → 100 s). Read, check and derive barely grow, because the copies are made after the joins.
- **The star schema is smaller than the raw table on disk:** the fact table holds twice the rows of raw (×2) in 97 MiB against raw's 119 MiB. Its columns are integer keys, while raw repeats names and addresses. (For BR5 the fact table was slightly larger.)
- **Compression:** the 470 MB of CSV is stored as 119 MiB in the raw table (about 4× smaller), and the whole star at ×1 needs about 65 MiB.
- **Read and check take about 80 s,** a third of the import. They parse 470 MB of CSV with escaped quotes, count rows per file against Python, and check every value.

## Behaviour verified

- **Start-up modes:** `up` with empty tables opens the app with "No data yet" (D3). `fresh` dropped only BR6's data tables: BR5's 1,193,900 bookings, 116 runs and 7 settings rows, and BR6's run history, were unchanged (D2).
- **Persistence:** after a ClickHouse restart, the same `import_id` and row counts (D10).
- **Busy lock:** during an import, a second import from `route_launch.py` and from `scripts/br6.py` was refused with "Busy: a BR6 import is in progress", and the app's Import button was disabled (D8, P6).
- **Bad input:** a Beijing file with one latitude set to `abc` failed at the check step with "Values that can't be stored as-is, per column: {'latitude': 1}". The stored data was unchanged and the staging database was removed (D7). An unknown city fails before Spark starts and lists the available cities.
- **`--if-empty` (what `make demo` runs):** with data stored, the job exits at once ("BR6 data already imported").
- **Chosen cities (D9)** and **multiplier ×2 (D12):** 15,309 rows for Beijing and Tokyo. At ×2: 5,259,082 fact rows, `copy_no` 0–1, 160,598 distinct (visitor, copy) pairs, places unchanged.
- **Shared app:** the menu has five items. The three selection pages show both BRs with a status line and open the right pages. BR5's pages still open at `/`, `/settings`, `/data` and `/run_history`. BR6's Data page shows status, the city table, write progress ("0 of 2,629,541 rows written" while writing) and the busy state. The sidebar shows BR5's links, BR6's links, or both, depending on the page.
- **BR5 after the change:** a BR5 analysis run (City Hotel, Portugal) completed as before.
- **Unit tests:** `make br6-test`, 10 tests passed.

## Not yet measured

- The pandas writer with a larger driver (1–1.5 GB), once the BR6 Advanced Settings screen can change it.
- Grafana dashboards for BR6 (monitoring plan).
