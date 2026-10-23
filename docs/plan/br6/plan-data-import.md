# BR6 Plan: Data Import and Start-up Modes

Implements [spec-data-import.md](../../spec/br6/spec-data-import.md) (D1–D14 and the Data screen). It fills the tables designed in [plan-data-model.md](plan-data-model.md). The changes to the shared app that this stage needs (menu, selection pages, sidebar) are in [plan-shared-app.md](plan-shared-app.md).

**Status:** implemented and tested end to end on 2026-10-23; figures in [result-data-import.md](../../result/br6/result-data-import.md).

This plan follows [BR5's import plan](../br5/plan-data-import.md) step by step and says only where BR6 differs. It does not cover the Route Finder pipeline, the Advanced Settings screen or the Grafana dashboards; each will get its own plan. Two things built here are reused by them: the run history and settings tables, and the BR6 job modules.

## 1. Decisions taken in this plan

Change any of these before implementation starts.

| # | Decision | Why |
|---|---|---|
| P1 | **The import is a Spark batch job, BR6's `job.py` with `MODE = "batch"`.** With no arguments it imports only if no data is stored yet. | Same as BR5: keeps the BR plug-in convention. `make demo` and `make demo-br BR=6` work and never wipe BR6 data. |
| P2 | **Replace = build everything in `br6_staging`, verify, then swap table by table** (`EXCHANGE TABLES`). | Same as BR5. The old data stays until the new data is verified (D6, D7). |
| P3 | **New shared modules in `jobs/common/`, built from BR5's code:** run history and busy lock, settings store, job launcher, Spark settings catalog, run figures (section 3). **BR5's own copies are not touched.** | BR6 needs exactly what BR5 built, and CLAUDE.md asks for shared code to grow additively. BR5 is benchmarked and finished, so changing it now would put its results at risk for no gain. Moving BR5 onto the shared modules is possible later. *Alternative:* refactor BR5 onto them now (less duplicate code, but BR5 must be re-tested). |
| P4 | **BR6 module names never repeat BR5's** (`route_runs.py`, not `runs.py`). | The shared app loads BR5's and BR6's job folders into one Python process, so two `runs.py` files would clash. |
| P5 | **The app container stays `br5-app`.** It also gets the profile `br6`, and its Python path gets `jobs/br6_route`. | Its name is used by BR5's scripts and links and as the Spark driver host. Renaming it would change BR5. |
| P6 | **A BR6 import or run refuses to start while any BR5 or BR6 job is running.** BR6 reads `br5.pipeline_runs` for this (read-only). | The Spark worker is shared. Two jobs at once wait for each other's cores and spoil the timings (spec G7). BR5's code stays unchanged, so the rule is one-sided; the restart guide says so. |
| P7 | **Choosing cities (D9) replaces the whole BR6 data with the chosen cities.** There is no adding of one city to the stored data. | One import id and one swap, as in BR5. Re-importing all 15 cities is one click. |
| P8 | **Multiplier choices are ×1, ×2 and ×5** (not ×10/×50). Only the fact table is repeated (data model, section 1). | ×1 is already 2.6 million check-ins, 2.2× the BR5 benchmark data at ×10. ×5 gives 13 million fact rows, which is enough to show optimization effects on the laptop. |
| P9 | **The import loads the category → place type file** into `dim_place_type` and `category_place_type`. Every run reloads the file before it starts (pipeline plan), so an edit counts on the next run (D13). | The star is complete right after an import, and the Data screen can show how many categories are grouped. |
| P10 | **BR6 commands don't start Kafka.** | BR6 has no streaming input. |
| P11 | **The "place statistics at import" option is left to the pipeline plan.** It adds one step, `place_stats`, between `verify` and `swap`. | Which statistics to keep depends on the scoring, which the pipeline plan designs. |

## 2. Flow

```
make br6-import / Data screen button (Route Finder panel)
  └─ route_launch.py import → spark-submit jobs/br6_route/job.py --replace [--cities a,b] [--multiplier N]
       1. checks: busy (BR5 or BR6)?  files real (not LFS pointers), headers right?  data exists (--if-empty → stop)?
       2. register run in br6.pipeline_runs (status running, heartbeat every 10 s)
       3. read the chosen CSV files as text → check every value can be typed → type them
       4. add city, source_row, import_id; cache
       5. into br6_staging: raw_checkin → dimensions → fact (×N copies, joins) 
       6. run the model checks (section 6)
       7. EXCHANGE each br6_staging table with its br6 table; drop br6_staging
       8. finish run (succeeded / failed + error, step durations, figures)
     on any failure: drop br6_staging, mark failed, exit non-zero; br6 tables untouched
```

## 3. Files

| File | New/changed | Content |
|---|---|---|
| `sql/clickhouse/ddl_br6.sql` | new | `CREATE DATABASE IF NOT EXISTS br6`; all tables of the data model, `pipeline_runs` and `settings` (`IF NOT EXISTS`) |
| `data/br6/cities.csv` | new (LFS) | `city_code,city_name,country_code,country_name,timezone` for the 15 files (for example `kuwait_city,Kuwait City,KW,Kuwait,Asia/Kuwait`). Written by hand |
| `data/br6/category_place_type.csv` | new (LFS) | The grouping table; first draft already written (data model, section 5) |
| `jobs/common/run_store.py` | new (shared) | From BR5's `runs.py`: the `Run` class (run row, steps, heartbeat), the busy check, the settings store, table helpers, cluster info, stop on SIGTERM. The database and the log prefix are parameters |
| `jobs/common/job_launch.py` | new (shared) | From BR5's `launch.py`: build the `spark-submit` command, start the job in its own process group, stop it cleanly |
| `jobs/common/spark_settings.py` | new (shared) | From BR5's `run_settings.py`: the Spark setting catalog (groups, defaults, checks), `spark_conf`, snapshot. BR-specific settings are added by each BR |
| `jobs/common/run_figures.py` | new (shared) | From BR5's `run_metrics.py`: Spark and ClickHouse figures stored with each run |
| `jobs/br6_route/job.py` | new | Import job entry point (section 5) |
| `jobs/br6_route/checkin_source.py` | new | Reads the CSV files as text, checks headers and values, types the columns |
| `jobs/br6_route/route_star.py` | new | Builds the dimensions and the fact |
| `jobs/br6_route/route_runs.py` | new | BR6 names on top of `run_store` (database, table list), the data status, the busy check across BR5 and BR6 |
| `jobs/br6_route/route_settings.py` | new | BR6 settings: the shared Spark settings plus writer and write partitions. Extended by the config plan |
| `jobs/br6_route/route_launch.py` | new | Starts BR6 jobs with the stored settings; also a CLI (as BR5's `launch.py`) |
| `jobs/br6_route/route_status.py` | new | Prints the data status (`make br6-status`) |
| `scripts/br6.py` | new | Launcher: `up`, `fresh`, `import`, `status` (section 7) |
| `Makefile` | additive | `br6-up`, `br6-fresh`, `br6-import`, `br6-status` |
| `docker-compose.yml` | additive | `br5-app`, `spark-history` and `docker-stats` get the profile `br6` as well as `br5`. `br5-app` gets `/opt/jobs/br6_route` on its `PYTHONPATH` and `BR6_LOG_DIR=/opt/logs/br6` |
| `apps/br5/…` | changed / new | See [plan-shared-app.md](plan-shared-app.md). BR5's page files are not changed |
| `CLAUDE.md` | small edit | Generic lines only: shared job modules in `jobs/common/`; BRs that share an app need unique module names |

`scripts/start.py` needs no change: BR5 already made it skip the table truncation for batch BRs (D11), and it ships `jobs/common/*.py` and the job's sibling files with `spark-submit`.

## 4. Tables

All tables are defined in [plan-data-model.md](plan-data-model.md). `pipeline_runs` and `settings` are never dropped by fresh mode and never swapped (D2).

## 5. Import job (`jobs/br6_route/job.py`)

Arguments:
- `--if-empty` (the default with no arguments, as `start.py` calls it) or `--replace`
- `--cities a,b,c` (file names; default: every `*_checkins.csv` in `data/br6/`)
- `--multiplier N` (1, 2 or 5 from the app; any N ≥ 1 from the CLI)
- `--writer partitions|pandas`, `--write-partitions N` (default: the stored settings)
- `--run-id`, `--settings-json`, `--settings-label` (passed by `route_launch.py`, as in BR5)

The Spark application is named **"BR6 import"**, so the shared Spark history page can be searched by BR (spec M10).

Steps (each is a recorded step with its duration):

1. **Pre-checks** (before the run is recorded, as in BR5).
   - Busy: exit if `br6.pipeline_runs` or `br5.pipeline_runs` has a running row with a fresh heartbeat (P6).
   - Each chosen file must exist and must not be an LFS pointer ("run `git lfs pull`"). Its header must be exactly the 15 expected columns.
   - Each chosen city must be in `cities.csv`.
   - With `--if-empty`, exit 0 when `br6.fact_checkin` already has rows.
2. **`read`.** Read the chosen files in one Spark read, every column as text.
   - Options: `quote='"'`, `escape='"'` (data model, section 2), `mode=FAILFAST` (a row with the wrong number of fields stops the import instead of being padded or cut).
   - The city comes from the file name (`input_file_name()`).
   - `source_row` is the row number inside its file: `row_number()` per city, ordered by `monotonically_increasing_id()`, which follows the file order.
3. **`check`.** Nothing is dropped or changed silently.
   - **Lost-value check** (as in BR5): count values that are not empty but don't survive the cast (the shared session has ANSI off), integers outside their ClickHouse type, coordinates outside ±90 / ±180, and empty values where the column is never empty (all columns except latitude, longitude, name and address). Any count > 0 fails the import and lists the columns.
   - **Row counts:** rows read per city must equal the file's line count minus the header (counted on the driver). The first and last row of each file must have `source_row` 1 and N.
   - **Place consistency:** a (city, venue) must have one name, one address and one position. 0 on this data; a later file that breaks it fails clearly instead of picking one value at random.
   - Categories missing from the grouping file are not an error. They are counted, logged and shown on the Data screen.
4. **`derive`.** Type the columns, add `import_id`, the city key and the local date; cache the typed input and count it.
5. **`write_raw`.** `raw_checkin` from the typed input (×1).
6. **`write_dims`.** Build the dimensions as in the data model (keys, main category, city centre, dates, place types). The small tables are inserted from the driver; `dim_place` (339k rows) and `dim_visitor` (80k) go through the chosen writer.
7. **`write_fact`.** The original rows joined to the dimensions, then copied (*built:* the joins run once and a broadcast `range(N)` is cross-joined after them for `copy_no`, instead of joining every copy):
   - `dim_city`, `dim_category` and `dim_area` are broadcast.
   - `dim_place` and `dim_visitor` use Spark's choice, so the join strategy shows in `explain()` and changes with the threshold setting.
   - `date_key` is computed from `checkin_time` in the city's time zone.
   - The fact is repartitioned to `--write-partitions` and written with the chosen writer. The job logs `explain(mode="formatted")` for the report.
8. **`verify`.** The checks in section 6, on `br6_staging`. They also catch double inserts from a retried Spark task.
9. **`swap`.** `EXCHANGE TABLES` dimensions first, then raw and fact; then drop `br6_staging`.
10. **Record.** Rows read, expected and written; the chosen cities in `criteria`; the source files and their total size; step durations; Spark and ClickHouse figures (shared `run_figures`).

**Writers:** the existing `write_partitions` (`jobs/common/clickhouse_io.py`, already has a `database` argument) and the `toPandas` baseline. At ×1 the baseline sends 2.6 million rows through the driver in `br5-app`. If it runs out of memory, that is a finding for the report, not a bug (as for BR5 at ×50).

## 6. Checks (end of every import)

| Check | Expected at ×1, all 15 cities |
|---|---|
| `count(raw_checkin)` = rows read; `count(fact_checkin)` = rows read × N; the same per city | 2,629,541 |
| Every fact key exists in its dimension (anti-join count) | 0 |
| `dim_place` = distinct (city, venue) in raw; places without location | 338,660; 85,216 |
| Fact rows whose place has no location = raw rows with no latitude (× N) | 348,592 |
| `dim_visitor` = distinct (city, user) in raw | 80,299 |
| Check-ins per category: raw = fact (× N) | equal for all 764 |
| Every category has one grouping row (missing ones → *Not offered*, counted) | 764 rows, 0 missing with the draft file |
| `dim_place.main_category_key` set for every place; `categories_seen > 1` | all; 4,556 |

## 7. Commands (D1, D3, D4)

| Command | Does |
|---|---|
| `make br6-up` | Refuses if a BR5 or BR6 job is running (restarting Spark would kill it). Starts ClickHouse, Spark, Prometheus, Grafana, the Spark history server, `docker-stats` and the shared app; creates the BR6 tables if missing; prints the URLs (the app's Route Finder and BR6 Data pages, Grafana, Spark). With no data, the app opens and the Data screen says "no data yet, import now" (D3) |
| `make br6-fresh` | Same services. **Drops the BR6 data tables only** (every `br6` table except `pipeline_runs` and `settings`; never other BRs' tables), recreates them, imports all chosen cities, then prints the URLs (D2) |
| `make br6-import` | Makes sure the services are up, asks "Replace N stored check-ins? [y/N]" when data exists (D6), then runs `route_launch.py import` inside `br5-app` and shows its progress lines |
| `make br6-status` | Prints what the Data screen shows |

- Variables: `CITIES=jakarta,tokyo`, `MULTIPLIER=2`, `WRITER=pandas`, `PRESET=name`, `YES=1`.
- As with BR5, CLI imports run in `br5-app` through `route_launch.py`, the same code the app's button uses, with the stored settings.
- `make br5-up`, `make br5-fresh` and the other BR5 commands keep working as today (D3b). Both BRs' commands start the same app.

## 8. Data screen (Route Finder panel; D3, D5–D9, spec section 5)

Reached from the menu *Data*, then the *Route Finder* panel ([plan-shared-app.md](plan-shared-app.md)). Built like BR5's Data page:

- **Status:**
  - Loaded or not, total check-ins, multiplier ("synthetic ×N" when N > 1).
  - Import time and duration, source files and total size. These come from the latest succeeded import whose `import_id` matches the live fact table.
  - A **table per city:** city, country, check-ins, check-ins without location, places, visitors.
  - Categories not grouped (normally 0), with a hint to edit `data/br6/category_place_type.csv`.
  - Row count of each table (in an expander), so the star schema is visible.
- **Import / Re-import:**
  - City choice (multiselect, default all 15; D9) and data size ×1 / ×2 / ×5.
  - The settings in use (writer, write partitions, executor memory) as one caption line.
  - A confirmation when data exists: "This replaces the N stored check-ins. The current data stays until the new import succeeds."
- **Progress:** refreshes every 2 s and shows the current step in plain words. During `write_raw` and `write_fact` it also shows a bar: the staging table's row count against the expected rows. A **Stop import** button keeps the stored data.
- **On failure:** the error from `pipeline_runs`; the data shown is unchanged (D7).
- **Busy:** while any BR5 or BR6 job runs, the button is disabled with a message saying which one (D8, P6).

## 9. Order of work

| Step | Work | Done when |
|---|---|---|
| 1 | DDL, `cities.csv`, compose profile and path | `make demo-br BR=6` creates the `br6` tables. BR3, BR4 and BR5 behave as before |
| 2 | Shared modules in `jobs/common/` | Unit-checked from `spark-local`. `git diff` shows no change to BR5 files |
| 3 | Import job: read, check, raw table | A manual `spark-submit` loads 2,629,541 rows; every problem count is 0 |
| 4 | Import job: star schema, checks, swap | Section 6 checks pass; a second import replaces the first |
| 5 | `route_launch.py`, `scripts/br6.py`, Makefile | All four commands work from the terminal |
| 6 | Shared app changes and the BR6 Data screen ([plan-shared-app.md](plan-shared-app.md)) | Import from the button; progress, stop, busy and error states visible; BR5 pages as before |
| 7 | CLAUDE.md lines; spec notes (P7, P8) | Docs match the code |
| 8 | Verification (section 10), then `docs/result/br6/result-data-import.md` | All rows pass; figures recorded |

## 10. Verification

| Check | Expected | Covers |
|---|---|---|
| Import ×1, all cities | Section 6 values; per-city counts match the file profile | Correctness |
| Import ×2 | 5,259,082 fact rows; `copy_no` 0–1; dimensions and raw unchanged; 160,598 distinct (visitor, copy) | D12 |
| Import of two cities (`CITIES=beijing,tokyo`) | 15,309 check-ins; only those cities on the Data screen | D9 |
| `make down`, then `make br6-up` | Same `import_id` and row counts | D10, D1 |
| `make br6-fresh` with BR5 data stored | BR5 counts, run history and settings unchanged; BR6 run history kept | D2 |
| `make demo-br BR=6` with data present | Job exits immediately; data kept | P1 |
| `make br5-up`, BR5 import and one BR5 run after the BR6 work | Behave as before | D3b, P3 |
| Import of a broken file (bad header, a bad value, an unknown city) | Run marked failed with a clear message; stored data unchanged | D7 |
| Start a BR6 import while a BR6 or BR5 job runs (CLI and button) | Refused with "busy" and which job | D8, P6 |
| Fresh volumes, then `make br6-up` | App opens; BR6 Data says "no data yet" | D3 |
| `WRITER=pandas` vs `partitions` at ×1 | Durations recorded for both (or the pandas memory failure, recorded as a finding) | Writer baseline |

## 11. Risks and notes

- **Import time is unknown until measured.** The data is 470 MB, against BR5's 17 MB file, on a 2-core worker. If ×5 takes too long on the laptop, ×5 is dropped from the app's choices (the CLI keeps any N).
- **File order for `source_row`:** it relies on Spark reading the chunks of one file in order, which it does. The first/last-row check (section 5, step 3) would show it if not.
- **Busy lock race:** two jobs started within the same second could both pass the check. Acceptable for a single-user local app, as in BR5.
- **Non-atomic multi-table swap:** atomic per table, not across tables, as in BR5. A crash in the middle leaves the run marked failed; re-running the import fixes it.
- **Duplicated code with BR5 (P3)** until BR5 is moved to the shared modules, if ever.
