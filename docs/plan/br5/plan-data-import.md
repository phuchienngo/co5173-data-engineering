# BR5 Plan: Data Import and Start-up Modes

Implements [spec-data-import.md](../../spec/br5/spec-data-import.md) (D1–D12 and the Data screen). It fills the tables designed in [plan-data-model.md](plan-data-model.md) and follows the rules in [spec.md](../../spec/br5/spec.md).

This plan does not cover the Promotion Finder analysis, the Advanced Settings screen or the Grafana dashboards; each will get its own plan. Two things built here will be reused by them: the run history table and the app skeleton.

## 1. Decisions taken in this plan

Change any of these before implementation starts.

| # | Decision | Why |
|---|---|---|
| P1 | **The import is a Spark batch job.** It reads the CSV, builds the raw table and the star schema, and writes them to ClickHouse. | Uses the project stack. Import time, joins, skew and caching all become things we can tune and measure. The multiplier (D11) is one line in Spark. |
| P2 | **The import is BR5's `job.py`, with `MODE = "batch"`.** With no arguments it imports only if no data is stored yet. | Keeps the BR plug-in convention. `make demo` and `make demo-br BR=5` still work and never wipe BR5 data (D9). |
| P3 | **BR5 tables live in database `br5`.** `start.py` truncates `fact_*` tables only for **stream** BRs; batch BRs manage their own data. | The star schema needs normal `fact_`/`dim_` names, and BR4 already has `default.fact_bookings`. The truncation change is additive: BR3 and BR4 are stream BRs, so they behave as before (D10). |
| P4 | **Replace means "build everything in database `br5_staging`, verify, then swap table by table".** The swap uses ClickHouse `EXCHANGE TABLES`. | The old data stays until the new data is verified (D6, D7). Each swap is atomic and the whole swap takes milliseconds; the busy lock keeps runs from reading during it. |
| P5 | **New shared writer: `write_partitions` (`foreachPartition` + `clickhouse_connect`).** The existing `toPandas` writer stays as the baseline, selected with `--writer pandas`. | `toPandas` funnels every row through a 512 MB driver, which will fail at ×50. This is the "parallel writer" already planned in CLAUDE.md, and it needs no new JARs. |
| P6 | **The web app uses Streamlit** and runs in a new `br5-app` container built from the existing Spark image. The app launches `spark-submit` itself. | Drop-downs, buttons and charts in plain Python. The container already has `spark-submit`, `pyspark` and `clickhouse_connect`, and it doesn't need the Docker socket. |
| P7 | **One BR5 job at a time, enforced through `br5.pipeline_runs`.** A job marked `running` with a recent heartbeat means busy (D8). | Works the same for the CLI and the app, and for later analysis runs too. |
| P8 | **BR5 commands don't start Kafka.** | BR5 has no streaming input. This saves about 1 GB of RAM on the laptop. |
| P9 | **"NA" is also read as empty.** | The file uses `NULL` for agent, company and country, but `NA` for `children` (4 rows). The spec mentions only "NULL"; it will be updated to match. |

## 2. Flow

```
make br5-import / Data screen button
  └─ spark-submit jobs/br5_promotion/job.py --replace [--multiplier N] [--writer partitions|pandas]
       1. checks: busy?  file is real (not an LFS pointer)?  data exists (--if-empty → stop)?
       2. register run in br5.pipeline_runs (status running, heartbeat every 10 s)
       3. read CSV as text → "NULL"/"NA" → empty → cast to types → verify no value was lost
       4. add source_row, import_id, is_duplicate; cache; repeat N times (copy_no) if multiplier > 1
       5. into br5_staging: write raw_booking → build and write dims → build fact (broadcast joins) and write
       6. run the model checks (plan-data-model.md section 4)
       7. EXCHANGE each br5_staging table with its br5 table; drop br5_staging
       8. finish run (succeeded / failed + error, step durations)
     on any failure: drop br5_staging, mark failed, exit non-zero; br5 tables untouched
```

## 3. Files

| File | New/changed | Content |
|---|---|---|
| `sql/clickhouse/ddl_br5.sql` | new | `CREATE DATABASE IF NOT EXISTS br5`; all tables from the data model plus `pipeline_runs` (`IF NOT EXISTS`) |
| `data/br5/countries.csv` | new (LFS) | `code,name` for every code in the bookings file. Generated once with `pycountry` on the dev machine (not a runtime dependency); codes that aren't ISO (for example `CN`, `TMP`) are added by hand |
| `jobs/br5_promotion/job.py` | new | Import job entry point (section 5) |
| `jobs/br5_promotion/star.py` | new | Builds the dimension and fact DataFrames from the typed input |
| `jobs/br5_promotion/runs.py` | new | Run-history and busy-lock helpers, shared by the job, the launcher and the app |
| `jobs/common/clickhouse_io.py` | additive | `write_partitions(df, table, batch_rows)` |
| `scripts/start.py` | additive | `create_clickhouse_tables(br, truncate=True)`, called with `truncate` only for stream BRs; `spark_submit_cmd(..., job_args=(), packages=SPARK_KAFKA_PACKAGE)`; `start_stack(services=None)`. Defaults behave as today |
| `scripts/br5.py` | new | Launcher: `up`, `fresh`, `import`, `status` (section 6) |
| `Makefile` | additive | `br5-up`, `br5-fresh`, `br5-import`, `br5-status`; `down` and `clean` also cover the `br5` profile |
| `docker-compose.yml` | additive | `br5-app` service (profile `br5`, port 8501, `hostname: br5-app`, mounts `./apps`, `./jobs`, `./data`, `./logs`) |
| `requirements.txt` | additive | `streamlit` |
| `apps/br5/app.py`, `apps/br5/pages/*` | new | App skeleton with 3 pages. Only the Data page is implemented here (section 7) |
| `CLAUDE.md` | small edit | Generic lines only: truncation applies to stream BRs; a BR may have its own database, launcher `scripts/brN.py` and app `apps/brN/` |

## 4. Tables

The star schema and the raw table are defined in [plan-data-model.md](plan-data-model.md).

**Column types for `raw_booking`:**

| Columns | Type |
|---|---|
| hotel, arrival_date_month, meal, market_segment, distribution_channel, reserved/assigned_room_type, deposit_type, customer_type, reservation_status | `LowCardinality(String)` |
| country | `LowCardinality(Nullable(String))` |
| children | `Nullable(UInt8)` |
| agent, company | `Nullable(UInt16)` |
| lead_time, arrival_date_year, days_in_waiting_list | `UInt16` |
| all other integer columns (flags, nights, guests, counts) | `UInt8` (each column's max fits) |
| adr | `Float64` (negative values exist and are kept) |
| reservation_status_date | `Date` |

The fact table uses the same types for the same values.

**`br5.pipeline_runs`** (ReplacingMergeTree(`updated_at`), ORDER BY `run_id`; read with `FINAL`). This is the run history for imports now and analysis runs later. It is **never** dropped by fresh mode or swapped (D2).

`run_id, run_type ('import'), status ('running'|'succeeded'|'failed'), step, started_at, finished_at, duration_ms, step_durations (Map(String, UInt32)), rows_read, rows_expected, rows_written, multiplier, writer, source_file, source_bytes, error, heartbeat_at, updated_at`

## 5. Import job (`jobs/br5_promotion/job.py`)

Arguments:
- `--if-empty` (the default when called with no arguments, as `start.py` does)
- `--replace`
- `--multiplier N` (default 1)
- `--writer partitions|pandas` (default `partitions`)
- `--write-partitions N` (default 2, the worker's core count)

The job:

1. **Pre-checks.**
   - Exit with "busy" if `pipeline_runs` has a `running` row with a heartbeat in the last 30 s.
   - Fail with "run `git lfs pull`" if the CSV starts with `version https://git-lfs`.
   - With `--if-empty`, exit 0 when `br5.fact_booking` already has rows.
2. **Read.** Read every column as text and check that the header is exactly the 32 expected columns.
   - Turn `NULL` and `NA` into empty values.
   - Cast each column to its type.
   - **Lost-value check:** count values that were not empty before the cast but are empty after it (the shared session has ANSI off, so a bad cast silently returns null). If any are found, fail and list the columns. No row is dropped.
3. **Derive.**
   - `source_row` comes from `zipWithIndex` on the file order. `import_id` is a new UUID.
   - `is_duplicate` comes from a window over all 32 columns, ordered by `source_row`.
   - The result is cached. For the multiplier, `crossJoin(spark.range(N))` produces `copy_no`.
4. **Write to `br5_staging`.** Recreate the database with the same tables, then:
   1. `raw_booking`, from the typed input.
   2. Dimensions, built from the original (not multiplied) rows. Keys are assigned as in the data model. `dim_date` is generated for the full date range. `dim_country` joins the codes with `countries.csv`.
   3. `fact_booking`: the multiplied rows joined to the dimensions (broadcast). Date keys are computed rather than joined; a missing country maps to key 0.

   The large tables (raw, fact) are repartitioned to `--write-partitions` and written with the chosen writer. The dimensions are small and are inserted from the driver.
5. **Verify.** Run the checks in [plan-data-model.md](plan-data-model.md) section 4 on `br5_staging`. These also catch double inserts from a retried Spark task.
6. **Swap.** `EXCHANGE TABLES br5.X AND br5_staging.X`, dimensions first, then raw and fact. Then drop `br5_staging`, which now holds the old data.
7. **Record.** Store step durations (`read`, `check`, `derive`, `write_raw`, `write_dims`, `write_fact`, `verify`, `swap`) in `step_durations`; the monitoring plan will chart them. The job also logs `explain()` of the fact build, for the report.

**`write_partitions`:**
- Each executor opens its own `clickhouse_connect` client and inserts in batches of `batch_rows` (default 50,000).
- The connection settings are read **on the driver** and passed into the function, because the `spark-worker` container has no `CLICKHOUSE_HOST` variable.

## 6. Commands (D1, D4)

| Command | Does |
|---|---|
| `make br5-up` | Starts ClickHouse, the Spark master and worker, spark-local, Prometheus, Grafana and `br5-app`; creates the BR5 tables if missing; prints the app, Grafana and Spark UI URLs. With no data stored, the app opens normally and shows "no data yet" (D3) |
| `make br5-fresh` | Same services. **Drops the BR5 data tables only** (every `br5` table except `pipeline_runs`; never other BRs' tables), recreates them, runs the import, then starts the app (D2) |
| `make br5-import` | Makes sure the services are up, then runs the import with `--replace`. Asks "Replace N existing bookings? [y/N]" when data exists (D6). Shows the job's progress lines and the result |
| `make br5-status` | Prints the same status the Data screen shows |

- Variables:
  - `MULTIPLIER=10`
  - `WRITER=pandas`
  - `YES=1` skips the confirmation
- CLI imports run inside `spark-local`, through `docker exec`, like `start.py` does.
- `make demo-br BR=5` still works. It runs `job.py` with no arguments, so it imports only when data is empty.

## 7. App skeleton and Data screen (D3, D5–D8, section 5 of the spec)

`apps/br5/app.py` is a Streamlit app with three pages: Promotion Finder, Advanced Settings and Data. The first two are placeholders here; they show "no data yet, import now" when the fact table is empty.

**Data page:**
- **Status:**
  - Loaded or not, booking count, and multiplier (labelled "synthetic ×N" when N > 1).
  - Import time and duration, and the source file and its size. These come from the latest succeeded import whose `import_id` matches the live fact table.
  - Row count of each table, so the star schema is visible.
- **Import / Re-import button:**
  - A multiplier choice: ×1, ×10, ×50.
  - A confirmation dialog when data exists.
  - The button launches the same `spark-submit` command as the CLI, as a background process, logging to `logs/br5/import-<run_id>.log`.
- **Progress:** refreshes every 2 s and shows the current step from `pipeline_runs`. During `write_raw` and `write_fact`, it also shows a bar: the staging table's row count against `rows_expected`.
- **On failure:** shows the error from `pipeline_runs`; the old data is untouched (D7).
- **Busy:** while any BR5 job is running, the button is disabled with a "busy" message (D8).

`br5-app` sets `hostname: br5-app` and passes `spark.driver.host=br5-app`, so the Spark executors can reach the driver.

## 8. Order of work

| Step | Work | Done when |
|---|---|---|
| 1 | DDL, `countries.csv`, `start.py` truncation change | `make demo-br BR=5` creates the `br5` tables; BR3 and BR4 still truncate |
| 2 | `write_partitions` | Unit-checked on a small DataFrame from `spark-local` |
| 3 | Import job: raw table | A manual `spark-submit` loads 119,390 rows; the lost-value check is 0 |
| 4 | Import job: star schema, checks, swap | Model checks pass; the swap replaces the data |
| 5 | Other `start.py` additions, `scripts/br5.py`, Makefile | All four commands work from the terminal |
| 6 | `br5-app` container and Data page | Import from the button, progress, busy and error states all visible |
| 7 | CLAUDE.md lines and spec update (P9, star schema mention) | Docs match the code |
| 8 | Verification (section 9) | All rows pass |

## 9. Verification

| Check | Expected | Covers |
|---|---|---|
| Import ×1 | 119,390 rows in raw and fact. Raw nulls: country 488, children 4, agent 16,340, company 112,593. `sum(adr)` matches pandas on the CSV. Model checks pass | Correctness |
| Import ×10 | 1,193,900 fact rows; `copy_no` 0–9; dimensions unchanged; multiplier shown on the Data page | D11 |
| `make down`, then `make br5-up` | Same `import_id` and row counts | D9, D1 |
| `make br5-fresh` with BR3/BR4 tables filled | BR3/BR4 counts unchanged; `pipeline_runs` history kept | D2 |
| `make demo-br BR=3,4` after BR5 work | Behaves as before, including truncation | No break to BR3/BR4 |
| `make demo-br BR=5` with data present | Job exits immediately; data kept | D10, P2 |
| Import of a broken file (bad header or bad value) | Run marked failed with a clear error; old data and row counts unchanged | D7 |
| Start a second import while one runs (CLI and button) | Refused with "busy" | D8 |
| Fresh volumes, then `make br5-up` | App opens and shows "no data yet" | D3 |
| `WRITER=pandas` vs `partitions` at ×1 and ×10 | Both succeed; durations recorded. `pandas` at ×50 may run out of driver memory, which is a finding for the report, not a bug | P5 |

## 10. Risks and notes

- **Busy lock race:** two imports started within the same second could both pass the busy check. This is acceptable for a single-user local app.
- **Non-atomic multi-table swap:** the swap is atomic per table, not across tables. A crash in the middle of step 6 would leave mixed tables. The fix is to re-run the import, and the run is marked failed, so this is visible.
- **Package download:** BR5 jobs don't pass the Kafka package (`packages=None`), so no download is needed before the first run.
- **Data size:** the source file is 17 MB, so Spark reads it in one partition. The repartition before the write is what gives the writer its parallelism.
- **Spark start-up:** a Spark job takes about 15–30 s to start. Short import runs are dominated by start-up, so the multiplier matters for meaningful write benchmarks.
