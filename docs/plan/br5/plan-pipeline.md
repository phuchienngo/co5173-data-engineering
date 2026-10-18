# BR5 Plan: Data Processing Pipeline and Settings

Implements the analysis run behind the Promotion Finder ([spec.md](../../spec/br5/spec.md) sections 3–4) and its settings ([spec-pipeline-config.md](../../spec/br5/spec-pipeline-config.md), C1–C13 except C9). It reads the star schema from [plan-data-model.md](plan-data-model.md), which [plan-data-import.md](plan-data-import.md) fills, and reuses that plan's run history, busy lock and app skeleton.

Not covered: Grafana dashboards and the run-history compare view (monitoring plan), and selectable table layouts (C9, Could). Testing here is enough to show the pipeline is correct and settings take effect. The full benchmark matrix is planned and run with the monitoring step, where the resource figures are available.

## 1. Decisions taken in this plan

Confirmed; the spec was updated to match.

| # | Decision | Why |
|---|---|---|
| A1 | **Each run is its own Spark application** (`spark-submit` per click). | Resource settings (executor memory, cores) only apply when an application starts, and each run starts clean, so runs compare fairly. Cost: 15–30 s start-up per run. This settles open decision 5 of the spec. |
| A2 | **The run reads the star schema, not the CSV or the raw table.** It reads only the fact columns it needs. The filters chosen (hotel, country, room type) are pushed down into the ClickHouse query. | Column pruning and filter pushdown are the first optimizations. Reading the wide raw table stays a later benchmark variant (data model section 3). |
| A3 | **A new parallel reader: each Spark task reads its own slice from ClickHouse as Arrow** (`mapInArrow` + `clickhouse_connect.query_arrow`, slice = `source_row % N`). The baseline reader is a single query on the driver, turned into a DataFrame. | It mirrors `write_partitions`. It needs no new JARs (pyarrow 26 is already in the image) and gives a reader-vs-reader comparison. |
| A4 | **Spark does the heavy part; the rules run in plain Python on the driver.** Spark: read, clean, aggregate per target × month, then a second pass for the season profile of the kept targets. Python (`rules.py`): low season, potential, ranking, offers. | The aggregated data is at most a few thousand rows. Pure Python rules are easy to read and unit-test without Spark. The two Spark passes over the cleaned rows make caching measurable. |
| A5 | **An alternative engine: the same aggregates computed by ClickHouse SQL** (`engine = clickhouse`, run as plain Python, no Spark). Its results feed the same `rules.py`. | The course asks for alternatives benchmarked against the main solution. It also serves as a correctness reference: both engines must give the same cards. Spec: engine setting and C13. |
| A6 | **Settings are one registry** (`run_settings.py`): name, group, explanation, type, range, default. The app, the CLI launcher and the jobs all use it. The current values and named presets are stored in ClickHouse (`br5.settings`), so the app and `make br5-run` use the same values. | One source of truth for the screen, validation, the spark-submit command and the snapshot. |
| A7 | **Settings apply to imports too.** The writer choice and write partitions move from the Data page to Advanced Settings, and every import records a settings snapshot. | Imports are Spark applications too. Tuning (for example the broadcast threshold for the fact build) helps them as well. |
| A8 | **A settings combination that can never start is an error, not a warning.** For example, executor memory plus overhead larger than the worker's memory. | Spark standalone doesn't fail on such a run; it waits forever for resources, holding the busy lock. Other cluster checks stay warnings (C4). |
| A9 | **Results are normalized by the import multiplier.** Demand figures and the minimum-data check use valid bookings ÷ multiplier. | Synthetic copies must not change the business answer. This gives a cheap correctness check: ×1 and ×10 produce the same cards. |
| A10 | **A Stop button for runs started from the app** (kills the process group). A stopped run shows as interrupted once its heartbeat is stale. | A run stuck waiting for resources would otherwise keep the busy lock until the stack restarts. |

**Readings of the spec this plan uses** (confirmed and written into the spec):

| # | Spec point | Reading |
|---|---|---|
| S1 | Room type | The **reserved** room type (what guests asked for). |
| S2 | Hotel = All | Both hotels combined, not ranked: only country and room type are ranked (spec 3.2 table). |
| S3 | Peak-season bookings (rules 3.4) | The bookings of the **peak month** (the spec's definition of peak). |
| S4 | Low-season demand vs peak | The **average** demand of the low-season months. It is used for potential (peak − average low), for the discount tier, and in the "Nov – Feb: demand is 40% of peak" text (one text per range of adjacent months, wrapping from Dec to Jan). |
| S5 | Unknown country | Counted in the selection's demand, but never offered as a ranked target. |
| S6 | Month a stay belongs to | The arrival month (the whole stay counts there). |
| S7 | "If none triggers" (3.4) | The price rule always applies (add value, or discount "otherwise"), so this only happens when a season has no booking with ADR > 0. The card then shows the general-discount text. |
| S8 | Discount tier when the threshold is raised above 60% | Demand ratios above the last tier use the lightest tier. |
| S9 | Typical lead time; N for the early-bird offer | The exact median lead time of low-season bookings. N is the median rounded down to a multiple of 5 days. |
| S10 | "Where" (channels) | The two market segments with the most low-season bookings, with their shares. |

## 2. Flow of a run

```
Promotion Finder "Find promotions"  /  make br5-run HOTEL=.. COUNTRY=.. ROOM=.. [PRESET=..]
  └─ launch.py (inside br5-app): data loaded? busy? resolve settings (current or preset) → validate
       ├─ engine spark:      spark-submit analyze.py --hotel H --country C --room-type R --settings-json ..
       └─ engine clickhouse: python3 analyze.py ...           (no Spark application)
            1. register run in pipeline_runs (run_type 'analysis', criteria, settings snapshot, import_id)
            2. read       needed fact columns, filters pushed down; dims read on the driver
            3. clean      exclusion reason per row → counts per reason; cache cleaned rows (setting)
            4. aggregate  room-nights and bookings per target × year × month → driver
            5. rank       rules.py: per-year average, peak, low months, potential; minimum data; top N
            6. profile    kept targets only, low season vs peak month: median lead time, stay length,
                          family share, ADR (> 0 only), bookings per market segment → driver
            7. recommend  rules.py: offers, discount tier, evidence lines, channels → cards
            8. write      run_month and run_card rows; finish run (counts, step durations)
          on failure: run marked failed with the error; no partial results are shown
```

**Target** = the grouping the user left on **All**: by country, by room type, or by room type × country, inside the chosen filters. With everything chosen, there is one target. The **selection** (the chart in R1) is the sum of all targets, computed in Python from step 4 without another pass.

**Cleaning (step 3).** Every row gets at most one exclusion reason, checked in this order: canceled, zero nights, zero guests, then duplicate (only when the setting says remove). This keeps `read = used + Σ excluded` exact (R4). Two further counts are only reported, since those rows are kept: missing country (shown as Unknown) and ADR ≤ 0 (counted in demand, left out of price).

**Per-year average (spec 3.3).** The months that exist in the data come from one small ClickHouse query over the whole fact table (`DISTINCT intDiv(arrival_date_key, 100)`), not from the filtered rows. A target with no booking in a month that exists gets 0 for it.

**Join placement (setting).** The dimension names can be joined either:
- **late** (default): aggregate on integer keys, then attach names to the few result rows. Month and year come from the date key, so no join is needed while aggregating.
- **early** (baseline): join the fact with `dim_date`, `dim_hotel`, `dim_country`, `dim_room_type` and `dim_channel` before cleaning.

The early variant is what makes the broadcast-threshold and skew-join settings visible: Portugal is 41% of rows, so a sort-merge join on country is skewed.

## 3. Files

| File | New/changed | Content |
|---|---|---|
| `jobs/br5_promotion/run_settings.py` | new | Settings registry (section 4), built-in presets, `validate(values, cluster)` → errors and warnings, `spark_conf(values)`, `submit_args(values)`. Pure Python |
| `jobs/br5_promotion/rules.py` | new | Pure Python: month coverage and per-year average, peak and low months, ranges and their text, potential, minimum data, ranking, offers, discount tier, evidence lines, cards |
| `jobs/br5_promotion/analyze.py` | new | Run entry point (arguments: `--hotel`, `--country`, `--room-type`, `--run-id`, `--settings-json`). Steps as in section 2; picks the engine |
| `jobs/br5_promotion/pipeline_spark.py` | new | Spark engine: `read` (parallel Arrow or driver), `clean`, `aggregate`, `profile`. Logs `explain()` of the aggregate and the profile |
| `jobs/br5_promotion/pipeline_sql.py` | new | ClickHouse engine: the same outputs from two SQL queries (`quantileExact` for the median) |
| `jobs/br5_promotion/launch.py` | new | Builds and starts the spark-submit or python command for an import or a run from the resolved settings. Used by the app, and as a CLI (`python3 launch.py run ...`) by `scripts/br5.py` |
| `jobs/br5_promotion/runs.py` | additive | `run_type` `'analysis'`; new run fields (section 5); settings and preset load/save; `latest_result` |
| `jobs/br5_promotion/job.py` | small | Accepts `--settings-json`, builds its session with those Spark settings, records the snapshot. Existing arguments keep working (`make demo-br BR=5` unchanged) |
| `jobs/common/spark_session.py` | additive | `get_spark_session(app_name, conf=None)`: entries in `conf` override the shared defaults. Without `conf`, BR3/BR4 behave exactly as today |
| `sql/clickhouse/ddl_br5.sql` | additive | Tables `settings`, `run_month`, `run_card`; `ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS ...` |
| `apps/br5/backend.py` | changed | Launch through `launch.py`; Stop; settings and presets; option lists; read results |
| `apps/br5/views/promotion_finder.py` | rewritten | Promotion Finder screen (section 7) |
| `apps/br5/views/settings.py` | rewritten | Advanced Settings screen (section 6) |
| `apps/br5/views/data.py` | small | Technical options removed (now settings); a line says which writer and preset the import will use |
| `scripts/br5.py` | additive | `run` subcommand. `import` and `run` execute `launch.py` inside `br5-app`, so the CLI and the app build the same command |
| `Makefile` | additive | `br5-run` (variables `HOTEL`, `COUNTRY`, `ROOM`, `PRESET`), `PRESET` for `br5-import`, `br5-test` |
| `docker-compose.yml` | additive | `br5-app` also mounts `./tests` (read-only) |
| `tests/br5/test_rules.py`, `tests/br5/test_settings.py` | new | `unittest` (no new dependency), run inside `br5-app` by `make br5-test` |
| `docs/guide/br5/config-guide.md` | new | Restart guide (section 8) |
| `docs/spec/br5/*.md` | done | Updated with the confirmed decisions (engine and reader settings, C4 errors, C13, the S-readings, open decisions 4–5) |

## 4. Settings

Defaults are Spark's own, except the resources, which are sized for the demo cluster (1 worker: 2 cores, 2 GB). The run reads **(unset)** as "don't pass it, so Spark decides".

| Group | Setting | Default | Notes |
|---|---|---|---|
| Resources | `spark.driver.memory` | 512m | Passed as `--driver-memory`, since the driver JVM is already running in client mode. Warning above 1g: the app container has 2 GB |
| | `spark.executor.memory` | 512m | Error if memory + overhead (max 384 MB or 10%) is larger than the worker's memory (A8) |
| | `spark.executor.cores` | (unset) = all worker cores | Error if larger than the worker's cores |
| | `spark.cores.max` | 2 | |
| Memory | `spark.memory.fraction` / `storageFraction` | 0.6 / 0.5 | Range 0.1–0.9 |
| | `spark.memory.offHeap.enabled` / `.size` | false / 0 | Error if enabled with size 0 |
| Shuffle and partitions | `spark.sql.shuffle.partitions` | 200 | AQE coalesces them by default. The current import passes 4; it now follows the setting |
| | `spark.default.parallelism` | (unset) | Affects RDD operations only; the help text says so |
| | `spark.sql.files.maxPartitionBytes` | 128m | Affects the import's CSV read only |
| | `spark.shuffle.compress`, `spark.io.compression.codec` | true, lz4 | Codec choices: lz4, snappy, zstd, lzf |
| AQE | `spark.sql.adaptive.enabled`, `.coalescePartitions.enabled`, `.advisoryPartitionSizeInBytes`, `.skewJoin.enabled`, `.skewJoin.skewedPartitionFactor`, `.skewJoin.skewedPartitionThresholdInBytes` | true, true, 64m, true, 5, 256m | |
| Joins | `spark.sql.autoBroadcastJoinThreshold` | 10m | -1 turns broadcast off |
| | Join dimension names | late | early / late (section 2) |
| Serialization | `spark.serializer` | Java | Java / Kryo. The help text notes that DataFrames mostly bypass it |
| Caching | Cache the cleaned rows / storage level | on / MEMORY_AND_DISK | MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, OFF_HEAP |
| Reading and writing | Engine | spark | spark / clickhouse (A5) |
| | Reader / read partitions | parallel / 2 | parallel / driver (A3) |
| | Writer / write partitions (import) | partitions / 2 | partitions / pandas (C8, already built) |
| | ClickHouse `max_threads`, `max_memory_usage` | 0, 0 (server default) | Sent with every read query |
| Analysis rules (3b) | Low-season threshold | 60% | |
| | Early-bird / last-minute lead time | 30 / 14 days | |
| | Shorter stay, family share, price already low | 25% / half of peak / 30% | |
| | Discount tiers | < 40%: 20–30%; 40–50%: 10–20%; 50–60%: 5–10% | Edited as three bounds and labels |
| | Minimum valid bookings | 100 | Compared after dividing by the multiplier (A9) |
| | Exact duplicates | keep | keep / remove |
| | Number of cards | 10 | |

**Built-in presets** (read-only):
- **default**: the table above.
- **baseline**: the naive variant for comparisons. Driver reader, early join, broadcast off, AQE off, no cache, pandas writer.

Users save their own presets by name (C5).

**Where values go:**
- Spark settings go into `get_spark_session(conf=...)`, so they override the shared defaults. The driver memory goes on the command line.
- Pipeline and rule settings are read by the job from `--settings-json`.
- Snapshot (C6): `pipeline_runs.settings` stores every resolved value, plus the effective value Spark reports for each Spark key, plus the cluster seen at start (worker memory and cores from the Spark master's `/json` page).

## 5. Tables (additive)

**`br5.settings`.**
- Engine: `ReplacingMergeTree(updated_at)`, `ORDER BY name`.
- Columns: `name, values Map(String, String), is_deleted UInt8, updated_at`.
- The row `__current__` holds the working values. Other rows are user presets. Built-in presets live in code.
- Fresh mode keeps this table, like `pipeline_runs`.

**`br5.pipeline_runs`, new columns:**
- `criteria Map(String, String)`
- `settings Map(String, String)`
- `rows_used UInt64`
- `rows_excluded Map(String, UInt64)` (reasons, plus the two report-only counts)
- `result_count UInt16`

`rows_read` and `multiplier` are reused. `import_id` is the data the run read.

**`br5.run_month`** (`MergeTree ORDER BY (run_id, target_rank, month)`):
- Columns: `run_id, target_rank (0 = selection), month, demand, bookings, avg_adr Nullable, share_of_peak, is_low, is_peak`.
- Feeds the chart and the figures table.

**`br5.run_card`** (`MergeTree ORDER BY (run_id, target_rank)`):
- Target: `hotel, room_type, country_code, country_name`.
- Season: `low_months Array(UInt8), when_text, peak_month, peak_demand, low_demand, demand_ratio, potential, valid_bookings`.
- Recommendation: `offers Array(String), evidence Array(String), discount, channels Array(String)`.
- Profile figures (low vs peak): bookings, median lead time, average stay, family share, average ADR.

Results are kept for every run (they are small). The monitoring plan will chart them.

## 6. Advanced Settings screen (C1–C7, C12)

- **Layout:** one section per group. Each setting shows its name, a one-line explanation, its default and the current value in an input.
  - A changed value is marked, and has a per-setting **Reset**.
  - **Reset all** is at the top.
  - Analysis rules are their own group, labelled "for the team".
- **Edits are saved straight away** to `__current__` (C2). A wrong type or out-of-range value is not saved and shows a message. Cluster warnings show next to the setting; A8 errors block Run and Import until fixed (C4).
- **Presets:** choose one and **Load**; **Save as** a name; **Delete** (user presets only) (C5).
- **"Needs restart" (read-only, C7):**
  - Worker memory and cores, from the Spark master's `/json` page.
  - ClickHouse memory limit and CPU count, from `system` tables.
  - A link to the restart guide.

## 7. Promotion Finder screen (spec 3.1–3.2)

**Choices:** Hotel, Country and Room type drop-downs (Q1–Q3), each with **All**.
- Countries and room types are offered only with at least the minimum of valid bookings in total (cleaning rules and duplicate setting applied, divided by the multiplier). The lists come from one ClickHouse query, cached per import and per settings.
- Countries are shown as "Portugal (PRT)".
- No data loaded: the page shows the "no data yet" message with a link to Data.

**Find promotions:**
- Launches a run. The button is disabled while busy.
- Progress shows the current step every 2 s, with **Stop** for a run started from this app.

**Result** (the latest succeeded run, from the app or the CLI, headed with its choices):
- **R1:** an Altair bar chart of demand per month. Low months are coloured, with a line at the threshold, and the low-season text ("Nov – Feb: demand is 40% of peak").
- **R2:** cards ranked by potential.
  - Each card shows target, when, why (low vs peak demand, potential in room-nights per month), what (offers, each with its evidence line, and the discount tier), and where (channels).
  - When there is not enough data, or no month falls below the threshold, a message gives the reason instead of a chart.
- **R3:** an expander with the month table and each card's low-vs-peak figures.
- **R4:** an expander with:
  - run time and step durations
  - engine and preset, with settings that differ from the default highlighted
  - read / used / excluded per reason
- **R5:** CSV download of the cards.
- A result computed on an older import is labelled as such.

## 8. Restart guide (`docs/guide/br5/config-guide.md`, C10–C11)

**One entry per setting** that cannot change live: worker memory and cores, container memory and CPU limits (Spark, ClickHouse, Kafka), `SPARK_DAEMON_MEMORY`, ClickHouse server settings, and the Prometheus scrape interval. Each entry gives:
- what it does
- the file and line
- a sample value
- the exact restart command (`docker compose ... up -d <service>`)
- how to confirm it took effect (the Advanced Settings read-only section, `docker stats`, or a `system` table query)

ClickHouse server settings need a new `config.d` entry in `docker-compose.yml`; the guide shows the snippet, and the compose file gets a commented example. The guide ends with "how to run a fair comparison" (C11):
- change one thing at a time
- use the same data and choices
- discard the first run
- repeat 3 times
- compare medians from the run history

## 9. Order of work

| Step | Work | Done when |
|---|---|---|
| 1 | `rules.py` + `tests/br5/test_rules.py` | Unit tests pass |
| 2 | `run_settings.py` + tests; DDL additions; `runs.py` additions | Unit tests pass; DDL re-applies on an existing `br5` without errors |
| 3 | `get_spark_session(conf=)`; `pipeline_spark.py` + `analyze.py` (spark engine) | A manual run writes cards and months; the counts add up |
| 4 | `pipeline_sql.py` (clickhouse engine) | Same cards as the Spark engine |
| 5 | `launch.py`, `job.py` settings, `scripts/br5.py run`, Makefile | `make br5-run` and `make br5-import PRESET=baseline` work |
| 6 | Advanced Settings screen | Edit, reset, presets, validation and read-only section work |
| 7 | Promotion Finder screen, Data page change | Section 10 rows 1–8 checked from the browser |
| 8 | Restart guide, spec and plan updates, result file | Docs match the code; `result-pipeline.md` written |

## 10. Verification (kept small on purpose)

| # | Check | Expected | Covers |
|---|---|---|---|
| 1 | Unit tests: low-season ranges incl. Dec→Jan wrap, per-year average with uneven month coverage, potential, each offer trigger at its boundary, discount tiers, minimum data, ranking ties; settings types, ranges, A8 errors, preset merge | All pass | Rules, C4 |
| 2 | Run All / All / All at ×1, default settings | Succeeds; `read = used + Σ excluded`; `read` = fact rows; up to 10 cards; chart shows a low season | R1–R4, correctness |
| 3 | City Hotel / Portugal / room A, Spark engine vs ClickHouse engine | Same months, demand (to 0.01) and cards | A5, correctness |
| 4 | The same run twice | Identical cards | Determinism (spec 4) |
| 5 | Re-import at ×10, repeat run 2 | Same cards as at ×1 | A9 |
| 6 | Run with preset `baseline` | Snapshot shows its values and Spark's effective values match; the log's `explain()` shows a sort-merge join | C2, C6 |
| 7 | Enter an out-of-range value; set executor memory to 4g | First rejected; second blocks Run with a clear error | C4, A8 |
| 8 | A thin target (for example Resort Hotel + a small country + room G) | "Not enough data" with the count; no chart | Spec 3.2 |
| 9 | Start a run during an import; press Stop on a run | Refused as busy; the stopped run shows as interrupted and the lock clears | D8, A10 |
| 10 | `spark-submit` commands for BR3/BR4 | Unchanged (shared change is additive) | BR3/BR4 |

The result file records a few figures only:
- run time per step for row 2 at ×1 and ×10, Spark vs ClickHouse engine
- exclusion counts per reason
- how many countries and room types qualify
- the overall low season found

## 11. Risks and notes

- **Spark start-up dominates small runs.** At ×1 the ClickHouse engine will likely win by the start-up time alone. That is a finding for the report: the multiplier is what shows when Spark pays off.
- **Driver reader at ×50** may run out of driver memory, like the pandas writer. This is a finding, not a bug.
- **Skew handling** only shows with the early join and broadcast off, and with the skew threshold lowered (our partitions are far below the 256 MB default). The help text says so.
- **Parallel reader slices** each scan the fact table with a `source_row % N` filter. At these sizes a ClickHouse scan takes milliseconds; the read partitions setting caps N.
- **Floating-point sums** can differ in the last digits with partitioning, so figures are rounded to 2 decimals before comparing or showing them.
