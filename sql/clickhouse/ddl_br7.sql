-- BR7: unusual arrivals in NYC tourist zones, measured by yellow-taxi drop-offs.

CREATE TABLE IF NOT EXISTS dim_br7_zones
(
    zone_id UInt16,
    zone_name String,
    borough String,
    landmark String
)
ENGINE = MergeTree
ORDER BY zone_id;

-- Built from the zero-filled training months; hour_of_week is New York local time.
CREATE TABLE IF NOT EXISTS dim_br7_zone_hour_baseline
(
    zone_id UInt16,
    hour_of_week UInt16,
    mean_arrivals Float64,
    p90_arrivals Float64,
    training_hours UInt32
)
ENGINE = MergeTree
ORDER BY (zone_id, hour_of_week);

-- One row per zone and closed hour. A retried batch replaces the same key.
CREATE TABLE IF NOT EXISTS fact_br7_zone_hourly
(
    zone_id UInt16,
    observed_hour DateTime('UTC'),
    target_hour DateTime('UTC'),
    observed_arrivals UInt32,
    observed_passengers UInt32,
    usual_arrivals Nullable(Float64),
    predicted_arrivals Nullable(Float64),
    warning_threshold Nullable(Float64),
    alert_level LowCardinality(String),
    processed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (zone_id, observed_hour);

CREATE OR REPLACE VIEW v_br7_zone_hourly AS
SELECT f.zone_id AS zone_id,
       z.zone_name AS zone_name,
       z.landmark AS landmark,
       f.observed_hour AS observed_hour,
       toTimeZone(f.observed_hour, 'America/New_York') AS observed_hour_local,
       f.target_hour AS target_hour,
       f.observed_arrivals AS observed_arrivals,
       f.observed_passengers AS observed_passengers,
       f.usual_arrivals AS usual_arrivals,
       f.predicted_arrivals AS predicted_arrivals,
       f.warning_threshold AS warning_threshold,
       f.alert_level AS alert_level
FROM fact_br7_zone_hourly AS f FINAL
INNER JOIN dim_br7_zones AS z ON f.zone_id = z.zone_id;

-- The replay is historical, so each forecast can be checked against the next hour.
CREATE OR REPLACE VIEW v_br7_forecast_check AS
SELECT cur.zone_id AS zone_id,
       cur.zone_name AS zone_name,
       cur.target_hour AS target_hour,
       cur.predicted_arrivals AS predicted_arrivals,
       cur.warning_threshold AS warning_threshold,
       cur.alert_level AS alert_level,
       nxt.observed_arrivals AS actual_arrivals,
       abs(nxt.observed_arrivals - cur.predicted_arrivals) AS abs_error,
       toUInt8(nxt.observed_arrivals > cur.warning_threshold) AS actual_above_threshold
FROM v_br7_zone_hourly AS cur
INNER JOIN v_br7_zone_hourly AS nxt
    ON nxt.zone_id = cur.zone_id AND nxt.observed_hour = cur.target_hour
WHERE cur.predicted_arrivals IS NOT NULL;

-- ---------------------------------------------------------------------------
-- BR7 benchmark (docs/plan/br7/plan-benchmark.md). Same layout as br8: the 13
-- pinned TLC months imported once into br7 (built in br7_staging, swapped),
-- runs that read them and write their result tables, run history and settings.
-- Table names don't start with fact_, so start.py never truncates them.
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS br7;

-- taxi_zone_lookup.csv, as prepare.py reads it.
CREATE TABLE IF NOT EXISTS br7.zone
(
    zone_id      Int32,
    borough      Nullable(String),
    zone_name    Nullable(String),
    service_zone Nullable(String)
)
ENGINE = MergeTree
ORDER BY zone_id;

-- Every trip of the 13 yellow_tripdata files, with prepare.py load_month()'s
-- columns and casts, unlabelled and uncleaned (the run labels and cleans).
-- pickup_local / dropoff_local hold TLC's New York wall-clock digits, as
-- prepare.py's UTC-session timestamps do.
CREATE TABLE IF NOT EXISTS br7.raw_trip
(
    source_month    LowCardinality(String),
    vendor_id       Nullable(Int32),
    pickup_local    Nullable(DateTime64(6, 'UTC')),
    dropoff_local   Nullable(DateTime64(6, 'UTC')),
    pickup_zone_id  Nullable(Int32),
    dropoff_zone_id Nullable(Int32),
    passenger_count Nullable(Int32),
    trip_distance   Nullable(Float64),
    total_amount    Nullable(Float64),
    source_row      UInt32,
    import_id       UUID
)
ENGINE = MergeTree
ORDER BY (source_month, source_row);

-- Run history, same columns as br8.pipeline_runs (jobs/common/run_store.py).
CREATE TABLE IF NOT EXISTS br7.pipeline_runs
(
    run_id         UUID,
    run_type       LowCardinality(String),
    status         LowCardinality(String),
    step           String,
    started_at     DateTime64(3, 'UTC'),
    finished_at    Nullable(DateTime64(3, 'UTC')),
    duration_ms    Nullable(UInt64),
    step_durations Map(String, UInt32),
    rows_read      UInt64,
    rows_expected  UInt64,
    rows_written   UInt64,
    multiplier     UInt16,
    writer         LowCardinality(String),
    source_file    String,
    source_bytes   UInt64,
    import_id      Nullable(UUID),
    error          String,
    heartbeat_at   DateTime64(3, 'UTC'),
    updated_at     DateTime64(3, 'UTC'),
    criteria       Map(String, String),
    settings       Map(String, String),
    rows_used      UInt64,
    rows_excluded  Map(String, UInt64),
    result_count   UInt16,
    result_summary String,
    spark_app_id   String,
    resources      Map(String, Float64)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY run_id;

CREATE TABLE IF NOT EXISTS br7.settings
(
    name           String,
    setting_values Map(String, String),
    is_deleted     UInt8,
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY name;

-- A run's quality report: summary.json rows_by_quality.
CREATE TABLE IF NOT EXISTS br7.run_quality
(
    run_id       UUID,
    source_month String,
    quality      LowCardinality(String),
    rows         UInt64
)
ENGINE = MergeTree
ORDER BY (run_id, source_month, quality);

-- Tourist-zone trips per month before and after removing duplicates:
-- summary.json tourist_zone_trips_by_month and duplicate_trips_removed_in_tourist_zones.
CREATE TABLE IF NOT EXISTS br7.run_month
(
    run_id       UUID,
    source_month String,
    ok_trips     UInt64,
    trips        UInt64
)
ENGINE = MergeTree
ORDER BY (run_id, source_month);

-- prepared/baseline: zero-filled hourly arrivals per zone and New York hour of week.
CREATE TABLE IF NOT EXISTS br7.run_baseline
(
    run_id         UUID,
    zone_id        UInt16,
    hour_of_week   UInt16,
    mean_arrivals  Float64,
    p90_arrivals   Float64,
    training_hours UInt32
)
ENGINE = MergeTree
ORDER BY (run_id, zone_id, hour_of_week);

-- prepared/replay: the replay month's deduplicated tourist-zone drop-offs.
CREATE TABLE IF NOT EXISTS br7.run_replay
(
    run_id          UUID,
    event_id        String,
    dropoff_zone_id UInt16,
    pickup_zone_id  UInt16,
    dropoff_time    DateTime('UTC'),
    passenger_count Int32
)
ENGINE = MergeTree
ORDER BY (run_id, dropoff_zone_id, dropoff_time, event_id);

-- prepared/zones, plus summary.json replay_events_by_zone.
CREATE TABLE IF NOT EXISTS br7.run_zone
(
    run_id        UUID,
    zone_id       UInt16,
    zone_name     String,
    borough       String,
    landmark      String,
    replay_events UInt64
)
ENGINE = MergeTree
ORDER BY (run_id, zone_id);

-- One flat row per import and run, for Grafana. Also the readable names the
-- dashboards show, and whether a run's answer is the expected one: the
-- fingerprints below are those of tests/br7/benchmark.py EXPECTED (I1 checked
-- against prepare.py's summary.json); test_bench.py keeps them equal.
CREATE OR REPLACE VIEW br7.run_history AS
SELECT
    h.run_id AS run_id,
    h.run_type AS run_type,
    h.status AS status,
    h.step AS step,
    h.started_at AS started_at,
    h.finished_at AS finished_at,
    h.duration_ms / 1000 AS duration_s,
    greatest(h.duration_ms - arraySum(mapValues(h.step_durations)), 0) / 1000 AS outside_steps_s,
    if(h.settings['launch.settings_from'] = '', 'unknown', h.settings['launch.settings_from']) AS settings_from,
    replaceOne(settings_from, 'preset ', '') AS preset,
    if(h.settings['pipeline.engine'] = '' OR h.run_type = 'import', 'spark', h.settings['pipeline.engine']) AS engine,
    if(h.run_type = 'run', h.criteria['first_month'], '(import)') AS first_month,
    if(h.run_type = 'run', h.criteria['bench_block'], '') AS block,
    if(h.run_type = 'run', h.criteria['bench_cell'], '') AS cell,
    h.criteria['bench_repeat'] AS repeat,
    h.settings['cluster.worker_cores'] AS worker_cores,
    h.settings['cluster.clickhouse_max_threads'] AS clickhouse_threads,
    -- Readable names
    multiIf(h.run_type = 'import', 'Import', cell = '', 'Manual run',
            concat('Benchmark ', block, ' · ', cell, if(repeat = 'warmup', ' (warm-up)', ''))) AS what,
    multiIf(first_month = '(import)', 'All 13 TLC months', first_month = '2025-09', '12 + 1 months (full year)',
            first_month = '2026-03', '6 + 1 months', first_month = '2026-07', '2 + 1 months',
            concat('from ', first_month, ' + replay')) AS input_name,
    transform(preset,
        ['bench-baseline', 'bench-default', 'bench-read-4', 'bench-python-udf', 'bench-pandas-udf',
         'bench-no-cache', 'bench-no-broadcast', 'bench-no-aqe', 'bench-pandas-writer',
         'bench-clickhouse', 'bench-clickhouse-nomemlimit'],
        ['Spark · baseline (all optimizations off)', 'Spark · optimized (default)', 'Spark · optimized, 4 read slices',
         'Spark · Python UDF', 'Spark · pandas UDF', 'Spark · no cache', 'Spark · no broadcast join',
         'Spark · AQE off', 'Spark · pandas writer', 'ClickHouse SQL', 'ClickHouse SQL · all CPU threads'],
        concat(if(engine = 'clickhouse', 'ClickHouse SQL', 'Spark'), ' · ', settings_from)) AS variant,
    if(engine = 'clickhouse',
       concat('ClickHouse ', replaceRegexpAll(clickhouse_threads, '[^0-9]', ''), ' CPU'),
       concat('Spark ', worker_cores, ' cores')) AS hardware,
    multiIf(h.status = 'running', 'running…', h.status = 'failed', 'failed',
            concat(toString(round(duration_s, 1)), ' s')) AS time_text,
    concat(formatDateTime(h.started_at, '%m-%d %H:%i'), ' · ', what, ' · ', input_name, ' · ', variant, ' · ',
           time_text) AS label,
    transform(first_month, ['2025-09', '2026-03', '2026-07'],
        [
         'quality 78:1430201692577259842 · months 13:15137985340863003765 · baseline 1680:5612386359426095381 · replay 495883:10744859790416406667 · zones 10:16375376100902537780',
         'quality 42:9220579395180375109 · months 7:7993513530869101767 · baseline 1680:3815081965555152652 · replay 495883:10744859790416406667 · zones 10:16375376100902537780',
         'quality 18:17638448230720137655 · months 3:15746878458273779626 · baseline 1680:7034955573801892843 · replay 495883:10744859790416406667 · zones 10:16375376100902537780'], '') AS expected_fingerprint,
    JSONExtractString(h.result_summary, 'fingerprint') AS fingerprint,
    multiIf(h.run_type = 'import', '', h.status != 'succeeded', '', expected_fingerprint = '', 'not checked',
            fingerprint = expected_fingerprint, '✓ correct', '✗ WRONG') AS correct,
    h.rows_read AS rows_read,
    h.rows_used AS rows_used,
    h.rows_written AS rows_written,
    h.rows_excluded AS rows_excluded,
    h.step_durations AS step_durations,
    h.settings AS settings,
    h.criteria AS criteria,
    h.resources AS resources,
    h.spark_app_id AS spark_app_id,
    h.import_id AS import_id,
    JSONExtractUInt(h.result_summary, 'baseline_cells') AS baseline_cells,
    JSONExtractUInt(h.result_summary, 'replay_events') AS replay_events,
    JSONExtractUInt(h.result_summary, 'trips') AS trips,
    JSONExtractUInt(h.result_summary, 'duplicates') AS duplicates,
    h.result_summary AS result_summary,
    h.error AS error
FROM br7.pipeline_runs AS h FINAL;
