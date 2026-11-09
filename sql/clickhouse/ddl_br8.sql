CREATE TABLE IF NOT EXISTS dim_br8_places
(
    place_id String,
    place_name String,
    category String,
    region_id String,
    city_name String,
    country_name String,
    latitude Nullable(Float64),
    longitude Nullable(Float64)
)
ENGINE = MergeTree ORDER BY place_id;

-- One logical visit per user/place/UTC day. Retries and replay do not add visits.
CREATE TABLE IF NOT EXISTS fact_br8_visits
(
    event_id String,
    user_id UInt64,
    place_id String,
    visit_date Date,
    visit_time DateTime64(6, 'UTC'),
    raw_checkins UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (place_id, user_id, visit_date);

CREATE VIEW IF NOT EXISTS v_br8_user_retention AS
SELECT place_id, user_id,
       count() AS visit_count,
       min(visit_date) AS first_visit,
       max(visit_date) AS last_visit,
       arraySort(groupArray(visit_date)) AS visit_days,
       toUInt8(visit_count >= 2) AS has_returned
FROM fact_br8_visits FINAL
GROUP BY place_id, user_id;

CREATE VIEW IF NOT EXISTS v_br8_place_retention AS
SELECT r.place_id, d.place_name, d.category, d.city_name, d.country_name,
       count() AS unique_visitors,
       countIf(r.has_returned = 1) AS returning_visitors,
       sum(r.visit_count) AS total_visits,
       round(100.0 * returning_visitors / unique_visitors, 2) AS repeat_visit_rate,
       min(r.first_visit) AS first_observed_date,
       max(r.last_visit) AS last_observed_date
FROM v_br8_user_retention AS r
INNER JOIN dim_br8_places AS d ON r.place_id = d.place_id
GROUP BY r.place_id, d.place_name, d.category, d.city_name, d.country_name;

-- Contributions occur once at the first day, and once at the second day.
CREATE VIEW IF NOT EXISTS v_br8_daily_retention AS
SELECT place_id, visit_date,
       sum(new_visitors) OVER w AS unique_visitors,
       sum(new_returning) OVER w AS returning_visitors,
       round(100.0 * returning_visitors / unique_visitors, 2) AS repeat_visit_rate
FROM
(
    SELECT place_id, visit_date,
           sum(new_visitors) AS new_visitors,
           sum(new_returning) AS new_returning
    FROM
    (
        SELECT place_id, first_visit AS visit_date, toUInt64(1) AS new_visitors,
               toUInt64(0) AS new_returning FROM v_br8_user_retention
        UNION ALL
        SELECT place_id, visit_days[2] AS visit_date, toUInt64(0) AS new_visitors,
               toUInt64(1) AS new_returning FROM v_br8_user_retention WHERE has_returned = 1
    )
    GROUP BY place_id, visit_date
)
WINDOW w AS (PARTITION BY place_id ORDER BY visit_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

-- ---------------------------------------------------------------------------
-- BR8 benchmark (docs/plan/br7/plan-benchmark.md). Same layout as br6: the
-- TripWorld source imported once into br8 (built in br8_staging, swapped), runs
-- that read it and write their result tables, run history and settings. Table
-- names don't start with fact_, so start.py never truncates them.
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS br8;

-- region_labels.parquet
CREATE TABLE IF NOT EXISTS br8.region
(
    region_id    String,
    city_name    Nullable(String),
    country_name Nullable(String)
)
ENGINE = MergeTree
ORDER BY region_id;

-- metadata/metadata_all.parquet: the columns a run uses, as stored.
CREATE TABLE IF NOT EXISTS br8.place
(
    place_id    String,
    fsq_name    Nullable(String),
    google_name Nullable(String),
    categories  Array(String),
    latitude    Nullable(Float64),
    longitude   Nullable(Float64),
    source_row  UInt32
)
ENGINE = MergeTree
ORDER BY place_id;

-- travel_behaviors.parquet: destination records (r_h != r_o) with c_o exploded,
-- one row per check-in, uncleaned (the run cleans).
CREATE TABLE IF NOT EXISTS br8.raw_checkin
(
    region_id    String,
    user_id      Int64,
    place_id     Nullable(String),
    visit_time   Nullable(DateTime64(6, 'UTC')),
    venue_schema Nullable(String),
    source_row   UInt32,
    import_id    UUID
)
ENGINE = MergeTree
ORDER BY (region_id, source_row);

-- Run history, same columns as br6.pipeline_runs (jobs/common/run_store.py).
CREATE TABLE IF NOT EXISTS br8.pipeline_runs
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

CREATE TABLE IF NOT EXISTS br8.settings
(
    name           String,
    setting_values Map(String, String),
    is_deleted     UInt8,
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY name;

-- A run's daily visits: the same rows prepare_br8.py writes to prepared/visits.
CREATE TABLE IF NOT EXISTS br8.run_visit
(
    run_id       UUID,
    event_id     String,
    user_id      Int64,
    place_id     String,
    visit_date   Date,
    visit_time   DateTime64(6, 'UTC'),
    raw_checkins UInt32
)
ENGINE = MergeTree
ORDER BY (run_id, place_id, user_id, visit_date);

-- A run's attractions: prepare_br8.py's prepared/places, plus repeat-visit counts.
CREATE TABLE IF NOT EXISTS br8.run_place
(
    run_id             UUID,
    place_id           String,
    place_name         String,
    category           String,
    region_id          String,
    city_name          String,
    country_name       String,
    latitude           Nullable(Float64),
    longitude          Nullable(Float64),
    visitors           UInt32,
    returning_visitors UInt32,
    visits             UInt32
)
ENGINE = MergeTree
ORDER BY (run_id, place_id);

-- One flat row per import and run, for Grafana. Also the readable names the
-- dashboards show, and whether a run's answer is the expected one: the
-- fingerprints below are those of tests/br8/benchmark.py EXPECTED (checked
-- against prepare_br8.py on the real data); test_bench.py keeps them equal.
CREATE OR REPLACE VIEW br8.run_history AS
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
    if(h.run_type = 'run', h.criteria['region'], '(import)') AS region,
    if(h.run_type = 'run', h.criteria['bench_block'], '') AS block,
    if(h.run_type = 'run', h.criteria['bench_cell'], '') AS cell,
    h.criteria['bench_repeat'] AS repeat,
    h.settings['cluster.worker_cores'] AS worker_cores,
    h.settings['cluster.clickhouse_max_threads'] AS clickhouse_threads,
    -- Readable names
    multiIf(h.run_type = 'import', 'Import', cell = '', 'Manual run',
            concat('Benchmark ', block, ' · ', cell, if(repeat = 'warmup', ' (warm-up)', ''))) AS what,
    multiIf(region = '(import)', 'All TripWorld data', region = 'ALL', 'All destinations',
            region = 'Q1490', 'Tokyo', region = 'Q1865', 'Kuala Lumpur', ifNull(g.city_name, region)) AS input_name,
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
    transform(region, ['ALL', 'Q1490', 'Q1865'],
        ['visits 107912:7797127441102435416 · places 10639:6599842764725940206',
         'visits 5416:15693983204303305139 · places 533:4136802300216255460',
         'visits 468:14393461901935006069 · places 51:3928297759179194327'], '') AS expected_fingerprint,
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
    h.result_count AS places,
    JSONExtractUInt(h.result_summary, 'visits') AS visits,
    JSONExtractUInt(h.result_summary, 'returning_pairs') AS returning_pairs,
    h.result_summary AS result_summary,
    h.error AS error
FROM br8.pipeline_runs AS h FINAL
LEFT JOIN br8.region AS g ON g.region_id = h.criteria['region'];
