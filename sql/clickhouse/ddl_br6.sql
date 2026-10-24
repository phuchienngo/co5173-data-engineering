-- BR6 keeps all its tables in its own database. It is a batch BR, so
-- start.py does not truncate them: the import job replaces the data tables
-- itself (jobs/br6_route/job.py). Model: docs/plan/br6/plan-data-model.md.
CREATE DATABASE IF NOT EXISTS br6;

-- Landing layer: the check-in files as-is (empty values stay empty), not
-- repeated by the data multiplier.
CREATE TABLE IF NOT EXISTS br6.raw_checkin
(
    city                   LowCardinality(String),  -- file name, for example new_york
    trail_id               String,
    user_id                UInt32,
    venue_id               UInt32,
    latitude               Nullable(Float64),
    longitude              Nullable(Float64),
    name                   Nullable(String),
    address                Nullable(String),
    venue_category         LowCardinality(String),
    venue_category_id      LowCardinality(String),
    venue_category_id_code UInt16,
    venue_city             LowCardinality(String),
    venue_city_latitude    Float64,
    venue_city_longitude   Float64,
    venue_country          LowCardinality(String),
    timestamp              DateTime('UTC'),
    import_id              UUID,
    source_row             UInt32
)
ENGINE = MergeTree
ORDER BY (city, source_row);

-- Star schema. Keys are assigned by sorting the natural values, from 1.
CREATE TABLE IF NOT EXISTS br6.dim_city
(
    city_key     UInt8,
    city_code    LowCardinality(String),
    city_name    String,
    country_code LowCardinality(String),
    country_name String,
    timezone     String,
    centre_lat   Float64,  -- median of the city's check-ins with a location
    centre_lon   Float64
)
ENGINE = MergeTree
ORDER BY city_key;

CREATE TABLE IF NOT EXISTS br6.dim_place
(
    place_key         UInt32,
    city_key          UInt8,
    venue_id          UInt32,
    name              Nullable(String),
    address           Nullable(String),
    latitude          Nullable(Float64),
    longitude         Nullable(Float64),
    has_location      UInt8,
    main_category_key UInt16,  -- most frequent category; a tie goes to the latest check-in's
    categories_seen   UInt16
)
ENGINE = MergeTree
ORDER BY (city_key, place_key);

CREATE TABLE IF NOT EXISTS br6.dim_visitor
(
    visitor_key UInt32,
    city_key    UInt8,
    user_id     UInt32
)
ENGINE = MergeTree
ORDER BY visitor_key;

CREATE TABLE IF NOT EXISTS br6.dim_category
(
    category_key  UInt16,
    category_id   String,  -- Foursquare category id
    category_name String
)
ENGINE = MergeTree
ORDER BY category_key;

CREATE TABLE IF NOT EXISTS br6.dim_place_type
(
    place_type_key  UInt8,
    place_type_name String,
    is_offered      UInt8,
    sort_order      UInt8
)
ENGINE = MergeTree
ORDER BY place_type_key;

-- Outrigger of dim_category: the editable grouping, reloaded by every run
-- from data/br6/category_place_type.csv.
CREATE TABLE IF NOT EXISTS br6.category_place_type
(
    category_key   UInt16,
    place_type_key UInt8
)
ENGINE = MergeTree
ORDER BY category_key;

CREATE TABLE IF NOT EXISTS br6.dim_area
(
    area_key  UInt16,
    city_key  UInt8,
    area_name String,
    area_lat  Float64,
    area_lon  Float64
)
ENGINE = MergeTree
ORDER BY area_key;

CREATE TABLE IF NOT EXISTS br6.dim_date
(
    date_key     UInt32,  -- YYYYMMDD
    date         Date,
    year         UInt16,
    quarter      UInt8,
    month        UInt8,
    month_name   LowCardinality(String),
    iso_week     UInt8,
    day_of_month UInt8,
    day_of_week  UInt8,   -- 1 = Monday
    is_weekend   UInt8
)
ENGINE = MergeTree
ORDER BY date_key;

-- One row per check-in (per copy when imported with a multiplier). A copy
-- counts as a different visitor and trail: (visitor_key, copy_no).
CREATE TABLE IF NOT EXISTS br6.fact_checkin
(
    city_key     UInt8,
    place_key    UInt32,
    visitor_key  UInt32,
    category_key UInt16,  -- as recorded on the check-in
    area_key     UInt16,  -- venue_city as recorded on the check-in
    date_key     UInt32,  -- local date in the city's time zone
    checkin_time DateTime('UTC'),
    trail_id     String,
    import_id    UUID,
    source_row   UInt32,
    copy_no      UInt16
)
ENGINE = MergeTree
PARTITION BY city_key
ORDER BY (city_key, place_key, visitor_key, checkin_time);

-- Run history for imports and runs, same columns as br5.pipeline_runs. Never
-- dropped or swapped by an import. Each update inserts a new version of the
-- row; read with FINAL.
CREATE TABLE IF NOT EXISTS br6.pipeline_runs
(
    run_id         UUID,
    run_type       LowCardinality(String),  -- 'import' | 'run'
    status         LowCardinality(String),  -- 'running' | 'succeeded' | 'failed'
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
    criteria       Map(String, String),     -- import: cities; run: the user's choices
    settings       Map(String, String),     -- snapshot of every setting used
    rows_used      UInt64,
    rows_excluded  Map(String, UInt64),     -- per cleaning reason
    result_count   UInt16,
    result_summary String,                  -- JSON
    spark_app_id   String,
    resources      Map(String, Float64)     -- spark.* (total, @step), clickhouse.*
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY run_id;

-- Current settings (__current__) and named presets. Kept by fresh mode.
CREATE TABLE IF NOT EXISTS br6.settings
(
    name           String,
    setting_values Map(String, String),
    is_deleted     UInt8,
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY name;

-- Route Finder (docs/plan/br6/plan-pipeline.md, section 6) ------------------

-- Check-ins and different visitors per place, counted in the original data
-- (before the multiplier). Built by the import, swapped with the data tables.
CREATE TABLE IF NOT EXISTS br6.place_stats
(
    city_key  UInt8,
    place_key UInt32,
    checkins  UInt32,
    visitors  UInt32
)
ENGINE = MergeTree
ORDER BY (city_key, place_key);

-- The points of a run's route: start, stops, end.
CREATE TABLE IF NOT EXISTS br6.run_point
(
    run_id        UUID,
    seq           UInt8,                   -- 0 = start
    role          LowCardinality(String),  -- start | stop | end
    place_key     UInt32,
    venue_id      UInt32,
    name          String,
    latitude      Float64,
    longitude     Float64,
    km_from_start Float64
)
ENGINE = MergeTree
ORDER BY (run_id, seq);

-- Every place a run found near its route, with its status and scores.
CREATE TABLE IF NOT EXISTS br6.run_place
(
    run_id         UUID,
    place_key      UInt32,
    venue_id       UInt32,
    name           String,
    address        String,
    category_name  String,
    place_type     LowCardinality(String),
    detour_km      Float64,
    position_km    Float64,
    leg            UInt8,                   -- 0 = from the start to the next point
    visitors       UInt32,
    checkins       UInt32,
    visitor_visits UInt32,
    type_share     Float64,
    popularity     Float64,
    fit            Float64,
    closeness      Float64,
    score          Float64,
    rank           UInt16,                  -- 0 = not suggested
    status         LowCardinality(String)   -- suggested | below the top N | few visitors | type not chosen | not offered
)
ENGINE = MergeTree
ORDER BY (run_id, place_key);
