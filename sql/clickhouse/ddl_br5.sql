-- BR5 keeps all its tables in its own database. It is a batch BR, so
-- start.py does not truncate them: the import job replaces the data tables
-- itself (jobs/br5_promotion/job.py). Model: docs/plan/br5/plan-data-model.md.
CREATE DATABASE IF NOT EXISTS br5;

-- Landing layer: the source file as-is ("NULL"/"NA" read as empty).
CREATE TABLE IF NOT EXISTS br5.raw_booking
(
    hotel                          LowCardinality(String),
    is_canceled                    UInt8,
    lead_time                      UInt16,
    arrival_date_year              UInt16,
    arrival_date_month             LowCardinality(String),
    arrival_date_week_number       UInt8,
    arrival_date_day_of_month      UInt8,
    stays_in_weekend_nights        UInt8,
    stays_in_week_nights           UInt8,
    adults                         UInt8,
    children                       Nullable(UInt8),
    babies                         UInt8,
    meal                           LowCardinality(String),
    country                        LowCardinality(Nullable(String)),
    market_segment                 LowCardinality(String),
    distribution_channel           LowCardinality(String),
    is_repeated_guest              UInt8,
    previous_cancellations         UInt8,
    previous_bookings_not_canceled UInt8,
    reserved_room_type             LowCardinality(String),
    assigned_room_type             LowCardinality(String),
    booking_changes                UInt8,
    deposit_type                   LowCardinality(String),
    agent                          Nullable(UInt16),
    company                        Nullable(UInt16),
    days_in_waiting_list           UInt16,
    customer_type                  LowCardinality(String),
    adr                            Float64,
    required_car_parking_spaces    UInt8,
    total_of_special_requests      UInt8,
    reservation_status             LowCardinality(String),
    reservation_status_date        Date,
    import_id                      UUID,
    source_row                     UInt32,
    copy_no                        UInt16
)
ENGINE = MergeTree
ORDER BY (hotel, arrival_date_year, source_row);

-- Star schema. Keys are assigned by sorting the natural values; 0 = Unknown.
CREATE TABLE IF NOT EXISTS br5.dim_date
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

CREATE TABLE IF NOT EXISTS br5.dim_hotel
(
    hotel_key  UInt8,
    hotel_name LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY hotel_key;

CREATE TABLE IF NOT EXISTS br5.dim_country
(
    country_key  UInt16,
    country_code String,
    country_name String
)
ENGINE = MergeTree
ORDER BY country_key;

CREATE TABLE IF NOT EXISTS br5.dim_room_type
(
    room_type_key  UInt8,
    room_type_code LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY room_type_key;

CREATE TABLE IF NOT EXISTS br5.dim_channel
(
    channel_key          UInt8,
    market_segment       LowCardinality(String),
    distribution_channel LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY channel_key;

-- Junk dimension: low-value booking flags grouped into one small table.
CREATE TABLE IF NOT EXISTS br5.dim_booking_profile
(
    profile_key        UInt8,
    meal               LowCardinality(String),
    deposit_type       LowCardinality(String),
    customer_type      LowCardinality(String),
    is_repeated_guest  UInt8,
    reservation_status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY profile_key;

-- One row per booking (per copy when imported with a multiplier).
CREATE TABLE IF NOT EXISTS br5.fact_booking
(
    arrival_date_key               UInt32,
    booking_date_key               UInt32,
    status_date_key                UInt32,
    hotel_key                      UInt8,
    country_key                    UInt16,
    reserved_room_type_key         UInt8,
    assigned_room_type_key         UInt8,
    channel_key                    UInt8,
    profile_key                    UInt8,
    agent_id                       Nullable(UInt16),
    company_id                     Nullable(UInt16),
    arrival_week_number            UInt8,  -- the file's own week number (not always the ISO week)
    is_canceled                    UInt8,
    lead_time                      UInt16,
    weekend_nights                 UInt8,
    week_nights                    UInt8,
    stay_nights                    UInt8,
    adults                         UInt8,
    children                       Nullable(UInt8),
    babies                         UInt8,
    adr                            Float64,
    previous_cancellations         UInt8,
    previous_bookings_not_canceled UInt8,
    booking_changes                UInt8,
    days_in_waiting_list           UInt16,
    required_car_parking_spaces    UInt8,
    total_of_special_requests      UInt8,
    is_duplicate                   UInt8,  -- exact copy of an earlier row in the file
    import_id                      UUID,
    source_row                     UInt32,
    copy_no                        UInt16
)
ENGINE = MergeTree
PARTITION BY intDiv(arrival_date_key, 10000)
ORDER BY (hotel_key, country_key, reserved_room_type_key, arrival_date_key);

-- Run history for imports and analysis runs. Never dropped or swapped by an
-- import. Each update inserts a new version of the row; read with FINAL.
CREATE TABLE IF NOT EXISTS br5.pipeline_runs
(
    run_id         UUID,
    run_type       LowCardinality(String),  -- 'import'
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
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY run_id;
