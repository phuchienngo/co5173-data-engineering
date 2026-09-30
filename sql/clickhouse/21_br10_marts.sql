-- ════════════════════════════════════════════════════════════════════════
--
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tourism.mart_br10_city_overview
(
    city_id                LowCardinality(String),
    city_name              String,
    guests                 UInt32,
    trips                  UInt32,
    reviews                UInt32,
    repeat_guests          UInt32,
    repeat_rate_pct        Float32,
    multi_city_guests      UInt32,
    multi_city_pct         Float32,
    median_days_to_return  Nullable(Float32),
    as_of_date             Date
)
ENGINE = MergeTree
ORDER BY city_id;


CREATE TABLE IF NOT EXISTS tourism.mart_br10_cohort
(
    cohort_month           Date,
    month_offset           UInt16,
    cohort_size            UInt32,
    active_guests          UInt32,
    retention_pct          Float32,
    cumulative_returned    UInt32,
    cumulative_return_pct  Float32
)
ENGINE = MergeTree
ORDER BY (cohort_month, month_offset);


CREATE TABLE IF NOT EXISTS tourism.mart_br10_city_flow
(
    from_city    LowCardinality(String),
    to_city      LowCardinality(String),
    transitions  UInt32,
    guests       UInt32
)
ENGINE = MergeTree
ORDER BY (from_city, to_city);


CREATE TABLE IF NOT EXISTS tourism.mart_br10_rfm_segment
(
    rfm_segment       LowCardinality(String),
    guests            UInt32,
    share_pct         Float32,
    avg_recency_days  Float32,
    avg_trips         Float32,
    avg_cities        Float32
)
ENGINE = MergeTree
ORDER BY rfm_segment;


CREATE TABLE IF NOT EXISTS tourism.mart_br10_yearly
(
    year               UInt16,
    city_id            LowCardinality(String),
    reviews            UInt32,
    guests             UInt32,
    returning_guests   UInt32,
    returning_pct      Float32
)
ENGINE = MergeTree
ORDER BY (city_id, year);
