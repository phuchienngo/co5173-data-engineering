-- ════════════════════════════════════════════════════════════════════════
--
--      fact_stay   ← batch.br10_01_stays
--      dim_guest   ← batch.br10_02_dim_guest
--
--
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tourism.fact_stay
(
    review_id      UInt64,
    reviewer_id    UInt64,
    listing_id     UInt64,
    city_id        LowCardinality(String),
    neighbourhood  String,
    room_type      LowCardinality(String),
    accommodates   UInt8,
    review_date    Date,
    trip_no        UInt32,
    is_new_trip    UInt8,
    updated_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(review_date)
ORDER BY (reviewer_id, review_date, review_id);


CREATE TABLE IF NOT EXISTS tourism.dim_guest
(
    reviewer_id           UInt64,
    first_trip_date       Date,
    last_trip_date        Date,
    cohort_month          Date,
    first_city            LowCardinality(String),
    last_city             LowCardinality(String),
    n_reviews             UInt32,
    n_trips               UInt32,
    n_cities              UInt8,
    cities                Array(String),
    max_trips_one_city    UInt32,
    is_repeat             UInt8,                   -- n_trips ≥ 2
    is_repeat_same_city   UInt8,
    is_multi_city         UInt8,                   -- n_cities ≥ 2
    days_to_second_trip   Nullable(Int32),
    avg_gap_days          Nullable(Float32),
    lifespan_days         Int32,
    recency_days          Int32,
    guest_tier            LowCardinality(String),  -- Champion | Loyal | Returning | One-time
    r_score               UInt8,
    f_score               UInt8,
    m_score               UInt8,
    rfm_segment           LowCardinality(String),
    as_of_date            Date,
    updated_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY reviewer_id;
