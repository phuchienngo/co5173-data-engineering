CREATE TABLE IF NOT EXISTS dim_tourist_site
(
    site_id String,
    is_core UInt8
)
ENGINE = MergeTree
ORDER BY site_id;

-- Trained on all source hours before the final 30-day replay window.
CREATE TABLE IF NOT EXISTS dim_site_hour_baseline
(
    site_id String,
    hour_of_week UInt16,
    mean_entries Float64,
    p90_entries Float64,
    training_hours UInt32
)
ENGINE = MergeTree
ORDER BY (site_id, hour_of_week);

CREATE TABLE IF NOT EXISTS fact_tourist_arrivals
(
    site_id String,
    observed_hour DateTime('UTC'),
    target_hour DateTime('UTC'),
    observed_entries UInt32,
    predicted_entries Float64,
    warning_threshold Float64,
    alert_level LowCardinality(String),
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (site_id, observed_hour);
