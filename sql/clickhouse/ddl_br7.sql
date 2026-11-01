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
