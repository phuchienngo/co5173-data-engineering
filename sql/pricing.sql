-- BR1 Dynamic Pricing: serving layer for Looker Studio.
-- Apply: docker exec -i clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" --multiquery < sql/pricing.sql
CREATE DATABASE IF NOT EXISTS pricing;

CREATE TABLE IF NOT EXISTS pricing.demand_ratio_hourly
(
    window_start     DateTime,
    location_id      UInt16,
    zone             String,
    landmark         String,
    active_bookings  UInt32,
    avg_fare         Float64,
    capacity         Nullable(UInt32),
    demand_ratio_pct Nullable(Float64),
    suggested_fare   Float64,
    price_action     String,
    source           String,  -- 'batch' = history, 'stream' = live replay
    updated_at       DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (location_id, window_start);

-- Hourly series for charts; FINAL keeps only the latest version of each zone-hour
-- (the streaming job re-emits a window every time its count changes).
CREATE OR REPLACE VIEW pricing.v_demand_ratio AS
SELECT
    t.window_start               AS window_start,
    toDate(t.window_start)       AS date,
    toHour(t.window_start)       AS hour_of_day,
    t.location_id                AS location_id,
    t.zone                       AS zone,
    t.landmark                   AS landmark,
    t.active_bookings            AS active_bookings,
    t.capacity                   AS capacity,
    round(t.demand_ratio_pct, 1) AS demand_ratio_pct,
    round(t.avg_fare, 2)         AS avg_fare,
    round(t.suggested_fare, 2)   AS suggested_fare,
    t.price_action               AS price_action,
    t.source                     AS source
FROM pricing.demand_ratio_hourly AS t FINAL;

-- Latest hour per landmark = the current pricing suggestion.
CREATE OR REPLACE VIEW pricing.v_latest_suggestion AS
SELECT *
FROM pricing.v_demand_ratio
WHERE (location_id, window_start) IN
(
    SELECT location_id, max(window_start)
    FROM pricing.v_demand_ratio
    GROUP BY location_id
);
