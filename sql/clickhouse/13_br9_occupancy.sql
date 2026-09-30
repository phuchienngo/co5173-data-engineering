-- ════════════════════════════════════════════════════════════════════════
--
--
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tourism.fact_occupancy_night
(
    city_id              LowCardinality(String),
    neighbourhood        String,
    neighbourhood_group  String,
    snapshot_date        Date,
    night_date           Date,
    nights_ahead         UInt16,      -- night_date − snapshot_date
    active_listings      UInt32,
    booked_listings      UInt32,
    capacity_guests      UInt32,
    booked_guests        UInt32,
    occupancy_pct        Float32,
    updated_at           DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(night_date)
ORDER BY (city_id, snapshot_date, night_date, neighbourhood);
