-- ════════════════════════════════════════════════════════════════════════
--
--      dim_destination, dim_airport   ← batch.br9_01_dim
--      dim_neighbourhood              ← batch.br9_04_occupancy
--
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tourism.dim_destination
(
    city_id           LowCardinality(String),   -- 'las-vegas'
    city_name         String,                   -- 'Las Vegas'
    state             LowCardinality(String),
    timezone          LowCardinality(String),   -- IANA, 'America/Los_Angeles'
    airbnb_path       String,                   -- 'united-states/nv/clark-county-nv'
    airbnb_snapshots  Array(Date),
    airports          Array(String),            -- ['LAS']
    latitude          Float64,
    longitude         Float64,
    updated_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY city_id;


CREATE TABLE IF NOT EXISTS tourism.dim_airport
(
    airport             LowCardinality(String),  -- IATA, 'LAS'
    city_id             LowCardinality(String),
    airport_name        String,
    bts_city_name       String,                  -- 'Las Vegas, NV'
    bts_city_market_id  UInt32,
    latitude            Float64,
    longitude           Float64,
    timezone            LowCardinality(String),
    arrivals_per_day    Float32,
    updated_at          DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY airport;


CREATE TABLE IF NOT EXISTS tourism.dim_neighbourhood
(
    city_id              LowCardinality(String),
    neighbourhood        String,
    neighbourhood_group  String,
    latitude             Float64,
    longitude            Float64,
    snapshot_date        Date,
    listings             UInt32,
    active_listings      UInt32,
    capacity_guests      UInt32,
    updated_at           DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (city_id, neighbourhood);
