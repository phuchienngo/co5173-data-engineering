CREATE TABLE IF NOT EXISTS fact_bookings
(
    booking_id       String,
    listing_id        String,
    revenue           Float64,
    revenue_bucket   String,  -- 'low' | 'medium' | 'high'
    ingested_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (listing_id, ingested_at);

-- Real Inside Airbnb listings (same source as BR3's dim_airbnb_listing).
-- Populated by jobs/br4_revenue_monitoring/job.py itself at startup (writes
-- the same listings it broadcasts for the join), not via a static INSERT.
CREATE TABLE IF NOT EXISTS dim_listing_pricing
(
    listing_id       String,
    neighbourhood    String,
    room_type        String,
    price             Float64,
    minimum_nights   Int32
)
ENGINE = MergeTree
ORDER BY listing_id;
