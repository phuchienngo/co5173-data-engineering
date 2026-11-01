CREATE TABLE IF NOT EXISTS fact_reviews
(
    review_id        String,
    listing_id        String,
    sentiment_label  String,
    ingested_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (listing_id, ingested_at);

-- Real Bangkok Airbnb listings (Inside Airbnb). Populated by
-- jobs/br3_sentiment_analysis/job.py itself at startup (writes the same
-- listings it broadcasts for the join), not via a static INSERT here.
CREATE TABLE IF NOT EXISTS dim_airbnb_listing
(
    listing_id    String,
    name           String,
    neighbourhood String,
    latitude       Float64,
    longitude      Float64,
    room_type      String
)
ENGINE = MergeTree
ORDER BY listing_id;
