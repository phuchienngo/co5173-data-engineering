-- Alternative to jobs/br4_revenue_monitoring/job.py: ClickHouse consumes
-- Kafka itself (Kafka engine) and a materialized view does the same join,
-- revenue rule and bucketing in-database (ELT instead of Spark ETL).
CREATE TABLE IF NOT EXISTS fact_bookings_ch
(
    booking_id       String,
    listing_id       String,
    revenue          Float64,
    revenue_bucket   String,
    event_time       DateTime64(3),  -- Kafka message timestamp
    ingested_at      DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (listing_id, ingested_at);

CREATE TABLE kafka_bookings_ch
(
    listing_id    String,
    id            String,
    revenue       String,
    nights_booked String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = '{topic}',
         kafka_group_name = '{group}',
         kafka_format = 'JSONEachRow',
         -- default block is ~1M rows: a 200k-review backlog in one block
         -- exceeded the 1 GB memory limit, so both BRs use 10k-row blocks
         kafka_max_block_size = 10000;

CREATE MATERIALIZED VIEW mv_bookings_ch TO fact_bookings_ch AS
WITH coalesce(toFloat64OrNull(k.revenue), d.price * coalesce(toInt32OrNull(k.nights_booked), 1)) AS rev
SELECT
    k.id AS booking_id,
    k.listing_id AS listing_id,
    rev AS revenue,
    multiIf(rev < 2000, 'low', rev < 8000, 'medium', 'high') AS revenue_bucket,
    k._timestamp_ms AS event_time
FROM kafka_bookings_ch AS k
INNER JOIN dim_listing_pricing AS d ON k.listing_id = d.listing_id;
