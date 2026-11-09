-- Alternative to jobs/br3_sentiment_analysis/job.py: same tokenization and
-- English filter (Spark's 181 English stop words, loaded into en_stopwords),
-- done by a materialized view on a Kafka engine table. VADER sentiment has
-- no ClickHouse equivalent, so this variant has no sentiment_label.
CREATE TABLE IF NOT EXISTS fact_reviews_ch
(
    review_id    String,
    listing_id   String,
    is_english   UInt8,
    keywords     Array(String),
    event_time       DateTime64(3),  -- Kafka message timestamp
    ingested_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (listing_id, ingested_at);

CREATE TABLE kafka_reviews_ch
(
    listing_id String,
    id         String,
    comments   String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = '{topic}',
         kafka_group_name = '{group}',
         kafka_format = 'JSONEachRow',
         -- default block is ~1M rows: 200k long reviews in one block exceeded
         -- the 1 GB memory limit (MEMORY_LIMIT_EXCEEDED, retried 105 times)
         kafka_max_block_size = 10000;

CREATE MATERIALIZED VIEW mv_reviews_ch TO fact_reviews_ch AS
WITH
    (SELECT groupArray(word) FROM en_stopwords) AS stop,
    extractAll(lower(comments), '[a-z]+') AS words
SELECT
    id AS review_id,
    listing_id,
    toUInt8(length(words) > 0 AND arrayCount(w -> has(stop, w), words) / length(words) >= 0.2) AS is_english,
    arrayFilter(w -> NOT has(stop, w), extractAll(lower(comments), '[a-z]{3,}')) AS keywords,
    _timestamp_ms AS event_time
FROM kafka_reviews_ch
WHERE listing_id IN (SELECT listing_id FROM dim_airbnb_listing);
