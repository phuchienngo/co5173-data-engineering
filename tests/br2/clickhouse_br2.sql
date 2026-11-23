-- Alternative to jobs/br2_dynamic_pricing/job.py run_stream: ClickHouse consumes
-- Kafka itself (Kafka engine), a materialized view filters the landmarks and keeps
-- hourly partial aggregates (ELT). The capacity join and the pricing rule are
-- applied at read time in v_demand_ratio_ch, as in the Spark job's write_ratios.
-- pricing.dim_capacity_ch and pricing.dim_landmark_ch are loaded by the harness.
CREATE TABLE IF NOT EXISTS pricing.dim_capacity_ch
(
    location_id UInt16,
    day_type    String,
    hour_of_day UInt8,
    capacity    UInt32
)
ENGINE = MergeTree
ORDER BY (location_id, day_type, hour_of_day);

CREATE TABLE IF NOT EXISTS pricing.dim_landmark_ch
(
    location_id UInt16,
    zone        String,
    landmark    String
)
ENGINE = MergeTree
ORDER BY location_id;

CREATE TABLE pricing.demand_ratio_hourly_ch
(
    location_id     UInt16,
    window_start    DateTime('UTC'),
    active_bookings SimpleAggregateFunction(sum, UInt64),
    fare_sum        SimpleAggregateFunction(sum, Float64),
    event_time      SimpleAggregateFunction(max, DateTime64(3)),  -- newest Kafka timestamp in the window
    ingested_at     SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree
ORDER BY (location_id, window_start);

CREATE TABLE pricing.kafka_trips_ch
(
    service         String,
    pickup_datetime String,
    pu_location_id  UInt16,
    do_location_id  UInt16,
    fare_amount     Float64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = '{topic}',
         kafka_group_name = '{group}',
         kafka_format = 'JSONEachRow',
         -- default block is ~1M rows, the container has 1 GB, so 10k-row blocks as in BR3/BR4
         kafka_max_block_size = 10000;

CREATE MATERIALIZED VIEW pricing.mv_trips_ch TO pricing.demand_ratio_hourly_ch AS
SELECT
    do_location_id AS location_id,
    toDateTime(toStartOfHour(parseDateTime64BestEffort(pickup_datetime, 3, 'UTC')), 'UTC') AS window_start,
    count() AS active_bookings,
    sum(fare_amount) AS fare_sum,
    max(ifNull(_timestamp_ms, toDateTime64(0, 3))) AS event_time,
    max(now64(3)) AS ingested_at
FROM pricing.kafka_trips_ch
WHERE do_location_id IN ({landmark_ids})
GROUP BY location_id, window_start;

CREATE OR REPLACE VIEW pricing.v_demand_ratio_ch AS
WITH
    a.active_bookings AS bookings,
    a.fare_sum / a.active_bookings AS avg_fare,
    if(c.capacity > 0, bookings * 100.0 / c.capacity, NULL) AS ratio,
    coalesce(ratio > 80, false) AS surge
SELECT
    a.window_start AS window_start,
    a.location_id AS location_id,
    l.zone AS zone,
    l.landmark AS landmark,
    bookings AS active_bookings,
    avg_fare,
    c.capacity AS capacity,
    ratio AS demand_ratio_pct,
    if(surge, avg_fare * 1.15, avg_fare) AS suggested_fare,
    if(surge, 'Nhu cầu > 80%: Gợi ý tăng giá 15%', 'Giữ giá') AS price_action,
    a.event_time AS event_time,
    a.ingested_at AS ingested_at
FROM
(
    SELECT location_id, window_start, active_bookings, fare_sum, event_time, ingested_at
    FROM pricing.demand_ratio_hourly_ch FINAL
) AS a
INNER JOIN pricing.dim_landmark_ch AS l ON a.location_id = l.location_id
LEFT JOIN pricing.dim_capacity_ch AS c ON a.location_id = c.location_id
    AND c.day_type = if(toDayOfWeek(a.window_start, 0, 'UTC') IN (6, 7), 'weekend', 'weekday')
    AND c.hour_of_day = toHour(a.window_start, 'UTC')
SETTINGS join_use_nulls = 1;  -- no capacity row -> NULL, as Spark's left join
