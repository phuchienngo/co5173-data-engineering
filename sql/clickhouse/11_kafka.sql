-- ════════════════════════════════════════════════════════════════════════
--
--
--  ┌──────────────┐  group=clickhouse_raw_landing  ┌───────────────┐
--  │ Kafka topic  │───────────────────────────────▶│ Kafka Engine  │──▶ raw_flight_events
--  │ flight_events│                                └───────────────┘
--  │              │───────────────────────────────▶│ Spark Stream  │──▶ fact_airport_flow_15min
--  └──────────────┘                                └───────────────┘
-- ════════════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS tourism.mv_kafka_to_raw;
DROP TABLE IF EXISTS tourism.kafka_flight_events;

CREATE TABLE tourism.kafka_flight_events
(
    event_id       String,
    flight_key     String,
    airport        LowCardinality(String),
    city_id        LowCardinality(String),
    event_type     LowCardinality(String),   -- arrival | departure | cancelled_* | diverted_arrival
    event_ts       DateTime64(3, 'UTC'),
    scheduled_ts   DateTime64(3, 'UTC'),
    delay_min      Nullable(Int32),
    is_delayed     UInt8,
    carrier        LowCardinality(String),
    flight_number  String,
    origin         LowCardinality(String),
    dest           LowCardinality(String),
    year_month     LowCardinality(String),
    produced_at    DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'flight_events',
    kafka_group_name           = 'clickhouse_raw_landing',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_max_block_size       = 65536,
    kafka_skip_broken_messages = 100,
    input_format_skip_unknown_fields = 1,
    date_time_input_format     = 'best_effort';


CREATE TABLE IF NOT EXISTS tourism.raw_flight_events
(
    event_id       String,
    flight_key     String,
    airport        LowCardinality(String),
    city_id        LowCardinality(String),
    event_type     LowCardinality(String),
    event_ts       DateTime64(3, 'UTC'),
    scheduled_ts   DateTime64(3, 'UTC'),
    delay_min      Nullable(Int32),
    is_delayed     UInt8,
    carrier        LowCardinality(String),
    flight_number  String,
    origin         LowCardinality(String),
    dest           LowCardinality(String),
    year_month     LowCardinality(String),
    produced_at    DateTime64(3, 'UTC'),

    kafka_partition UInt64,
    kafka_offset    UInt64,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 90 DAY;


CREATE MATERIALIZED VIEW tourism.mv_kafka_to_raw
TO tourism.raw_flight_events AS
SELECT
    event_id, flight_key, airport, city_id, event_type, event_ts, scheduled_ts,
    delay_min, is_delayed, carrier, flight_number, origin, dest, year_month, produced_at,
    _partition AS kafka_partition,
    _offset    AS kafka_offset
FROM tourism.kafka_flight_events;


-- ════════════════════════════════════════════════════════════════════
--
--  1) SELECT count(), max(event_ts) FROM tourism.raw_flight_events;
--  2) SELECT table, num_messages_read, last_exception_time, last_exception
--     FROM system.kafka_consumers FORMAT Vertical;
--  3) docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
--         --topic flight_events --from-beginning --max-messages 2
-- ════════════════════════════════════════════════════════════════════
