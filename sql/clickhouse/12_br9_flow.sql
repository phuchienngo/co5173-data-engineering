-- ════════════════════════════════════════════════════════════════════════
--
--      baseline_airport_slot     ← batch.br9_03_flow_baseline
--                                ← streaming.br9_05_stream_arrivals (source='stream')
--      alert_airport_events      ← batch.br9_03_flow_baseline
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tourism.baseline_airport_slot
(
    airport       LowCardinality(String),
    dow_local     UInt8,
    slot_local    UInt8,      -- 0 = 00:00–00:14 … 95 = 23:45–23:59
    n_days        UInt16,
    baseline_mean Float32,
    baseline_p50  Float32,
    baseline_p95  Float32,
    baseline_max  UInt16,
    history_from  Date,
    history_to    Date,
    updated_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (airport, dow_local, slot_local);


CREATE TABLE IF NOT EXISTS tourism.fact_airport_flow_15min
(
    window_ts         DateTime('UTC'),
    airport           LowCardinality(String),
    city_id           LowCardinality(String),
    window_local      DateTime,
    dow_local         UInt8,
    slot_local        UInt8,
    arrivals          UInt16,
    departures        UInt16,
    delayed_arrivals  UInt16,
    cancelled         UInt16,
    diverted          UInt16,
    avg_arr_delay     Nullable(Float32),
    baseline_p50      Float32,
    baseline_p95      Float32,
    capacity          Float32,
    load_pct          Float32,
    delayed_share     Float32,
    flow_severity     UInt8,
    alert_level       LowCardinality(String),
    source            LowCardinality(String),   -- 'batch' | 'stream'
    updated_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_ts)
ORDER BY (airport, window_ts);


CREATE TABLE IF NOT EXISTS tourism.alert_airport_events
(
    window_ts      DateTime('UTC'),
    airport        LowCardinality(String),
    city_id        LowCardinality(String),
    window_local   DateTime,
    flow_severity  UInt8,
    prev_severity  UInt8,
    alert_level    LowCardinality(String),
    arrivals       UInt16,
    capacity       Float32,
    load_pct       Float32,
    delayed_share  Float32,
    message        String,
    source         LowCardinality(String),
    created_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(window_ts)
ORDER BY (airport, window_ts);
