-- ════════════════════════════════════════════════════════════════════════
--
--             --multiquery < sql/clickhouse/00_database.sql
--
--  ────────────────────────────────────────────────────────────────────
--  ────────────────────────────────────────────────────────────────────
--  project.dataset.table        →  database.table
--  bytes_billed                 →  system.query_log.read_bytes
--
-- ════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS tourism;


-- ────────────────────────────────────────────────────────────────────
-- ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tourism.pipeline_log
(
    run_ts        DateTime              DEFAULT now(),
    run_id        String,
    job_name      LowCardinality(String),           -- 'br9_03_flow_baseline'
    br            LowCardinality(String),           -- 'BR9' | 'BR10' | 'COMMON'
    step          String,
    status        LowCardinality(String),           -- STARTED | SUCCESS | FAILED
    rows_written  UInt64                DEFAULT 0,
    duration_sec  Float32               DEFAULT 0,
    message       String                DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_ts)
ORDER BY (run_ts, job_name)
TTL run_ts + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;


-- ────────────────────────────────────────────────────────────────────
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_pipeline_status AS
SELECT
    job_name,
    br,
    argMax(status,       run_ts) AS last_status,
    max(run_ts)                  AS last_run_ts,
    argMax(rows_written, run_ts) AS last_rows,
    argMax(duration_sec, run_ts) AS last_duration_sec,
    argMax(message,      run_ts) AS last_message,
    countIf(status = 'FAILED')   AS failures_all_time
FROM tourism.pipeline_log
GROUP BY job_name, br
ORDER BY last_run_ts DESC;
