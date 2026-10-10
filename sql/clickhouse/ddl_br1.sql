-- BR1 Tour route: serving tables for the Leaflet map and Grafana.
-- Applied by scripts/start.py (clickhouse-client --multiquery); fact_* tables are truncated by the job at start.
CREATE DATABASE IF NOT EXISTS tour;

CREATE TABLE IF NOT EXISTS tour.dim_landmark (osm_id String, name String, category LowCardinality(String), city LowCardinality(String), lat Float64, lon Float64, ticket_vnd UInt32) ENGINE = MergeTree ORDER BY (city, osm_id);
CREATE TABLE IF NOT EXISTS tour.dist_matrix (city LowCardinality(String), from_id String, to_id String, dist_km Float32, fare_vnd UInt32) ENGINE = MergeTree ORDER BY (city, from_id, to_id);
CREATE TABLE IF NOT EXISTS tour.fact_tour (request_id UInt64, city LowCardinality(String), n_stops UInt8, stop_ids Array(String), stop_names Array(String), lats Array(Float64), lons Array(Float64), total_km Float32, total_fare_vnd UInt32, total_ticket_vnd UInt32, total_cost_vnd UInt32) ENGINE = ReplacingMergeTree ORDER BY (city, n_stops, total_cost_vnd, request_id);
CREATE TABLE IF NOT EXISTS tour.fact_tour_leg (request_id UInt64, seq UInt8, from_id String, to_id String, to_name String, leg_km Float32, leg_fare_vnd UInt32, ticket_vnd UInt32, cum_cost_vnd UInt32) ENGINE = ReplacingMergeTree ORDER BY (request_id, seq);
CREATE TABLE IF NOT EXISTS tour.job_run (run_mode LowCardinality(String), stage LowCardinality(String), seconds Float32, rows UInt64, requests UInt32, ran_at DateTime) ENGINE = MergeTree ORDER BY (ran_at, run_mode, stage);
CREATE OR REPLACE VIEW tour.v_latest_run AS  -- latest ran_at per run_mode, all stages
  SELECT * FROM tour.job_run WHERE (run_mode, ran_at) IN (SELECT run_mode, max(ran_at) FROM tour.job_run GROUP BY run_mode);
