# BR1: Dynamic Pricing Insights

**Business Requirement:** Hỗ trợ định giá linh hoạt thời gian thực (Dynamic Pricing Insights). The platform ingests NYC taxi trip fact tables and derives demand ratios (active bookings vs. landmark zone capacity) per hour. A machine-learning-ready pipeline streams these ratios into ClickHouse, where Looker Studio charts the fill trend and automatically suggests price adjustments: "Nhu cầu > 80%: Gợi ý tăng giá 15%" when demand exceeds capacity, and "Giữ giá" (hold price) otherwise.

---

## 1. Data Sources

| Source | Type | Volume | Schema | Public? | Notes |
|--------|------|--------|--------|---------|-------|
| **NYC TLC Yellow Taxi** | Trip records | ~3–4M trips/month | `tpep_pickup_datetime` → `pickup_datetime`, `DOLocationID` → `dropoff_location`, `fare_amount` | Yes | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. Monthly parquet at https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet. **Chosen for BR1.** |
| **NYC TLC FHVHV** (Uber/Lyft) | Trip records | ~20M trips/month | `pickup_datetime`, `base_passenger_fare` | Yes | Scale-up option via `--service fhvhv` flag. |
| **TLC Taxi Zone Lookup** | Zone reference | 263 zones | LocationID, zone name | Yes | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv. Joined by location ID. |
| **TLC Monthly Industry Metrics** | Aggregate metrics | Monthly | Active vehicles (citywide only) | Yes | https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv. Citywide only; not per-zone. Why capacity is derived instead. |
| **Inside Airbnb NYC** | Room availability | Variable | Calendar, listing metadata | Yes | https://insideairbnb.com/get-the-data/. Candidate for "rooms" variant (out of scope for BR1). |

**Why not others:** Chicago taxi (BigQuery historical archive ends 2023), Kaggle hotel-booking-demand (119k rows, too small for statistical validity), Vietnam (no public trip- or booking-level data at scale).

---

## 2. Architecture

**Data flow:**

```
scripts/download_tlc.sh (yellow 2025-07 2026-07)
          ↓
    data/br1/raw/{yellow,fhvhv}/*.parquet (fact: 3–4M rows/month)
          ↓
jobs/batch_fact_capacity.py
    - Read raw parquet via read_trips()
    - Write fact_trips → data/br1/lake/fact_trips (partitioned by service, pickup_month)
    - Compute P95 capacity per (location_id, day_type, hour_of_day) over baseline 2025-07..2026-06
    - Write dim_capacity → data/br1/lake/dim_capacity
    - Compute hourly demand ratios (baseline period) → INSERT ClickHouse pricing.demand_ratio_hourly (source='batch')
          ↓
jobs/publish_trips.py --month 2026-07
    - Read fact_trips for a replay month
    - Sort by pickup_datetime, serialize to JSON, write to Kafka topic tlc.trips
          ↓
Kafka (topic: tlc.trips) — simulates live feed
          ↓
jobs/stream_demand_ratio.py
    - Consume tlc.trips via Kafka Structured Streaming
    - 1-hour tumbling windows per landmark (do_location_id ∈ LANDMARKS)
    - Join with capacity
    - Compute demand_ratio_pct, price_action, suggested_fare
    - Write (update mode) → ClickHouse pricing.demand_ratio_hourly (source='stream')
          ↓
ClickHouse (ReplacingMergeTree, MySQL port 9004)
    - Views: v_demand_ratio (hourly series), v_latest_suggestion (current prices)
          ↓
Looker Studio (via bore.pub tunnel)
    - Charts: time series (demand_ratio_pct over time, by landmark)
    - Heatmap: landmark × hour_of_day
    - Suggestion table: landmark, demand_ratio_pct, avg_fare, suggested_fare, price_action
    - Scorecard: count of zones > 80%
```

---

## 3. Metric Definitions

**Landmark zones (16 target locations):**

| Location ID | Zone Name | Landmark Label |
|-------------|-----------|---|
| 12 | Battery Park | Statue of Liberty ferry |
| 43 | Central Park | Central Park |
| 45 | Chinatown | Chinatown |
| 66 | DUMBO/Vinegar Hill | Brooklyn Bridge / DUMBO |
| 132 | JFK Airport | JFK Airport |
| 138 | LaGuardia Airport | LaGuardia Airport |
| 144 | Little Italy/NoLiTa | Little Italy |
| 161 | Midtown Center | Rockefeller Center |
| 186 | Penn Station/Madison Sq West | Madison Square Garden |
| 209 | Seaport | South Street Seaport |
| 211 | SoHo | SoHo |
| 230 | Times Sq/Theatre District | Times Square & Broadway |
| 234 | Union Sq | Union Square |
| 246 | West Chelsea/Hudson Yards | High Line & Hudson Yards |
| 249 | West Village | West Village |
| 261 | World Trade Center | 9/11 Memorial & One WTC |

**Active Bookings:** Count of trips whose `DOLocationID` (dropoff location) matches the landmark zone, grouped by pickup hour (`window_start`).

**Capacity:** P95 (95th percentile) of hourly active bookings per (location_id, day_type, hour_of_day) computed over the baseline period 2025-07 through 2026-06. This is a "practical capacity" derived statistically, since no public per-zone fleet supply data exists. Hours with zero bookings are excluded, so very quiet zones may slightly overestimate capacity.

**Demand Ratio:** `(active_bookings × 100) / capacity`, or `NULL` if capacity is 0 or missing.

**Price Action & Suggested Fare:**
- If `demand_ratio_pct > 80`: action = `"Nhu cầu > 80%: Gợi ý tăng giá 15%"`, suggested_fare = `avg_fare × 1.15`
- Otherwise: action = `"Giữ giá"`, suggested_fare = `avg_fare`

**Timestamps:** NYC local wall-clock time (not UTC-shifted). The pipeline keeps UTC as the session timezone to preserve hour slot boundaries unshifted.

---

## 4. Runbook

BR1 owns `data/br1/`, matching the per-BR layout used by BR3 and BR4. See
[`data/br1/README.md`](../data/br1/README.md) for file layout and source URLs.

With Docker Desktop running, download and run the full pipeline:

```bash
bash scripts/run_br1.sh yellow 2025-07 2026-06 2026-07
```

Arguments: service, baseline start, baseline end, replay month. The script reuses
downloaded files, initializes the pricing schema, computes the baseline, publishes
the replay month to `tlc.trips`, and runs the pricing stream in the foreground.
Existing Kafka records/checkpoints are preserved; use the reset section for a fresh replay.
To download data only, use Step 0 below.

**Prerequisites:**
- Docker Desktop running (Linux: also `chmod -R a+rwX data` after download to allow Spark uid 1001 write access).
- Git repo cloned and working directory is repo root.

### Step 0: Download NYC TLC Data

```bash
scripts/download_tlc.sh yellow 2025-07 2026-07
```

This downloads ~13 monthly yellow taxi parquet files (~1 GB total) to `data/br1/raw/yellow/`. The script also downloads `data/br1/raw/taxi_zone_lookup.csv` if missing.

### Step 1: Start Docker Services

```bash
docker compose --profile full --profile monitoring --profile br1 up -d --build clickhouse kafka kafka-ui spark-master spark-worker pyspark prometheus grafana
```

The BR1 driver uses the shared Spark 4.0 image and matching Scala 2.13 Kafka connector. Monitoring provisions both the shared demo and BR1 pricing dashboards. The upstream `make demo` runner discovers BR3/BR4; run BR1 with the commands below.

Wait for healthchecks to pass (about 30 seconds):

```bash
docker compose ps
```

### Step 2: Initialize ClickHouse Schema

```bash
docker exec -i clickhouse clickhouse-client --password "${CLICKHOUSE_PASSWORD:-clickhouse}" --multiquery < sql/pricing.sql
```

Creates database `pricing`, table `pricing.demand_ratio_hourly` (ReplacingMergeTree), and views `v_demand_ratio` and `v_latest_suggestion`.

### Step 3: Self-Check

```bash
docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/common.py
```

Expected output: `pricing self-check OK`. This runs the unit tests in `common.py` to verify pricing logic.

### Step 4: Batch — Compute Baseline Capacity

```bash
docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/batch_fact_capacity.py
```

**Optional arguments** (all have sensible defaults):
- `--service yellow` — TLC service name (`yellow` or `fhvhv`). Default: `yellow`.
- `--baseline-from 2025-07` — First month of baseline period (YYYY-MM). Default: `2025-07`.
- `--baseline-to 2026-06` — Last month of baseline period (YYYY-MM). Default: `2026-06`.

This step:
1. Reads all parquet files under `data/br1/raw/yellow/`
2. Writes fact_trips to `data/br1/lake/fact_trips`
3. Computes P95 capacity and writes to `data/br1/lake/dim_capacity`
4. Inserts historical demand ratios (baseline months only, source='batch') into ClickHouse

Typical output:
```
fact rows (yellow): 42500000, baseline landmark-hours: 8000, capacity slots: 1500
```

### Step 5: Replay — Publish One Month to Kafka

```bash
docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/publish_trips.py --month 2026-07
```

**Optional arguments:**
- `--month 2026-07` — Replay month (YYYY-MM). Default: `2026-07`.
- `--service yellow` — TLC service name. Default: `yellow`.

This serializes all trips from the specified month to JSON and writes them to Kafka topic `tlc.trips` in pickup_datetime order. Typical output:
```
published 3245000 yellow trips for 2026-07 to tlc.trips
```

### Step 6: Stream — Start Real-Time Demand Ratio Pipeline

Run in the background with logging:

```bash
docker exec -d -e MAX_OFFSETS=50000 pyspark bash -c 'bash /opt/jobs/submit.sh /opt/jobs/stream_demand_ratio.py > /opt/data/br1/stream.log 2>&1'
```

The stream is capped at 1 core (`spark.cores.max=1`), so batch and publish jobs can run while it is live.

Then tail the log:

```bash
tail -f data/br1/stream.log
```

**Environment variable `MAX_OFFSETS`** controls replay speed:
- **Default:** 20000 (about 4 hours of yellow taxi data per 10 s micro-batch)
- **50000:** Roughly 10 hours per 10 s micro-batch (faster replay)
- **10000:** Roughly 2 hours per 10 s micro-batch (slower replay, lower latency simulation)

Yellow taxi averages ~5000 trips/hour, so `MAX_OFFSETS=20000` represents a ~4-hour window of live traffic per micro-batch trigger.

**To stop streaming:**

```bash
docker compose restart pyspark
```

### Step 7: Check Results in ClickHouse

Connect to ClickHouse and query:

```bash
docker exec -i clickhouse clickhouse-client --password "${CLICKHOUSE_PASSWORD:-clickhouse}" --multiquery
```

**Query 1: Row counts and demand distribution**

```sql
SELECT
  source,
  COUNT(*) as n_rows,
  uniq(location_id) as n_zones,
  min(window_start) as earliest_hour,
  max(window_start) as latest_hour,
  countIf(demand_ratio_pct > 80) as n_surge_hours
FROM pricing.v_demand_ratio
GROUP BY source
ORDER BY source DESC;
```

Expected: ~8000 hours (baseline) + growing stream hours. Surge hours ≈ 10–20% typically.

**Query 2: Latest suggestions**

```sql
SELECT * FROM pricing.v_latest_suggestion ORDER BY demand_ratio_pct DESC;
```

Expected: Latest window_start per landmark, with price_action and suggested_fare.

### Step 8: Monitoring UIs

- **Spark Master:** http://localhost:8082 (also Spark worker metrics via `/metrics/prometheus/`)
- **Kafka UI:** http://localhost:8080 (browse topic `tlc.trips`)
- **Grafana:** http://localhost:3000 (username: `admin`, password: `admin`; already configured to scrape Spark and ClickHouse Prometheus)

#### Local dashboard (Grafana)

Access the BR1 Dynamic Pricing dashboard at **http://localhost:3000/d/br1-pricing** with credentials `admin:admin`.

The dashboard displays a fixed time range of **July 2026** (the replay month), which matches the `stream` data in ClickHouse. To view the full historical data including the baseline period, adjust the date range to **2025-07-01 to 2026-07-31** in the dashboard's date picker.

**Panels:**
- **Demand ratio % by landmark:** Time series showing hourly demand ratios per landmark, with a red threshold at 80% surge.
- **Current suggestion per landmark:** Table of latest suggestions with demand ratio, suggested fare, and price action.
- **Landmarks above 80% now:** Stat card showing count of zones currently in surge (demand_ratio_pct > 80).
- **Surge hours by landmark in range:** Table of total surge hours per landmark within the selected time window.

No tunnel is needed—the dashboard queries ClickHouse directly inside the `data-net` Docker network.

---

## 5. Serving to Looker Studio

### Security Warning ⚠️

The public tunnel (`bore.pub`) exposes ClickHouse's MySQL port (9004) to the internet. By default, the `default` user has password `clickhouse` (public in this repo). **Before starting the tunnel:**

1. **Set strong passwords** in `.env` (see below).
2. **Create a read-only `looker` user** (see below).
3. **Stop the tunnel after your demo** to minimize exposure.

### Setup

**Step A: Configure passwords**

```bash
cp .env.example .env
# Edit .env and set strong values:
# CLICKHOUSE_PASSWORD=<random-string-min-32-chars>
# LOOKER_PASSWORD=<another-random-string-min-32-chars>
```

**Step B: Restart ClickHouse with new password**

```bash
set -a; . ./.env; set +a
docker compose up -d clickhouse pyspark
```

Wait for ClickHouse healthcheck to pass:

```bash
docker compose ps | grep clickhouse
```

**Step C: Create read-only `looker` user**

```bash
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" --multiquery -q "CREATE USER IF NOT EXISTS looker IDENTIFIED WITH double_sha1_password BY '$LOOKER_PASSWORD'; GRANT SELECT ON pricing.* TO looker"
```

**Step D: Start the public tunnel**

```bash
docker compose --profile tunnel up -d tunnel
```

Wait 2–3 seconds, then get the public address:

```bash
docker logs tunnel
```

Output will show:
```
listening on... <tunnel-id>.bore.pub:<port>
```

**Step E: Create data sources in Looker Studio**

In Looker Studio (https://looker.studio):

1. **Create → Data source → MySQL connector**
2. Fill in:
   - **Host:** `<tunnel-id>.bore.pub` (from step D)
   - **Port:** `<port>` (from step D, usually 45xxx)
   - **Database:** `pricing`
   - **Username:** `looker`
   - **Password:** `$LOOKER_PASSWORD` (from .env)
3. Click "Authenticate"
4. **Add data**: Select table `v_demand_ratio`
5. Click "Create report"

Create a second data source for `v_latest_suggestion` using the same host/port/credentials.

If the table list fails or the query times out, use **Custom Query** instead:

```sql
SELECT * FROM pricing.v_demand_ratio
```

### Report Layout

**Data freshness:** Set to 1 minute (MySQL connector minimum; any slower misses micro-batch updates).

**Recommended charts:**

1. **Time series:** X-axis = `window_start`, Y-axis = `demand_ratio_pct`, breakdown by `landmark`. Add reference line at 80 (surge threshold).
2. **Heatmap / Pivot table:** Rows = `landmark`, Columns = `hour_of_day`, Values = average `demand_ratio_pct`.
3. **Table:** Columns = landmark, demand_ratio_pct, avg_fare, suggested_fare, price_action. Conditional formatting: red cell background if `demand_ratio_pct > 80`.
4. **Scorecard:** Single value = count of zones with `demand_ratio_pct > 80`.
5. **Filters:** Date range (picker on `date`), landmark (dropdown).

### Stop the Tunnel

```bash
docker compose --profile tunnel stop tunnel
```

### Fallback Tunnels

If `bore.pub` is down (no SLA):

**Pinggy (free, 60-minute sessions):**
```bash
ssh -p 443 -R0:localhost:9004 tcp@a.pinggy.io
```

**ngrok (free plan requires card verification for TCP):**
```bash
ngrok tcp 9004
```

---

## 6. Reset and Replay Again

To purge the stream and start a fresh replay:

```bash
# Delete Kafka topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic tlc.trips

# Clear Spark streaming checkpoint
rm -rf data/br1/checkpoints/demand_ratio

# Remove stream-sourced rows from ClickHouse
docker exec clickhouse clickhouse-client --password "${CLICKHOUSE_PASSWORD:-clickhouse}" -q "ALTER TABLE pricing.demand_ratio_hourly DELETE WHERE source = 'stream'"

# Republish the month and restart streaming (see steps 5 & 6 of runbook)
docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/publish_trips.py --month 2026-07
docker exec -d -e MAX_OFFSETS=50000 pyspark bash -c 'bash /opt/jobs/submit.sh /opt/jobs/stream_demand_ratio.py > /opt/data/br1/stream.log 2>&1'
```

---

## 7. Assumptions and Limitations

- **Capacity is derived (P95), not ground truth.** No public per-zone active vehicle count exists; the baseline months 2025-07 to 2026-06 establish a "practical capacity" based on historical traffic. Hours with zero bookings are excluded, so very quiet zones' capacity may be slightly overestimated.
- **"Real-time" is a replay.** The pipeline re-publishes one completed calendar month to Kafka in order, simulating a live feed. True live data would require a connection to TLC's real-time API (not public at this scale).
- **Looker freshness ≥ 1 minute.** The MySQL connector's minimum data freshness is 1 minute; micro-batches arrive every 10 seconds but charts update at most every minute.
- **Public tunnel (bore.pub) has no SLA.** Free service may go down; fallback to `pinggy` or `ngrok` if needed.
- **bitnamilegacy images receive no security updates.** `bitnami/spark` was removed from Docker Hub in September 2025. The archived bitnamilegacy versions are stable but no longer patched. For production, migrate to a maintained Spark image and keep the Kafka connector matched to its Spark and Scala versions.
- **BR2 (batch demand forecast) and rooms variant are out of scope.** BR1 covers taxi trips only. A multi-modal version (rooms, other services) would require extending LANDMARKS and adding data sources.

---

**Generated:** 2026-09-23 | **Audience:** CO5173 team & lecturer
