# BR2: Dynamic Pricing Insights

**Business Requirement:** Hỗ trợ định giá linh hoạt thời gian thực (Dynamic Pricing Insights). The platform ingests NYC taxi trip fact tables and derives demand ratios (active bookings vs. landmark zone capacity) per hour. A machine-learning-ready pipeline streams these ratios into ClickHouse, where Looker Studio charts the fill trend and automatically suggests price adjustments: "Nhu cầu > 80%: Gợi ý tăng giá 15%" when demand exceeds capacity, and "Giữ giá" (hold price) otherwise.

---

## 1. Data Sources

| Source | Type | Volume | Schema | Public? | Notes |
|--------|------|--------|--------|---------|-------|
| **NYC TLC Yellow Taxi** | Trip records | ~3–4M trips/month | `tpep_pickup_datetime` → `pickup_datetime`, `DOLocationID` → `dropoff_location`, `fare_amount` | Yes | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. Monthly parquet at https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet. **Chosen for BR2.** |
| **NYC TLC FHVHV** (Uber/Lyft) | Trip records | ~20M trips/month | `pickup_datetime`, `base_passenger_fare` | Yes | Scale-up option via `--service fhvhv` flag. |
| **TLC Taxi Zone Lookup** | Zone reference | 263 zones | LocationID, zone name | Yes | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv. Joined by location ID. |
| **TLC Monthly Industry Metrics** | Aggregate metrics | Monthly | Active vehicles (citywide only) | Yes | https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv. Citywide only; not per-zone. Why capacity is derived instead. |
| **Inside Airbnb NYC** | Room availability | Variable | Calendar, listing metadata | Yes | https://insideairbnb.com/get-the-data/. Candidate for "rooms" variant (out of scope for BR2). |

**Why not others:** Chicago taxi (BigQuery historical archive ends 2023), Kaggle hotel-booking-demand (119k rows, too small for statistical validity), Vietnam (no public trip- or booking-level data at scale).

---

## 2. Architecture

**Data flow:**

```
scripts/download_tlc.sh (yellow 2025-07 2026-07)
          ↓
    data/br2/raw/{yellow,fhvhv}/*.parquet (fact: 3–4M rows/month)
          ↓
jobs/br2_dynamic_pricing/job.py (baseline step; skipped if data/br2/lake/dim_capacity exists)
    - Read raw parquet via read_trips() (pricing.py)
    - Write fact_trips → data/br2/lake/fact_trips (partitioned by service, pickup_month)
    - Compute P95 capacity per (location_id, day_type, hour_of_day) over baseline 2025-07..2026-06
    - Write dim_capacity → data/br2/lake/dim_capacity
    - Compute hourly demand ratios (baseline period) → INSERT ClickHouse pricing.demand_ratio_hourly (source='batch')
          ↓
ingestion/producers/produce_br2.py --interval X [--limit N | --loop]
    - Read fact_trips for the replay month (BR2_REPLAY_MONTH, default 2026-07) via pyarrow
    - Sort by pickup_datetime, serialize to JSON, publish to Kafka topic tlc.trips
          ↓
Kafka (topic: tlc.trips) — simulates live feed
          ↓
jobs/br2_dynamic_pricing/job.py (streaming query, started before the producer)
    - Consume tlc.trips via Kafka Structured Streaming (maxOffsetsPerTrigger = MAX_OFFSETS)
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

BR2 owns `data/br2/`, matching the per-BR layout used by BR3 and BR4. See
[`data/br2/README.md`](../../data/br2/README.md) for file layout and source URLs.

### Step 0: Prerequisites

- Docker Desktop running.
- Git repo cloned and working directory is the repo root.
- `.env` created from the template (`cp .env.example .env`). Set `CLICKHOUSE_PASSWORD` now; `LOOKER_PASSWORD` is needed in Section 5.
- Linux only: after the download, run `chmod -R a+rwX data` so the Spark container (uid 1001) can write.

### Step 1: Download NYC TLC Data

```bash
bash scripts/download_tlc.sh yellow 2025-07 2026-07
```

This downloads ~13 monthly yellow taxi parquet files (~1 GB total) to `data/br2/raw/yellow/`. The script also downloads `data/br2/raw/taxi_zone_lookup.csv` if missing. The pipeline does not download data; this step is a manual prerequisite.

### Step 2: Run the Pipeline

```bash
make demo-br BR=2
```

`scripts/start.py` runs these steps in order:

1. **Infra:** builds the `spark-local` image and starts `clickhouse`, `kafka`, `tour-map`, `spark-local`, `grafana`, `prometheus`, `spark-master` and `spark-worker`. Waits for the ClickHouse and Kafka healthchecks.
2. **Topics:** `scripts/topics.py --reset` recreates the Kafka topics, including `tlc.trips`.
3. **Schema:** applies `sql/clickhouse/ddl_br2.sql` (database `pricing`, table `pricing.demand_ratio_hourly`, views `v_demand_ratio` and `v_latest_suggestion`).
4. **Spark job:** submits `jobs/br2_dynamic_pricing/job.py` from the `spark-local` container to the Spark cluster (driver and executor 512m, `spark.cores.max=1`, `spark.sql.shuffle.partitions=4`). On the first run, or whenever `data/br2/lake/dim_capacity` is missing, the job builds the baseline from the raw parquet first, then starts the streaming query. `start.py` waits up to `SPARK_READY_TIMEOUT` (default 180 s) for the streaming query to start.
5. **Producer:** starts `ingestion/producers/produce_br2.py`, which replays the `BR2_REPLAY_MONTH` lake month to `tlc.trips`.

Logs: `logs/demo/br2-spark.log` and `logs/demo/br2-producer.log`. Ctrl+C in that terminal stops the Spark job and the producer.

Every run resets `tlc.trips` but not the Spark checkpoint, so delete `data/br2/checkpoints` before rerunning (see Section 6).

> **First run:** the baseline finishes before the streaming query starts, so it can exceed the 180 s readiness wait. Use `SPARK_READY_TIMEOUT=1800 make demo-br BR=2`.

### Step 3: Check Results in ClickHouse


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

### Step 4: Monitoring UIs

- **Spark Master:** http://localhost:8082 (also Spark worker metrics via `/metrics/prometheus/`)
- **Kafka UI:** http://localhost:8080 (browse topic `tlc.trips`)
- **Grafana:** http://localhost:3000 (username: `admin`, password: `admin`; already configured to scrape Spark and ClickHouse Prometheus)

#### Local dashboard (Grafana)

Access the BR2 Dynamic Pricing dashboard at **http://localhost:3000/d/br2-pricing** with credentials `admin:admin`.

The dashboard displays a fixed time range of **July 2026** (the replay month), which matches the `stream` data in ClickHouse. To view the full historical data including the baseline period, adjust the date range to **2025-07-01 to 2026-07-31** in the dashboard's date picker.

**Panels:**
- **Demand ratio % by landmark:** Time series showing hourly demand ratios per landmark, with a red threshold at 80% surge.
- **Current suggestion per landmark:** Table of latest suggestions with demand ratio, suggested fare, and price action.
- **Landmarks above 80% now:** Stat card showing count of zones currently in surge (demand_ratio_pct > 80).
- **Surge hours by landmark in range:** Table of total surge hours per landmark within the selected time window.

No tunnel is needed—the dashboard queries ClickHouse directly inside the `data-net` Docker network.

### Tuning Knobs

| Setting | Read by | Default | Effect |
|---------|---------|---------|--------|
| `MAX_OFFSETS` | Spark job | `20000` | Max Kafka records per 10 s micro-batch. 20000 ≈ 4 h of yellow taxi data; 50000 ≈ 10 h (faster replay); 10000 ≈ 2 h. |
| `PRODUCER_INTERVAL` (make var) | producer `--interval` | `0` | Seconds between published events. |
| `PRODUCER_LIMIT` (make var) | producer `--events` | unset (full month) | Publish only the first N events. |
| `BR2_SERVICE` | Spark job, producer | `yellow` | TLC service. |
| `BR2_BASELINE_FROM` / `BR2_BASELINE_TO` | Spark job | `2025-07` / `2026-06` | Baseline window for `dim_capacity`. |
| `BR2_REPLAY_MONTH` | producer | `2026-07` | Lake month to replay. |
| `SPARK_READY_TIMEOUT` | `start.py` | `180` | Seconds to wait for the streaming query to start. |

**Caveat:** `scripts/start.py` forwards host env vars matching `BR<N>_*` and `MAX_OFFSETS` into the container (`docker exec -e`), so `BR2_REPLAY_MONTH=2026-08 make demo-br BR=2` works.


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
docker compose up -d clickhouse
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

`make demo-br BR=2` already resets the Kafka topic `tlc.trips`. It does not clear the Spark checkpoint or the stream rows in ClickHouse, so clear those before a fresh replay:

```bash
# Clear the Spark streaming checkpoint
rm -rf data/br2/checkpoints

# Remove stream-sourced rows from ClickHouse
docker exec clickhouse clickhouse-client --password "${CLICKHOUSE_PASSWORD:-clickhouse}" -q "ALTER TABLE pricing.demand_ratio_hourly DELETE WHERE source = 'stream'"

# Rerun: resets tlc.trips, reapplies the DDL, restarts streaming and the replay
make demo-br BR=2
```

To rebuild the baseline as well, also delete `data/br2/lake/dim_capacity` before rerunning. The next run recomputes the baseline and re-inserts the `source='batch'` rows; `demand_ratio_hourly` is a ReplacingMergeTree, so duplicates collapse on merge.

---

## 7. Assumptions and Limitations

- **Capacity is derived (P95), not ground truth.** No public per-zone active vehicle count exists; the baseline months 2025-07 to 2026-06 establish a "practical capacity" based on historical traffic. Hours with zero bookings are excluded, so very quiet zones' capacity may be slightly overestimated.
- **"Real-time" is a replay.** The pipeline re-publishes one completed calendar month to Kafka in order, simulating a live feed. True live data would require a connection to TLC's real-time API (not public at this scale).
- **Looker freshness ≥ 1 minute.** The MySQL connector's minimum data freshness is 1 minute; micro-batches arrive every 10 seconds but charts update at most every minute.
- **Public tunnel (bore.pub) has no SLA.** Free service may go down; fallback to `pinggy` or `ngrok` if needed.
- **bitnamilegacy images receive no security updates.** `bitnami/spark` was removed from Docker Hub in September 2025. The archived bitnamilegacy versions are stable but no longer patched. For production, migrate to a maintained Spark image and keep the Kafka connector matched to its Spark and Scala versions.
- **BR2 (batch demand forecast) and rooms variant are out of scope.** BR2 covers taxi trips only. A multi-modal version (rooms, other services) would require extending LANDMARKS and adding data sources.

---

**Generated:** 2026-09-23 | **Audience:** CO5173 team & lecturer
