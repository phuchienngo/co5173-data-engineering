# BR1 — Dynamic pricing data

BR1 keeps its files under `data/br1/`, alongside `data/br3/` and `data/br4/`.

- `raw/yellow/yellow_tripdata_YYYY-MM.parquet`: downloaded yellow taxi trips.
- `raw/fhvhv/fhvhv_tripdata_YYYY-MM.parquet`: optional high-volume for-hire trips.
- `raw/taxi_zone_lookup.csv`: NYC TLC zone reference.
- `lake/fact_trips/`: normalized trips, partitioned by service and pickup month.
- `lake/dim_capacity/`: baseline capacity derived from hourly bookings.
- `checkpoints/demand_ratio/`: Spark streaming progress.

Downloaded files, generated lake data, checkpoints and local logs are ignored by Git.

From the repository root, download BR1 data:

```bash
bash scripts/download_tlc.sh yellow 2025-07 2026-07
```

To download and run the complete BR1 pipeline with Docker Desktop running:

```bash
bash scripts/run_br1.sh yellow 2025-07 2026-06 2026-07
```

The arguments are service, baseline start month, baseline end month and replay month.
Existing downloaded files are reused. The run script creates the pricing schema,
runs the batch baseline, publishes the replay month to Kafka, and runs the pricing
stream in the foreground. It preserves existing Kafka records and checkpoints;
use the reset instructions in `docs/br1-dynamic-pricing.md` for a fresh replay.

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Trip downloads use `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`;
zone lookup uses `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`.

Ingestion code: `scripts/download_tlc.sh` fetches the source files;
`jobs/batch_fact_capacity.py` normalizes them;
`jobs/publish_trips.py` reads the replay month from the lake and publishes JSON
records to Kafka topic `tlc.trips` in pickup-time order. This feed replays monthly
historical data. `jobs/stream_demand_ratio.py` consumes it and writes pricing
suggestions to ClickHouse.
