# BR2 — Dynamic pricing data

BR2 keeps its files under `data/br2/`, alongside `data/br3/` and `data/br4/`.

- `raw/yellow/yellow_tripdata_YYYY-MM.parquet`: downloaded yellow taxi trips.
- `raw/fhvhv/fhvhv_tripdata_YYYY-MM.parquet`: optional high-volume for-hire trips.
- `raw/taxi_zone_lookup.csv`: NYC TLC zone reference.
- `lake/fact_trips/`: normalized trips, partitioned by service and pickup month.
- `lake/dim_capacity/`: baseline capacity derived from hourly bookings.
- `checkpoints/demand_ratio/`: Spark streaming progress.

Downloaded files, generated lake data, checkpoints and local logs are ignored by Git.

From the repository root, download BR2 data:

```bash
bash scripts/download_tlc.sh yellow 2025-07 2026-07
```

To download and run the complete BR2 pipeline with Docker Desktop running:

```bash
make demo-br BR=2
```

`make demo-br` applies `sql/clickhouse/ddl_br2.sql`, runs the Spark job
`jobs/br2_dynamic_pricing/job.py` (baseline once, then the pricing stream), and starts
the replay producer `ingestion/producers/produce_br2.py`. The baseline is skipped when
`data/br2/lake/dim_capacity` already exists; delete that folder to rebuild it.
Service and months default to `BR2_SERVICE=yellow`, `BR2_BASELINE_FROM=2025-07`,
`BR2_BASELINE_TO=2026-06` and `BR2_REPLAY_MONTH=2026-07`.

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Trip downloads use `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`;
zone lookup uses `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`.

Ingestion code: `scripts/download_tlc.sh` fetches the source files;
`jobs/br2_dynamic_pricing/job.py` normalizes them into the lake;
`ingestion/producers/produce_br2.py` reads the replay month from the lake and publishes JSON
records to Kafka topic `tlc.trips` in pickup-time order. This feed replays monthly
historical data. The same job consumes it and writes pricing suggestions to ClickHouse.
