# BR1: Tour route data

BR1 keeps its files under `data/br1/`.

- `landmarks.csv` (committed): tourist landmarks from OpenStreetMap, 1,954 rows across hcmc, hanoi and danang. Columns: `osm_id,name,category,city,lat,lon,ticket_vnd`. `osm_id` is the OSM element id (`n…` node, `w…` way, `r…` relation). `ticket_vnd` is an estimate from the OSM `fee`/`charge` tags or a per-category default.
- Refresh: `python3 scripts/fetch_osm_landmarks.py` (queries Overpass, always fetches all three cities, overwrites `landmarks.csv`). Data is © OpenStreetMap contributors, ODbL.
- `lake/` (gitignored): Spark output in Parquet. `dist_matrix/mode=optimized/city=…` holds pairwise distances; `tour_requests/mode=naive|optimized/` and `fact_tour/mode=naive|optimized/` hold the benchmark requests and solved tours.
- `lake/checkpoints/tour_requests/` (gitignored): Structured Streaming progress for the request stream. The job deletes it at start.

Run the whole BR1 pipeline with Docker Desktop running and `.env` set:

```bash
make demo-br BR=1
```

`scripts/start.py` starts the stack, creates the `tour` tables, runs `jobs/br1_tour_route/job.py`, and starts the producer `ingestion/producers/produce_br1.py`. Knobs such as `BR1_BENCH_REQUESTS` and `BR1_SEED` are listed in `docs/br1/tour-route.md`, Section 9.
