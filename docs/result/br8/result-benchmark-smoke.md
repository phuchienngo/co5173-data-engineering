# BR8 Result: benchmark tooling smoke test

First end-to-end runs of the BR8 benchmark tooling (docs/plan/br7/plan-benchmark.md, section 12), 2026-11-07,
on the dev laptop (WSL 7.6 GB RAM), Spark worker 2 cores / 2 GB, executor 512 MB, ClickHouse 1 CPU / 1 GB.
Single manual runs (`make br8-run`), not benchmark blocks: no warm-up, no repeats.

## Import (`make br8-import`)

| Figure | Value |
|---|---|
| Destination check-ins (c_o exploded) | 2,764,448 |
| Places (metadata_all) | 687,173 |
| Regions | 1,173 |
| Run time | 62.0 s (read 14.8 s, write 41.8 s, verify 0.1 s, swap 0.0 s) |
| Checks | row counts equal Spark's; `visit_time` range unchanged after the insert (no time zone shift); place ids unique |

First attempt failed: the import cached both tables and used a round-robin `repartition`, which sorts each
partition; the 512 MB executor ran out of memory. Fixed by not caching and repartitioning on the slice key.

## Runs

| Input | Settings | Engine | Run time | Steps (read / clean / match / aggregate / write) | Result |
|---|---|---|---|---|---|
| I2 Tokyo | `bench-clickhouse` | ClickHouse SQL | 2.6 s | 0.0 / 0.1 / 0.5 / 0.5 / 0.3 s | 5,416 visits, 533 places · ✓ expected fingerprint |
| I2 Tokyo | `bench-default` | Spark | 70.2 s | 14.8 / 11.9 / 19.7 / 6.3 / 11.0 s | same fingerprint ✓ |
| I1 all destinations | `bench-default` | Spark | 120.1 s | 24.6 / 26.7 / 39.2 / 8.2 / 12.7 s | 107,912 visits, 10,639 places · ✓ |
| I1 all destinations | `bench-baseline` | Spark | 345.7 s | 14.8 / 15.6 / 63.5 / 51.9 / 193.1 s | same fingerprint ✓ |

- Spark and ClickHouse give the same rows, which are those of `prepare_br8.py` (107,912 visits, 10,639 places on
  all data; checked row by row against an independent pandas version before these runs).
- The baseline (driver reader, Python UDF, no cache, no AQE, no broadcast, pandas writer) is **2.9× slower** than
  the optimized default on all data; Spark CPU 219.3 s against 94.3 s, shuffle 1,600 MB against 364 MB. Its write
  step (193 s, the pandas writer recomputing the uncached plan) and match step (Python UDF) dominate.
- The 2 GB driver held the driver reader's 2.76 M rows: no out-of-memory (plan O5).
- Every Grafana panel query runs through the ClickHouse data source on the stored runs.

Next: `make br8-bench BLOCK=S2`, then S1, S3, V, H1–H3, each followed by `make br8-bench-check`.
