# BR5 Spec: Advanced Settings and Restart Guide

Part of [spec.md](spec.md). Purpose: let us change how the data processing runs, then compare results for the report.

## 1. Two kinds of settings

| Kind | When it takes effect | How we change it |
|---|---|---|
| **Per-run** | On the next **Run** (each run starts its own Spark application, so no restart is needed) | **Advanced Settings** screen |
| **Needs restart** | Only after restarting a service or the stack | The restart guide (section 4) |

## 2. Advanced Settings screen

| ID | Requirement | Priority |
|---|---|---|
| C1 | Lists the per-run settings in section 3, grouped as shown, each with: name, one-line explanation, default, current value | Must |
| C2 | Editing a setting and pressing Run uses the new value; no restart | Must |
| C3 | **Reset to defaults** per setting and for all | Must |
| C4 | Values are checked before a run: wrong type or out-of-range is rejected with a message; settings that cannot fit the cluster (for example executor memory larger than the worker has) get a warning | Should |
| C5 | **Named presets:** save the current settings under a name (for example "baseline", "tuned") and load them later | Should |
| C6 | Every run records a snapshot of all settings used (shown in run history and Grafana) | Must |
| C7 | A "Needs restart" section on the same screen shows the current values read from the running stack (read-only) and links to the restart guide | Could |

## 3. Per-run settings

Defaults are Spark's own unless noted; the plan fixes exact defaults for our small demo cluster.

| Group | Setting | What it affects |
|---|---|---|
| **Resources** | `spark.executor.memory`, `spark.executor.cores`, `spark.cores.max`, `spark.driver.memory` | How much memory and how many cores a run may use |
| **Memory** | `spark.memory.fraction`, `spark.memory.storageFraction`, `spark.memory.offHeap.enabled` / `.size` | Split between working memory and cached data; off-heap memory |
| **Shuffle and partitions** | `spark.sql.shuffle.partitions`, `spark.default.parallelism`, `spark.sql.files.maxPartitionBytes`, `spark.shuffle.compress`, `spark.io.compression.codec` | How data is split and moved between steps |
| **Adaptive Query Execution (AQE)** | `spark.sql.adaptive.enabled`, `.coalescePartitions.enabled`, `.advisoryPartitionSizeInBytes`, `.skewJoin.enabled`, `.skewJoin.skewedPartitionFactor`, `.skewJoin.skewedPartitionThresholdInBytes` | Spark adjusting the plan while running, including handling uneven (skewed) partitions |
| **Joins** | `spark.sql.autoBroadcastJoinThreshold` | Whether small tables are sent to every worker |
| **Serialization** | `spark.serializer` (Java / Kryo) | How data is packed when moved or cached |
| **Caching** | Cache the cleaned data between steps (on/off), storage level | Reuse vs. recompute |
| **Reading and writing** | Number of parallel writers to the data store; ClickHouse per-query limits (`max_threads`, `max_memory_usage`) | Speed of reading from and writing to ClickHouse |

| ID | Requirement | Priority |
|---|---|---|
| C8 | Keep the simple, single-threaded write path as a "baseline" choice next to the parallel one, so the two can be compared | Should |
| C9 | Selectable ClickHouse table layouts (sorting/partitioning), applied on the next import | Could |

## 3b. Analysis rules (team settings, not shown to the business user)

These control **what the recommendation says**, not how fast it is produced. They are on the same screen in their own group, take effect on the next run, and are recorded with each run like any other setting. Defaults are in the main spec (sections 3.3 to 3.5).

| Rule | Default |
|---|---|
| Low-season threshold (share of peak demand) | 60% |
| Promotion triggers: lead-time limits (early-bird / last-minute), shorter-stay %, family share, "price already low" % | 30 days / 14 days / 25% / half of peak / 30% |
| Discount tiers (demand level → discount range) | as in the main spec |
| Minimum valid bookings for a country or room type to be offered | 100 |
| Exact duplicate rows: keep or remove | Keep |

| ID | Requirement | Priority |
|---|---|---|
| C12 | Analysis rules are editable on the Advanced Settings screen, with Reset to defaults and presets like the other settings | Should |

## 4. Restart guide (`docs/guide/br5/config-guide.md`)

Delivered with the implementation. For **every** setting that cannot change live, it states: what it does, where to set it (file and line), a sample value, **which services to restart**, and how to confirm it took effect.

Settings it must cover:

| Setting | Where | Restart |
|---|---|---|
| Spark worker memory and cores (`SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES`) | `docker-compose.yml` | Spark worker |
| Container memory/CPU limits (Spark, ClickHouse, Kafka) | `docker-compose.yml` | The changed container |
| Spark master/worker background memory (`SPARK_DAEMON_MEMORY`) | `docker-compose.yml` | Spark master and worker |
| ClickHouse server-level settings (for example total memory, background threads) | ClickHouse config in `docker-compose.yml` | ClickHouse |
| Metrics scrape settings | `docker-compose.yml` (Prometheus config) | Prometheus |

| ID | Requirement | Priority |
|---|---|---|
| C10 | The guide lists each setting once, with the exact steps and the restart command; written for someone who has not seen the code | Must |
| C11 | A short "how to run a fair comparison" section: change one thing at a time, same data, same criteria, repeat a few times | Should |
