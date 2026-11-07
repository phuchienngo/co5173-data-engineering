# BR6 Spec: Advanced Settings and Restart Guide

Part of [spec.md](spec.md). Purpose: let us change how the data processing runs, then compare results for the report.

## 1. Two kinds of settings

| Kind | When it takes effect | How we change it |
|---|---|---|
| **Per-run** | On the next **Run** or **Import** (each starts its own Spark application, so no restart is needed) | **Advanced Settings** screen |
| **Needs restart** | Only after restarting a service or the stack | The restart guide (section 4) |

## 2. Advanced Settings screen

Reached from the menu *Advanced Settings*, then the *Route Finder* panel (main spec, section 2). Its layout follows BR5's Advanced Settings screen, and its settings and presets are separate from BR5's.

| ID | Requirement | Priority |
|---|---|---|
| C1 | Lists the per-run settings in sections 3 and 3b, grouped as shown, each with: name, one-line explanation, default, current value | Must |
| C2 | Editing a setting and pressing Run uses the new value; no restart | Must |
| C3 | **Reset to defaults** per setting and for all | Must |
| C4 | Values are checked before a run: wrong type or out-of-range is rejected with a message. A combination that can never start on the cluster (for example executor memory larger than the worker has) is an error that blocks Run and Import, because Spark would wait for resources forever. Other doubtful values get a warning | Should |
| C5 | **Named presets:** save the current settings under a name (for example "baseline", "tuned") and load them later | Should |
| C6 | Every run records a snapshot of all settings used (shown in run history and Grafana) | Must |
| C7 | A "Needs restart" section shows the current values read from the running stack (read-only) and links to the restart guide | Could |

## 3. Per-run settings

**Spark settings:** the same groups as BR5 (resources, memory, shuffle and partitions, adaptive query execution, joins, serialization, caching, reading and writing; see section 3 of the [BR5 config spec](../br5/spec-pipeline-config.md)). Defaults are Spark's own unless the plan fixes others for our small demo cluster.

**BR6-specific settings:** these are the optimization choices this BR is about, each with a baseline and an optimized option so the two can be compared.

| Group | Setting | What it affects |
|---|---|---|
| **Finding places near the route** | **Method:** compare every place of the city with the route (baseline), or first sort places into a grid of small squares and compare only the squares the route passes through (optimized) | How much work the route matching needs; the main speed-up for large cities |
| | **Square size** (for the optimized method) | Too small: many squares to check; too large: little saved |
| | **Distance function:** native Spark expressions (optimized), `pandas_udf`, or a plain Python UDF (baseline) | How costly each distance is. With native expressions the grid saves little at 49k places; with the Python UDF its saving shows |
| **Place statistics** | Compute popularity and visitor habits at **every run** (baseline), or **once at import** and reuse them (optimized) | Run time vs. import time and freshness after a re-grouping of categories |
| **Reading and writing** | Same as BR5: engine (Spark, or ClickHouse SQL as the alternative solution), parallel or single-path reading and writing, number of readers and writers, ClickHouse per-query limits | Where the work is done; speed of reading and writing |

| ID | Requirement | Priority |
|---|---|---|
| C8 | Keep the simple, single-path write as a "baseline" choice next to the parallel one | Should |
| C9 | Both place-matching methods, and all three distance functions, give **exactly the same list** for the same choices; only speed differs | Must |
| C10 | The ClickHouse SQL engine gives the same list as the Spark engine, so the two can be compared fairly | Could |
| C11 | Selectable ClickHouse table layouts (sorting/partitioning), applied on the next import | Could |

## 3b. Analysis rules (team settings, not shown to the visitor)

These control **what the suggestions say**, not how fast they are produced. They are on the same screen in their own group, take effect on the next run, and are recorded with each run like any other setting. Defaults are in the main spec (3.3 to 3.5).

| Rule | Default |
|---|---|
| Maximum detour from the route | 1 km |
| Number of suggestions shown | 10 |
| Score weights: popularity / fit / closeness | 50% / 30% / 20% |
| Score weights for General visitor: popularity / closeness | 70% / 30% |
| Minimum different visitors for a place to be suggested | 3 |
| Minimum check-ins for a visitor to be offered (offered place types only) | 10 |
| Distance from the city centre beyond which a check-in is excluded | 100 km |
| Category → place type table | the file in the repository (see import spec D13) |

| ID | Requirement | Priority |
|---|---|---|
| C12 | Analysis rules are editable on the Advanced Settings screen, with Reset to defaults and presets like the other settings | Should |

## 4. Restart guide (`docs/guide/br6/config-guide.md`)

Delivered with the implementation. For **every** setting that cannot change live, it states: what it does, where to set it (file and line), a sample value, **which services to restart**, and how to confirm it took effect. The stack and the app are shared with BR5 (and the other BRs), so the guide says which of them a restart affects, including that restarting the app closes BR5's pages too.

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
| C13 | The guide lists each setting once, with the exact steps and the restart command; written for someone who has not seen the code | Must |
| C14 | A short "how to run a fair comparison" section: change one thing at a time, same city and route, same criteria, repeat a few times | Should |
