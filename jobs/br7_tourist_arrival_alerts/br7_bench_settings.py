"""BR7 benchmark per-run settings: the shared Spark settings plus BR7's own
optimization choices, in one catalog for the launcher, the jobs and the run
snapshot (docs/plan/br7/plan-benchmark.md, 3.3). Built like br8_bench_settings.py;
the analysis itself is prepare.py's, unchanged.
"""

from spark_settings import EVENT_LOG_SETTING, MB, SPARK_GROUPS, SPARK_SETTINGS, STORAGE_LEVELS, Catalog, Setting

GROUPS = [*SPARK_GROUPS, "Caching", "Quality labels", "Reading and writing", "Monitoring"]

_s = Setting
BR7_SETTINGS = [
    _s("pipeline.cache", "Caching", "Cache the cleaned trips",
       "Keep the deduplicated tourist-zone trips (prepare.py caches them too), which the month counts, the "
       "baseline and the replay all read, instead of reading and cleaning every trip again.",
       "bool", "true", "pipeline"),
    _s("pipeline.storage_level", "Caching", "Storage level",
       "Where cached rows are kept.",
       "choice", "MEMORY_AND_DISK", "pipeline", choices=tuple(STORAGE_LEVELS)),
    _s("pipeline.udf", "Quality labels", "Quality label",
       "How each trip's quality label is computed: native Spark expressions (prepare.py's), a vectorized "
       "pandas_udf, or a plain Python UDF (baseline). Same labels in every case.",
       "choice", "native", "pipeline", choices=("native", "pandas", "python")),
    _s("pipeline.engine", "Reading and writing", "Engine",
       "spark: the main pipeline. clickhouse: the same run as ClickHouse SQL, without Spark (the alternative).",
       "choice", "spark", "pipeline", choices=("spark", "clickhouse")),
    # No driver reader for BR7 (plan R1): 50.7 M trips as Arrow on the driver need ~3 GB
    # outside the JVM heap and would take the 7.4 GB WSL host down.
    _s("pipeline.reader", "Reading and writing", "Reader",
       "parallel: each Spark task reads its own slice from ClickHouse. single: one connection and one "
       "partition, as Spark's JDBC reader without partitioning options (baseline).",
       "choice", "parallel", "pipeline", choices=("parallel", "single")),
    _s("pipeline.read_partitions", "Reading and writing", "Read partitions",
       "Slices read in parallel by the parallel reader.",
       "int", "2", "pipeline", min=1, max=64),
    _s("pipeline.writer", "Reading and writing", "Writer",
       "partitions: each Spark task writes its own rows (write_partitions). pandas: all rows go through the "
       "driver with toPandas (baseline).",
       "choice", "partitions", "pipeline", choices=("partitions", "pandas")),
    _s("pipeline.write_partitions", "Reading and writing", "Write partitions",
       "Spark partitions writing in parallel.",
       "int", "2", "pipeline", min=1, max=64),
    _s("clickhouse.max_threads", "Reading and writing", "ClickHouse max_threads",
       "Threads ClickHouse may use per query. 0: the server's default (its core count).",
       "int", "0", "clickhouse", min=0, max=64),
    _s("clickhouse.max_memory_usage", "Reading and writing", "ClickHouse max_memory_usage",
       "Memory limit per ClickHouse query. 0: the server's default.",
       "size", "0", "clickhouse", min=0, max=8192 * MB),
    _s("clickhouse.limit_threads_by_memory", "Reading and writing", "ClickHouse: threads need free memory",
       "On (the server's default): ClickHouse runs a query on fewer threads unless each thread has 1 GB of free "
       "memory, so in a 1 GB container every query runs on one thread. Off: max_threads_min_free_memory_per_thread "
       "= 0.",
       "bool", "true", "clickhouse"),
]

BUILTIN_PRESETS = {
    "default": {},
    # The naive variant (plan 3.3 L1). Single reader instead of the driver reader (plan R1);
    # the driver keeps 2 GB as the other BRs' baselines (plan O5).
    "baseline": {
        "pipeline.reader": "single",
        "pipeline.udf": "python",
        "pipeline.cache": "false",
        "pipeline.writer": "pandas",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.enabled": "false",
        "spark.driver.memory": "2g",
    },
}


def checks(typed, errors, warnings):
    if typed["pipeline.storage_level"] == "OFF_HEAP" and not typed["spark.memory.offHeap.enabled"]:
        errors["pipeline.storage_level"] = "OFF_HEAP needs off-heap memory turned on (Memory group)"


CATALOG = Catalog(GROUPS, [*SPARK_SETTINGS, *BR7_SETTINGS, EVENT_LOG_SETTING], BUILTIN_PRESETS, checks)
# prepare.py works in a UTC session and converts New York times explicitly; so do the benchmark jobs.
FIXED_CONF = {"spark.sql.session.timeZone": "UTC"}


def spark_conf(values):
    return {**CATALOG.spark_conf(values), **FIXED_CONF}


def clickhouse_query_settings(values):
    """ClickHouse settings sent with every query of a run (0 = server default)."""
    opts = CATALOG.options(values)["clickhouse"]
    limit_threads = opts.pop("limit_threads_by_memory")
    settings = {k: v for k, v in opts.items() if v}
    if not limit_threads:
        settings["max_threads_min_free_memory_per_thread"] = 0
    return settings
