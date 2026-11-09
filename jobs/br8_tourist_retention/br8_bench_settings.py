"""BR8 benchmark per-run settings: the shared Spark settings plus BR8's own
optimization choices, in one catalog for the launcher, the jobs and the run
snapshot (docs/plan/br7/plan-benchmark.md, 3.3). Built like BR6's
route_settings.py; the analysis itself is prepare_br8.py's, unchanged.
"""

from spark_settings import EVENT_LOG_SETTING, MB, SPARK_GROUPS, SPARK_SETTINGS, STORAGE_LEVELS, Catalog, Setting

GROUPS = [*SPARK_GROUPS, "Caching", "Attraction filter", "Reading and writing", "Analysis rules", "Monitoring"]

_s = Setting
BR8_SETTINGS = [
    _s("pipeline.cache", "Caching", "Cache reused rows",
       "Keep the cleaned check-ins and the attraction check-ins, which several later steps read, instead of "
       "computing them again from ClickHouse.",
       "bool", "true", "pipeline"),
    _s("pipeline.storage_level", "Caching", "Storage level",
       "Where cached rows are kept.",
       "choice", "MEMORY_AND_DISK", "pipeline", choices=tuple(STORAGE_LEVELS)),
    _s("pipeline.udf", "Attraction filter", "Attraction test",
       "How br8_rules.ATTRACTION_PATTERN is tested on each check-in: native Spark rlike, a vectorized "
       "pandas_udf, or a plain Python UDF calling br8_rules.is_attraction (baseline). Same answer in every case.",
       "choice", "native", "pipeline", choices=("native", "pandas", "python")),
    _s("pipeline.engine", "Reading and writing", "Engine",
       "spark: the main pipeline. clickhouse: the same run as ClickHouse SQL, without Spark (the alternative).",
       "choice", "spark", "pipeline", choices=("spark", "clickhouse")),
    _s("pipeline.reader", "Reading and writing", "Reader",
       "parallel: each Spark task reads its own slice from ClickHouse. driver: one query through the driver "
       "(baseline).",
       "choice", "parallel", "pipeline", choices=("parallel", "driver")),
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
    _s("rules.max_places", "Analysis rules", "Top attractions",
       "Keep the N attractions with the most different visitors (prepare_br8.py --max-places). 0: all.",
       "int", "0", "rule", min=0, max=1_000_000),
]

BUILTIN_PRESETS = {
    "default": {},
    # The naive variant (plan 3.3 L1). The driver reader needs room for the rows (plan O5).
    "baseline": {
        "pipeline.reader": "driver",
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


CATALOG = Catalog(GROUPS, [*SPARK_SETTINGS, *BR8_SETTINGS, EVENT_LOG_SETTING], BUILTIN_PRESETS, checks)
# prepare_br8.py works in UTC; so do the benchmark jobs.
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
