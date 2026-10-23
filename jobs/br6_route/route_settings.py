"""BR6 per-run settings: the shared Spark settings plus BR6's own. This stage
(docs/plan/br6/plan-data-import.md) only needs the import's reading and
writing; the config plan adds the Route Finder settings and analysis rules."""

from spark_settings import EVENT_LOG_SETTING, SPARK_GROUPS, SPARK_SETTINGS, Catalog, Setting

GROUPS = [*SPARK_GROUPS, "Reading and writing", "Monitoring"]

BR6_SETTINGS = [
    Setting("pipeline.writer", "Reading and writing", "Import writer",
            "partitions: each Spark task writes its own rows (parallel). pandas: all rows go through the driver "
            "(baseline).",
            "choice", "partitions", "pipeline", choices=("partitions", "pandas")),
    Setting("pipeline.write_partitions", "Reading and writing", "Import write partitions",
            "Spark partitions writing in parallel during an import.",
            "int", "2", "pipeline", min=1, max=64),
]

BUILTIN_PRESETS = {
    "default": {},
    # The naive variant the optimizations are compared with.
    "baseline": {
        "pipeline.writer": "pandas",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.enabled": "false",
    },
}

CATALOG = Catalog(GROUPS, [*SPARK_SETTINGS, *BR6_SETTINGS, EVENT_LOG_SETTING], BUILTIN_PRESETS)
# Every BR6 job reads and writes timestamps as UTC, whatever the container's zone.
FIXED_CONF = {"spark.sql.session.timeZone": "UTC"}


def spark_conf(values):
    return {**CATALOG.spark_conf(values), **FIXED_CONF}
