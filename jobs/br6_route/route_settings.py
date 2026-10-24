"""BR6 per-run settings: the shared Spark settings plus BR6's own (the Route
Finder's optimization choices and analysis rules, docs/plan/br6/plan-pipeline.md
section 5) in one catalog for the settings screen, the launcher, the jobs and
the run snapshot."""

from spark_settings import (
    EVENT_LOG_SETTING, MB, SPARK_GROUPS, SPARK_SETTINGS, STORAGE_LEVELS, Catalog, Setting,
)

GROUPS = [*SPARK_GROUPS, "Caching", "Finding places near the route", "Place statistics", "Reading and writing",
          "Analysis rules", "Monitoring"]
RULES_GROUP = "Analysis rules"

_s = Setting
BR6_SETTINGS = [
    # Caching
    _s("pipeline.cache", "Caching", "Cache the check-ins read",
       "Keep the check-ins read (place statistics every run) for the clean and visitor steps instead of "
       "reading them again. No effect when the statistics come from the import.",
       "bool", "true", "pipeline"),
    _s("pipeline.storage_level", "Caching", "Storage level",
       "Where cached rows are kept.",
       "choice", "MEMORY_AND_DISK", "pipeline", choices=tuple(STORAGE_LEVELS)),
    # Finding places near the route
    _s("pipeline.matching", "Finding places near the route", "Method",
       "grid: sort places into squares and compare only the squares near the route. all: compare every "
       "place of the city with the route (baseline). Both give the same list.",
       "choice", "grid", "pipeline", choices=("grid", "all")),
    _s("pipeline.cell_km", "Finding places near the route", "Square size (km)",
       "Side of a grid square. Too small: many squares to check; too large: few places skipped.",
       "float", "1.0", "pipeline", min=0.1, max=20),
    _s("pipeline.distance", "Finding places near the route", "Distance function",
       "How each place-to-route distance is computed: native Spark expressions, a vectorized pandas_udf, or a "
       "plain Python UDF called once per place and leg (baseline). Same numbers in every case.",
       "choice", "native", "pipeline", choices=("native", "pandas_udf", "python_udf")),
    # Place statistics
    _s("pipeline.place_stats", "Place statistics", "Place statistics",
       "import: read the check-ins and visitors per place counted once at import (br6.place_stats). run: "
       "count them from every check-in of the city at each run (baseline).",
       "choice", "import", "pipeline", choices=("import", "run")),
    # Reading and writing
    _s("pipeline.engine", "Reading and writing", "Engine",
       "spark: the main pipeline. clickhouse: the same run as ClickHouse SQL, without Spark (the alternative "
       "solution).",
       "choice", "spark", "pipeline", choices=("spark", "clickhouse")),
    _s("pipeline.reader", "Reading and writing", "Reader",
       "parallel: each Spark task reads its own slice of the check-ins from ClickHouse. driver: one query "
       "through the driver (baseline). Used when the place statistics are counted every run.",
       "choice", "parallel", "pipeline", choices=("parallel", "driver")),
    _s("pipeline.read_partitions", "Reading and writing", "Read partitions",
       "Slices read in parallel by the parallel reader.",
       "int", "2", "pipeline", min=1, max=64),
    _s("pipeline.writer", "Reading and writing", "Import writer",
       "partitions: each Spark task writes its own rows (parallel). pandas: all rows go through the driver "
       "(baseline).",
       "choice", "partitions", "pipeline", choices=("partitions", "pandas")),
    _s("pipeline.write_partitions", "Reading and writing", "Import write partitions",
       "Spark partitions writing in parallel during an import.",
       "int", "2", "pipeline", min=1, max=64),
    _s("clickhouse.max_threads", "Reading and writing", "ClickHouse max_threads",
       "Threads ClickHouse may use per query. 0: the server's default (its core count).",
       "int", "0", "clickhouse", min=0, max=64),
    _s("clickhouse.max_memory_usage", "Reading and writing", "ClickHouse max_memory_usage",
       "Memory limit per ClickHouse query. 0: the server's default.",
       "size", "0", "clickhouse", min=0, max=8192 * MB),
    # Analysis rules (team settings)
    _s("rules.max_detour_km", RULES_GROUP, "Maximum detour (km)",
       "A place is near the route when its straight-line distance to the route is at most this.",
       "float", "1.0", "rule", min=0.1, max=10),
    _s("rules.top_n", RULES_GROUP, "Number of suggestions",
       "How many places are suggested.",
       "int", "10", "rule", min=1, max=50),
    _s("rules.weight_popularity", RULES_GROUP, "Weight: popularity (%)",
       "Chosen visitor. Popularity, fit and closeness must add up to 100%.",
       "percent", "50", "rule", min=0, max=100),
    _s("rules.weight_fit", RULES_GROUP, "Weight: fit (%)",
       "Chosen visitor: how well the place type matches the visitor's own check-ins.",
       "percent", "30", "rule", min=0, max=100),
    _s("rules.weight_closeness", RULES_GROUP, "Weight: closeness (%)",
       "Chosen visitor: 1 at the route, 0 at the maximum detour.",
       "percent", "20", "rule", min=0, max=100),
    _s("rules.general_weight_popularity", RULES_GROUP, "General visitor weight: popularity (%)",
       "General visitor (no fit part). Popularity and closeness must add up to 100%.",
       "percent", "70", "rule", min=0, max=100),
    _s("rules.general_weight_closeness", RULES_GROUP, "General visitor weight: closeness (%)",
       "General visitor: weight of closeness.",
       "percent", "30", "rule", min=0, max=100),
    _s("rules.min_place_visitors", RULES_GROUP, "Minimum different visitors of a place",
       "A place is suggested only with at least this many different visitors (in the original data).",
       "int", "3", "rule", min=1, max=1000),
    _s("rules.min_visitor_checkins", RULES_GROUP, "Minimum check-ins of a visitor",
       "A visitor is offered only with at least this many used check-ins in the city (offered place types).",
       "int", "10", "rule", min=1, max=1000),
    _s("rules.max_centre_km", RULES_GROUP, "Far from the city (km)",
       "Check-ins at places farther than this from the city centre are excluded.",
       "float", "100", "rule", min=1, max=1000),
]

BUILTIN_PRESETS = {
    "default": {},
    # The naive variant the optimizations are compared with.
    "baseline": {
        "pipeline.matching": "all",
        "pipeline.distance": "python_udf",
        "pipeline.place_stats": "run",
        "pipeline.reader": "driver",
        "pipeline.cache": "false",
        "pipeline.writer": "pandas",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.enabled": "false",
    },
}


def checks(typed, errors, warnings):
    personal = sum(typed[f"rules.weight_{p}"] for p in ("popularity", "fit", "closeness"))
    if round(personal, 6) != 100:
        errors["rules.weight_popularity"] = f"popularity, fit and closeness add up to {personal:g}%, not 100%"
    general = typed["rules.general_weight_popularity"] + typed["rules.general_weight_closeness"]
    if round(general, 6) != 100:
        errors["rules.general_weight_popularity"] = f"the general visitor's weights add up to {general:g}%, not 100%"
    if typed["pipeline.storage_level"] == "OFF_HEAP" and not typed["spark.memory.offHeap.enabled"]:
        errors["pipeline.storage_level"] = "OFF_HEAP needs off-heap memory turned on (Memory group)"
    if typed["pipeline.cell_km"] * 20 < typed["rules.max_detour_km"]:
        warnings["pipeline.cell_km"] = "much smaller than the maximum detour: the grid has many squares to check"


CATALOG = Catalog(GROUPS, [*SPARK_SETTINGS, *BR6_SETTINGS, EVENT_LOG_SETTING], BUILTIN_PRESETS, checks)
# Every BR6 job reads and writes timestamps as UTC, whatever the container's zone.
FIXED_CONF = {"spark.sql.session.timeZone": "UTC"}


def spark_conf(values):
    return {**CATALOG.spark_conf(values), **FIXED_CONF}


def clickhouse_query_settings(values):
    """ClickHouse settings sent with every query of a run (0 = server default)."""
    return {k: v for k, v in CATALOG.options(values)["clickhouse"].items() if v}
