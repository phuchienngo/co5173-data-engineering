"""Per-run settings of a batch BR: the Spark settings every BR shares, plus the
BR's own settings, in one catalog for the settings screen, the launcher, the
jobs and the run snapshot.

Built from BR5's run_settings.py (jobs/br5_promotion), which keeps its own
copy. Values are kept as strings (that is how <db>.settings stores them); ""
means "unset: let Spark decide" for the settings that allow it. Pure Python,
so it can be unit-tested.
"""

import re
from dataclasses import dataclass, field

SERIALIZERS = {
    "java": "org.apache.spark.serializer.JavaSerializer",
    "kryo": "org.apache.spark.serializer.KryoSerializer",
}
STORAGE_LEVELS = ["MEMORY_ONLY", "MEMORY_AND_DISK", "MEMORY_AND_DISK_DESER", "DISK_ONLY", "OFF_HEAP"]
MB = 1024 * 1024
# Read by the spark-history service (docker-compose.yml); logs/ on the host.
EVENT_LOG_DIR = "/opt/logs/spark-events"


@dataclass(frozen=True)
class Setting:
    key: str
    group: str
    label: str
    help: str
    type: str  # size | int | float | bool | choice | percent | text
    default: str
    kind: str  # spark | submit | pipeline | clickhouse | rule
    choices: tuple = ()
    min: float = None
    max: float = None
    optional: bool = False  # "" (unset) allowed
    allow_off: bool = False  # "-1" allowed (broadcast threshold)
    extra: dict = field(default_factory=dict)


def _s(*args, **kwargs):
    return Setting(*args, **kwargs)


SPARK_GROUPS = [
    "Resources",
    "Memory",
    "Shuffle and partitions",
    "Adaptive Query Execution (AQE)",
    "Joins",
    "Serialization",
]

SPARK_SETTINGS = [
    # Resources
    _s("spark.driver.memory", "Resources", "Driver memory",
       "Memory of the driver process, which runs in the app container (2 GB in total).",
       "size", "512m", "submit", min=256 * MB, max=4096 * MB),
    _s("spark.executor.memory", "Resources", "Executor memory",
       "Memory of each executor on the worker. Spark adds an overhead of max(384 MB, 10%) on top.",
       "size", "512m", "spark", min=256 * MB, max=16384 * MB),
    _s("spark.executor.cores", "Resources", "Cores per executor",
       "Cores of each executor. Unset: one executor takes all the worker's cores.",
       "int", "", "spark", min=1, max=64, optional=True),
    _s("spark.cores.max", "Resources", "Total cores",
       "The most cores this run may use across the cluster.",
       "int", "2", "spark", min=1, max=64),
    # Memory
    _s("spark.memory.fraction", "Memory", "Memory fraction",
       "Share of executor memory used for work and cache; the rest is for user data and Spark internals.",
       "float", "0.6", "spark", min=0.1, max=0.9),
    _s("spark.memory.storageFraction", "Memory", "Storage fraction",
       "Share of that memory protected for cached data.",
       "float", "0.5", "spark", min=0.1, max=0.9),
    _s("spark.memory.offHeap.enabled", "Memory", "Off-heap memory",
       "Let Spark keep data outside the Java heap (needs a size below).",
       "bool", "false", "spark"),
    _s("spark.memory.offHeap.size", "Memory", "Off-heap size",
       "Off-heap memory per executor, added to its memory use.",
       "size", "0", "spark", min=0, max=8192 * MB),
    # Shuffle and partitions
    _s("spark.sql.shuffle.partitions", "Shuffle and partitions", "Shuffle partitions",
       "Partitions after a shuffle (joins, aggregations). AQE merges small ones when enabled.",
       "int", "200", "spark", min=1, max=5000),
    _s("spark.default.parallelism", "Shuffle and partitions", "Default parallelism",
       "Partitions for RDD operations only; DataFrame steps use shuffle partitions. Unset: total cores.",
       "int", "", "spark", min=1, max=5000, optional=True),
    _s("spark.sql.files.maxPartitionBytes", "Shuffle and partitions", "Max bytes per file partition",
       "Size of each partition when reading files. Affects the import's CSV read only.",
       "size", "128m", "spark", min=1 * MB, max=2048 * MB),
    _s("spark.shuffle.compress", "Shuffle and partitions", "Compress shuffle data",
       "Compress data moved between steps.",
       "bool", "true", "spark"),
    _s("spark.io.compression.codec", "Shuffle and partitions", "Compression codec",
       "Codec for shuffle data, spills and cached blocks.",
       "choice", "lz4", "spark", choices=("lz4", "snappy", "zstd", "lzf")),
    # AQE
    _s("spark.sql.adaptive.enabled", "Adaptive Query Execution (AQE)", "AQE",
       "Re-plan while running, using the real sizes of finished steps.",
       "bool", "true", "spark"),
    _s("spark.sql.adaptive.coalescePartitions.enabled", "Adaptive Query Execution (AQE)", "Merge small partitions",
       "Merge small shuffle partitions into bigger ones (needs AQE).",
       "bool", "true", "spark"),
    _s("spark.sql.adaptive.advisoryPartitionSizeInBytes", "Adaptive Query Execution (AQE)", "Target partition size",
       "Size AQE aims for when merging or splitting partitions.",
       "size", "64m", "spark", min=1 * MB, max=2048 * MB),
    _s("spark.sql.adaptive.skewJoin.enabled", "Adaptive Query Execution (AQE)", "Handle skewed joins",
       "Split oversized partitions of a sort-merge join.",
       "bool", "true", "spark"),
    _s("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "Adaptive Query Execution (AQE)", "Skew factor",
       "A partition is skewed if it is this many times the median size (and above the threshold below).",
       "float", "5", "spark", min=1, max=100),
    _s("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "Adaptive Query Execution (AQE)",
       "Skew threshold",
       "Minimum size of a skewed partition. Lower it to see skew handling on small partitions.",
       "size", "256m", "spark", min=0, max=4096 * MB),
    # Joins
    _s("spark.sql.autoBroadcastJoinThreshold", "Joins", "Broadcast threshold",
       "Tables smaller than this are sent to every executor instead of shuffled. -1 turns broadcast off.",
       "size", "10m", "spark", min=0, max=1024 * MB, allow_off=True),
    # Serialization
    _s("spark.serializer", "Serialization", "Serializer",
       "How Spark packs objects. DataFrames mostly use their own format, so expect a small effect.",
       "choice", "java", "spark", choices=tuple(SERIALIZERS)),
]

EVENT_LOG_SETTING = _s(
    "spark.eventLog.enabled", "Monitoring", "Spark event log",
    "Write the run's Spark event log, so its Spark page stays on the history server "
    "(http://localhost:18080) after the run. Costs a little disk and time; turn it off to measure that.",
    "bool", "true", "spark")

SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*$", re.IGNORECASE)
SIZE_UNITS = {"": 1, "k": 1024, "m": MB, "g": 1024 * MB, "t": 1024 * 1024 * MB}


def size_bytes(text):
    match = SIZE_RE.match(str(text))
    if not match:
        raise ValueError(f"'{text}' is not a size (examples: 512m, 2g, 0)")
    return int(float(match.group(1)) * SIZE_UNITS[match.group(2).lower()])


def show(s, value):
    if s.type == "size":
        if value >= 1024 * MB and value % (1024 * MB) == 0:
            return f"{value // (1024 * MB)}g"
        return f"{value // MB}m" if value >= MB else str(int(value))
    if s.type == "percent":
        return f"{value:g}%"
    return f"{value:g}" if isinstance(value, float) else str(value)


def overhead_bytes(executor_bytes):
    return max(384 * MB, executor_bytes // 10)


class Catalog:
    """settings: the shared Spark settings plus a BR's own ones.
    presets: {name: {key: value}} built-in presets ("default" = {}).
    checks: optional function(typed, errors, warnings) for the BR's own rules."""

    def __init__(self, groups, settings, presets, checks=None):
        self.groups = groups
        self.settings = settings
        self.by_key = {s.key: s for s in settings}
        self.presets = presets
        self._checks = checks

    def defaults(self):
        return {s.key: s.default for s in self.settings}

    def resolve(self, stored=None, preset=None):
        """Defaults, then a built-in preset's overrides, then stored values.
        Unknown keys (from an older version) are dropped."""
        values = self.defaults()
        values.update(self.presets.get(preset, {}))
        values.update({k: str(v) for k, v in (stored or {}).items() if k in self.by_key})
        return values

    def parse(self, key, text):
        """Typed value of one setting, or ValueError with a message for the
        user. None means unset."""
        s = self.by_key[key]
        text = "" if text is None else str(text).strip()
        if text == "":
            if s.optional:
                return None
            if s.type != "text":
                raise ValueError("a value is required")
        if s.allow_off and text == "-1":
            return -1
        if s.type == "bool":
            if text.lower() not in ("true", "false"):
                raise ValueError("must be true or false")
            return text.lower() == "true"
        if s.type == "choice":
            if text not in s.choices:
                raise ValueError(f"must be one of: {', '.join(s.choices)}")
            return text
        if s.type == "text":
            if not text:
                raise ValueError("must not be empty")
            return text
        try:
            if s.type == "size":
                value = size_bytes(text)
            elif s.type == "int":
                value = int(text)
            else:  # float, percent
                value = float(text)
        except ValueError as exc:
            if s.type == "size":
                raise
            raise ValueError(f"'{text}' is not a {'whole ' if s.type == 'int' else ''}number") from exc
        if s.min is not None and value < s.min or s.max is not None and value > s.max:
            raise ValueError(f"must be between {show(s, s.min)} and {show(s, s.max)}")
        return value

    def validate(self, values, cluster=None):
        """Returns (errors, warnings): {key: message}. Errors block a run.
        cluster: {"worker_memory_mb", "worker_cores"} read from the Spark
        master, or None when it can't be reached (cluster checks are skipped)."""
        errors, warnings, typed = {}, {}, {}
        for key, s in self.by_key.items():
            try:
                typed[key] = self.parse(key, values.get(key, s.default))
            except ValueError as exc:
                errors[key] = str(exc)
        if errors:
            return errors, warnings

        if typed["spark.memory.offHeap.enabled"] and not typed["spark.memory.offHeap.size"]:
            errors["spark.memory.offHeap.size"] = "must be above 0 when off-heap memory is on"
        executor_cores = typed["spark.executor.cores"]
        if executor_cores and executor_cores > typed["spark.cores.max"]:
            errors["spark.executor.cores"] = "is above the total cores, so no executor could start"
        if typed["spark.driver.memory"] > 1024 * MB:
            warnings["spark.driver.memory"] = "the app container has 2 GB in total, Streamlit included"
        if self._checks:
            self._checks(typed, errors, warnings)

        if cluster:
            executor = typed["spark.executor.memory"]
            worker = cluster["worker_memory_mb"] * MB
            cores = cluster["worker_cores"]
            if executor > worker:
                # Spark standalone doesn't fail: the run would wait for resources forever.
                errors["spark.executor.memory"] = (
                    f"is more than the worker has ({show(self.by_key['spark.executor.memory'], worker)}), "
                    "so no executor could ever start"
                )
            elif executor + overhead_bytes(executor) + (typed["spark.memory.offHeap.size"] or 0) > worker:
                warnings["spark.executor.memory"] = (
                    "with Spark's overhead and off-heap memory, one executor uses more than the worker has"
                )
            if executor_cores and executor_cores > cores:
                errors["spark.executor.cores"] = f"is more than the worker has ({cores}), so no executor could start"
            if executor <= worker:
                per_executor = executor_cores or cores
                wanted = max(1, min(typed["spark.cores.max"], cores) // per_executor)
                fitting = worker // executor
                if wanted > fitting:
                    warnings["spark.executor.memory"] = (
                        f"only {fitting} of the {wanted} executors fit in the worker's memory"
                    )
        return errors, warnings

    def spark_conf(self, values):
        """Spark settings to pass to the session ("" = unset is left out)."""
        conf = {}
        for s in self.settings:
            if s.kind != "spark":
                continue
            value = values.get(s.key, s.default)
            if value in ("", None):
                continue
            conf[s.key] = SERIALIZERS[value] if s.key == "spark.serializer" else str(value)
        if conf.get("spark.eventLog.enabled") == "true":
            conf["spark.eventLog.dir"] = "file:" + EVENT_LOG_DIR
        return conf

    def options(self, values):
        """Typed pipeline, ClickHouse and rule settings for the jobs, by kind
        ("pipeline", "clickhouse", "rules"). Percent values become fractions."""
        result = {"pipeline": {}, "clickhouse": {}, "rules": {}}
        for s in self.settings:
            if s.kind not in ("pipeline", "clickhouse", "rule"):
                continue
            value = self.parse(s.key, values.get(s.key, s.default))
            if s.type == "percent":
                value = value / 100
            group = "rules" if s.kind == "rule" else s.kind
            result[group][s.key.split(".", 1)[1]] = value
        return result

    def changed(self, values):
        """Keys whose value differs from the default."""
        return [s.key for s in self.settings if str(values.get(s.key, s.default)) != s.default]

    def snapshot(self, values, cluster, spark=None, label=""):
        """Every setting used, where they came from (current settings or a
        preset), the cluster seen at start and, for Spark runs, the value Spark
        itself reports for each Spark setting."""
        snap = dict(values)
        if label:
            snap["launch.settings_from"] = label
        for key, value in (cluster or {}).items():
            snap[f"cluster.{key}"] = str(value)
        if spark is not None:
            conf = spark.sparkContext.getConf()
            for key in [*self.spark_conf(values), "spark.driver.memory"]:
                effective = conf.get(key, None)
                if effective is None:
                    effective = spark.conf.get(key, "")
                snap[f"effective.{key}"] = str(effective)
        return snap
