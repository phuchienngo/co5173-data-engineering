import os
from pyspark.sql import SparkSession


def get_spark_session(app_name, conf=None):
    # conf: extra settings that override the defaults below (BR5 passes its
    # per-run settings here). Without it, the session is exactly as before.
    master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Spark 4.0 defaults ANSI mode on, which makes .cast() throw on a
        # malformed value instead of returning null — jobs rely on the
        # null-on-failure behavior to drop bad rows via .na.drop().
        .config("spark.sql.ansi.enabled", "false")
    )
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
