"""Build consistently configured Spark sessions."""

from __future__ import annotations

from pyspark.sql import SparkSession

from common.config import settings


def build_spark(app_name: str, *, local: bool = False, shuffle_partitions: int = 8) -> SparkSession:
    """Build spark."""
    master = "local[*]" if local else settings.spark_master

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)

        .config("spark.sql.session.timeZone", "UTC")

        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")


        .config("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
        # ── Ghi file ─────────────────────────────────────────────────
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")

        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")

        .config("spark.eventLog.enabled", "false")

        .config("spark.ui.showConsoleProgress", "false")
    )

    spark = builder.getOrCreate()


    spark.sparkContext.setLogLevel("WARN")
    return spark
