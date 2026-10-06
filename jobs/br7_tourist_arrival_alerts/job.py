"""Forecast next-hour entries and flag unusual arrivals for six Orléans sites."""

import os
from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from br7_sites import CORE_SITES, HOLDOUT_DAYS, PARTIAL_SITES
from clickhouse_io import truncate_table, write_dataframe
from spark_session import get_spark_session

SOURCE_PATH = "/opt/data/br7/frequentation_par_heure.csv"
EVENT_SCHEMA = StructType(
    [
        StructField("site", StringType()),
        StructField("horodatage", StringType()),
        StructField("entrees", IntegerType()),
    ]
)

spark = get_spark_session("br7_tourist_arrival_alerts")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

source = spark.read.option("header", True).option("encoding", "UTF-8").csv(SOURCE_PATH)
source = source.toDF(*(column.lstrip("\ufeff") for column in source.columns))
history = (
    source.select(
        F.col("site").alias("site_id"),
        F.to_timestamp("horodatage").alias("observed_hour"),
        F.col("entrees").cast("int").alias("observed_entries"),
    )
    .filter(F.col("site_id").isin(*CORE_SITES))
    .filter(F.col("observed_hour").isNotNull())
    .filter(F.col("observed_entries") >= 0)
)
latest_hour = history.agg(F.max("observed_hour")).first()[0]
if latest_hour is None:
    raise ValueError("No valid core-site hourly observations in BR7 source")
cutoff = latest_hour - timedelta(days=HOLDOUT_DAYS)


def hour_of_week(column):
    # Monday = 0, Sunday = 6, in the UTC timezone used by the source.
    return ((F.dayofweek(column) + F.lit(5)) % F.lit(7)) * F.lit(24) + F.hour(column)


baseline = (
    history.filter(F.col("observed_hour") < F.lit(cutoff))
    .withColumn("hour_of_week", hour_of_week(F.col("observed_hour")))
    .groupBy("site_id", "hour_of_week")
    .agg(
        F.avg("observed_entries").alias("mean_entries"),
        F.percentile_approx("observed_entries", 0.9).cast("double").alias("p90_entries"),
        F.count("*").alias("training_hours"),
    )
    .filter(F.col("training_hours") >= 10)
    .cache()
)
if baseline.isEmpty():
    raise ValueError("No BR7 baseline with at least 10 historical matching hours")

dim_site = spark.createDataFrame(
    [(site, 1) for site in CORE_SITES] + [(site, 0) for site in PARTIAL_SITES],
    ["site_id", "is_core"],
)
truncate_table("dim_tourist_site")
write_dataframe(dim_site.toPandas(), "dim_tourist_site")
truncate_table("dim_site_hour_baseline")
write_dataframe(baseline.toPandas(), "dim_site_hour_baseline")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    .option("subscribe", "tourist_arrivals")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)
observed = (
    raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
    .select(
        F.col("data.site").alias("site_id"),
        F.to_timestamp(F.col("data.horodatage")).alias("observed_hour"),
        F.col("data.entrees").alias("observed_entries"),
    )
    .filter(F.col("site_id").isin(*CORE_SITES))
    .filter(F.col("observed_hour").isNotNull())
    .filter(F.col("observed_entries") >= 0)
    .withColumn("target_hour", F.col("observed_hour") + F.expr("INTERVAL 1 HOUR"))
    .withColumn("hour_of_week", hour_of_week(F.col("target_hour")))
)

forecast = (
    observed.join(F.broadcast(baseline), ["site_id", "hour_of_week"], "inner")
    .withColumn(
        "predicted_entries",
        F.round((F.col("observed_entries") + F.col("mean_entries")) / 2, 2),
    )
    .withColumn("warning_threshold", F.col("p90_entries"))
    .withColumn(
        "alert_level",
        F.when(F.col("predicted_entries") > F.col("warning_threshold"), "warning").otherwise("normal"),
    )
    .select(
        "site_id",
        "observed_hour",
        "target_hour",
        "observed_entries",
        "predicted_entries",
        "warning_threshold",
        "alert_level",
    )
)


def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        write_dataframe(batch_df.toPandas(), "fact_tourist_arrivals")


forecast.writeStream.foreachBatch(write_batch).trigger(processingTime="5 seconds").start().awaitTermination()
