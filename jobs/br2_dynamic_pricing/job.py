"""BR2 dynamic pricing: TLC history -> capacity baseline (once) -> streaming demand ratio -> ClickHouse."""

import argparse
import glob
import os
import shutil
import sys

from pyspark.sql import functions as F

from pricing import (
    CAPACITY_PATH, CHECKPOINT_DIR, FACT_PATH, KAFKA_BOOTSTRAP, LANDMARKS, RAW_DIR, TOPIC,
    read_trips, with_slot_keys, write_ratios,
)
from spark_session import get_spark_session

TRIP_SCHEMA = "service string, pickup_datetime timestamp, pu_location_id int, do_location_id int, fare_amount double"


def run_baseline(spark, service, baseline_from, baseline_to):
    files = sorted(glob.glob(f"{RAW_DIR}/{service}/*.parquet"))
    if not files:
        sys.exit(f"no parquet under {RAW_DIR}/{service} - run `bash scripts/download_tlc.sh` first")

    # Dynamic overwrite so rebuilding one service keeps other services' partitions.
    read_trips(spark, service, files).write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("service", "pickup_month").parquet(FACT_PATH)

    fact = spark.read.parquet(FACT_PATH)
    hourly = (fact.where((F.col("service") == service)
                         & F.col("pickup_month").between(baseline_from, baseline_to)
                         & F.col("do_location_id").isin(list(LANDMARKS)))
              .groupBy(F.col("do_location_id").alias("location_id"), F.date_trunc("hour", "pickup_datetime").alias("window_start"))
              .agg(F.count("*").alias("active_bookings"), F.avg("fare_amount").alias("avg_fare"))
              .cache())

    # ponytail: hours with zero bookings are absent, so P95 skews high for quiet zones; cross-join a full hour calendar if that matters
    capacity = (with_slot_keys(hourly, "window_start")
                .groupBy("location_id", "day_type", "hour_of_day")
                .agg(F.percentile_approx("active_bookings", 0.95).cast("int").alias("capacity")))
    capacity.write.mode("overwrite").parquet(CAPACITY_PATH)

    write_ratios(hourly, spark.read.parquet(CAPACITY_PATH), "batch")

    n_fact = fact.where(F.col("service") == service).count()
    n_hours = hourly.count()
    n_cap = spark.read.parquet(CAPACITY_PATH).count()
    print(f"fact rows ({service}): {n_fact}, baseline landmark-hours: {n_hours}, capacity slots: {n_cap}")


def run_stream(spark):
    # start.py resets Kafka topics every run, so checkpointed offsets would be invalid.
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
    capacity = spark.read.parquet(CAPACITY_PATH).cache()

    trips = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS", "20000"))
        .load()
        .select(F.from_json(F.col("value").cast("string"), TRIP_SCHEMA).alias("t"))
        .select("t.*")
        .where(F.col("do_location_id").isin(list(LANDMARKS)))
    )

    hourly = (
        trips.withWatermark("pickup_datetime", "2 hours")
        .groupBy(F.window("pickup_datetime", "1 hour"), F.col("do_location_id").alias("location_id"))
        .agg(F.count("*").alias("active_bookings"), F.avg("fare_amount").alias("avg_fare"))
        .select("location_id", F.col("window.start").alias("window_start"), "active_bookings", "avg_fare")
    )

    # update mode re-emits a window whenever its count changes; ClickHouse ReplacingMergeTree
    # keeps the latest row per (location_id, window_start).
    (
        hourly.writeStream.queryName("demand_ratio").outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .foreachBatch(lambda batch, _batch_id: write_ratios(batch, capacity, "stream"))
        .start()
        .awaitTermination()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", default=os.getenv("BR2_SERVICE", "yellow"))
    parser.add_argument("--baseline-from", default=os.getenv("BR2_BASELINE_FROM", "2025-07"))
    parser.add_argument("--baseline-to", default=os.getenv("BR2_BASELINE_TO", "2026-06"))
    args = parser.parse_args()

    spark = get_spark_session("br2-dynamic-pricing")
    # TLC timestamps are NYC wall-clock; UTC session keeps hours unshifted
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    if os.path.exists(CAPACITY_PATH):
        print("dim_capacity exists, skipping baseline; delete data/br2/lake/dim_capacity to rebuild")
    else:
        run_baseline(spark, args.service, args.baseline_from, args.baseline_to)
    run_stream(spark)


if __name__ == "__main__":
    main()
