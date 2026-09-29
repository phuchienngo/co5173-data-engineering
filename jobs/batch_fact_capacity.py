"""BR1 batch: TLC raw parquet -> fact_trips lake -> dim_capacity (P95) -> historical demand ratio in ClickHouse."""

import argparse
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common import RAW_DIR, FACT_PATH, CAPACITY_PATH, LANDMARKS, read_trips, with_slot_keys, write_ratios

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", default="yellow")
    parser.add_argument("--baseline-from", default="2025-07")
    parser.add_argument("--baseline-to", default="2026-06")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("br1-batch-fact-capacity").getOrCreate()

    files = sorted(glob.glob(f"{RAW_DIR}/{args.service}/*.parquet"))
    if not files:
        raise SystemExit(f"no parquet under {RAW_DIR}/{args.service} - run scripts/download_tlc.sh first")

    # Dynamic overwrite so rebuilding one service keeps other services' partitions.
    read_trips(spark, args.service, files).write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("service", "pickup_month").parquet(FACT_PATH)

    fact = spark.read.parquet(FACT_PATH)
    hourly = (fact.where((F.col("service") == args.service)
                         & F.col("pickup_month").between(args.baseline_from, args.baseline_to)
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

    n_fact = fact.where(F.col("service") == args.service).count()
    n_hours = hourly.count()
    n_cap = spark.read.parquet(CAPACITY_PATH).count()
    print(f"fact rows ({args.service}): {n_fact}, baseline landmark-hours: {n_hours}, capacity slots: {n_cap}")

    spark.stop()
