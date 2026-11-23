"""BR2 dynamic pricing: TLC history -> capacity baseline (once) -> streaming demand ratio -> ClickHouse.

Benchmark mode (tests/br2/benchmark.py): `--bench` times the capacity baseline built from the raw
monthly files vs from the partitioned lake, writes timings to pricing.job_run, then exits (no
demand_ratio_hourly rows, no CAPACITY_PATH, no stream).
  --bench-rep N          repetition number stored in job_run (default 1)
  --bench-order a,b      variant order, raw and/or lake (default raw,lake)
  --bench-lake-build     first time the raw -> lake write (stage lake_build) into FACT_PATH
                         (dynamic overwrite, same data); do not run it while produce_br2.py is reading
"""

import argparse
import glob
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from pricing import (
    CAPACITY_PATH, CHECKPOINT_DIR, FACT_PATH, KAFKA_BOOTSTRAP, LANDMARKS, RAW_DIR, TOPIC,
    ch_insert, read_trips, with_slot_keys, write_ratios,
)
from spark_session import get_spark_session

TRIP_SCHEMA = "service string, pickup_datetime timestamp, pu_location_id int, do_location_id int, fare_amount double"


def raw_files(service):
    files = sorted(glob.glob(f"{RAW_DIR}/{service}/*.parquet"))
    if not files:
        sys.exit(f"no parquet under {RAW_DIR}/{service} - run `bash scripts/download_tlc.sh` first")
    return files


def write_lake(spark, service, files):
    # Dynamic overwrite so rebuilding one service keeps other services' partitions.
    read_trips(spark, service, files).write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("service", "pickup_month").parquet(FACT_PATH)


def hourly_bookings(trips):
    return (trips.groupBy(F.col("do_location_id").alias("location_id"), F.date_trunc("hour", "pickup_datetime").alias("window_start"))
            .agg(F.count("*").alias("active_bookings"), F.avg("fare_amount").alias("avg_fare")))


def capacity_slots(hourly):
    # ponytail: hours with zero bookings are absent, so P95 skews high for quiet zones; cross-join a full hour calendar if that matters
    return (with_slot_keys(hourly, "window_start")
            .groupBy("location_id", "day_type", "hour_of_day")
            .agg(F.percentile_approx("active_bookings", 0.95).cast("int").alias("capacity")))


def run_baseline(spark, service, baseline_from, baseline_to):
    write_lake(spark, service, raw_files(service))

    fact = spark.read.parquet(FACT_PATH)
    hourly = hourly_bookings(fact.where((F.col("service") == service)
                                        & F.col("pickup_month").between(baseline_from, baseline_to)
                                        & F.col("do_location_id").isin(list(LANDMARKS)))).cache()

    capacity = capacity_slots(hourly)
    capacity.write.mode("overwrite").parquet(CAPACITY_PATH)

    write_ratios(hourly, spark.read.parquet(CAPACITY_PATH), "batch")

    n_fact = fact.where(F.col("service") == service).count()
    n_hours = hourly.count()
    n_cap = spark.read.parquet(CAPACITY_PATH).count()
    print(f"fact rows ({service}): {n_fact}, baseline landmark-hours: {n_hours}, capacity slots: {n_cap}")


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def run_bench(spark, args):
    service = args.service
    files = raw_files(service)
    landmarks = list(LANDMARKS)
    rows = []  # (run_mode, stage, seconds, rows, checksum)

    if args.bench_lake_build:
        seconds, _ = timed(lambda: write_lake(spark, service, files))
        n_lake = spark.read.parquet(FACT_PATH).where(F.col("service") == service).count()
        rows.append(("lake", "lake_build", seconds, n_lake, 0))

    def trips(variant):
        if variant == "raw":
            return read_trips(spark, service, files).where(
                F.col("pickup_month").between(args.baseline_from, args.baseline_to) & F.col("do_location_id").isin(landmarks))
        # partition columns service/pickup_month are pruned from the path, other columns by Parquet column pruning
        return (spark.read.parquet(FACT_PATH)
                .where((F.col("service") == service)
                       & F.col("pickup_month").between(args.baseline_from, args.baseline_to)
                       & F.col("do_location_id").isin(landmarks))
                .select("do_location_id", "pickup_datetime", "fare_amount"))

    checks = {}
    for variant in args.bench_order.split(","):
        hourly = hourly_bookings(trips(variant)).cache()
        print(f"== {variant}: hourly plan ==", flush=True)
        hourly.explain(mode="formatted")
        h_sec, (n_hours, checksum) = timed(
            lambda: tuple(hourly.agg(F.count("*"), F.sum("active_bookings")).collect()[0]))
        c_sec, capacity_rows = timed(lambda: capacity_slots(hourly).collect())
        hourly.unpersist()
        checksum = int(checksum or 0)
        checks[variant] = (n_hours, checksum)
        rows += [(variant, "hourly", h_sec, n_hours, checksum),
                 (variant, "capacity", c_sec, len(capacity_rows), sum(r["capacity"] or 0 for r in capacity_rows)),
                 (variant, "total", h_sec + c_sec, n_hours, checksum)]

    ran_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ch_insert([json.dumps({"run_mode": m, "stage": s, "rep": args.bench_rep, "seconds": sec, "rows": n,
                           "checksum": c, "ran_at": ran_at}) for m, s, sec, n, c in rows], table="pricing.job_run")
    print(f"{'run_mode':<9}{'stage':<11}{'seconds':>9}{'rows':>12}{'checksum':>16}")
    for m, s, sec, n, c in rows:
        print(f"{m:<9}{s:<11}{sec:>9.2f}{n:>12}{c:>16}")
    if "raw" in checks and "lake" in checks:
        print(f"hourly checksum raw={checks['raw']} lake={checks['lake']} {'OK' if checks['raw'] == checks['lake'] else 'MISMATCH'}")


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
        .select(F.from_json(F.col("value").cast("string"), TRIP_SCHEMA).alias("t"), "timestamp")
        .select("t.*", "timestamp")
        .where(F.col("do_location_id").isin(list(LANDMARKS)))
    )

    hourly = (
        trips.withWatermark("pickup_datetime", "2 hours")
        .groupBy(F.window("pickup_datetime", "1 hour"), F.col("do_location_id").alias("location_id"))
        .agg(F.count("*").alias("active_bookings"), F.avg("fare_amount").alias("avg_fare"),
             F.max("timestamp").alias("event_time"))
        .select("location_id", F.col("window.start").alias("window_start"), "active_bookings", "avg_fare", "event_time")
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
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--bench-rep", type=int, default=1)
    parser.add_argument("--bench-order", default="raw,lake")
    parser.add_argument("--bench-lake-build", action="store_true")
    args = parser.parse_args()

    spark = get_spark_session("br2-dynamic-pricing")
    # TLC timestamps are NYC wall-clock; UTC session keeps hours unshifted
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    if args.bench:
        run_bench(spark, args)
        return

    if os.path.exists(CAPACITY_PATH):
        print("dim_capacity exists, skipping baseline; delete data/br2/lake/dim_capacity to rebuild")
    else:
        run_baseline(spark, args.service, args.baseline_from, args.baseline_to)
    run_stream(spark)


if __name__ == "__main__":
    main()
