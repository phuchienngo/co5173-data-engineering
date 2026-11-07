"""BR7 batch preparation: clean TLC trips, build hourly baselines, export replay.

Run through scripts/prepare_br7.py, which downloads and checksums the inputs.
All timestamps are handled with the Spark session in UTC. TLC stores local
New York wall-clock times, so they are converted explicitly with LOCAL_TZ.
"""

import argparse
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from br7_rules import LOCAL_TZ, MIN_TRAINING_HOURS, TOURIST_ZONES

DATA = "/opt/data/br7"
MAX_TRIP_SECONDS = 6 * 3600
MAX_TRIP_MILES = 100


def month_bounds(month):
    start = datetime.strptime(month, "%Y-%m")
    end = start.replace(year=start.year + (start.month == 12), month=start.month % 12 + 1)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def local_hour_of_week(column):
    # Input is a UTC instant; Monday 00:00 New York time = 0.
    local = F.from_utc_timestamp(column, LOCAL_TZ)
    return ((F.dayofweek(local) + 5) % 7) * 24 + F.hour(local)


def load_month(spark, month):
    """Read one TLC file, cast to a stable schema and label every row's quality."""
    frame = spark.read.parquet(f"{DATA}/raw/yellow_tripdata_{month}.parquet")
    start, end = month_bounds(month)
    # Casting the TLC wall-clock value to a UTC-session timestamp keeps the digits;
    # to_utc_timestamp then interprets those digits as New York local time.
    pickup_local = F.col("tpep_pickup_datetime").cast("timestamp")
    dropoff_local = F.col("tpep_dropoff_datetime").cast("timestamp")
    trips = frame.select(
        F.lit(month).alias("source_month"),
        F.col("VendorID").cast("int").alias("vendor_id"),
        pickup_local.alias("pickup_local"),
        dropoff_local.alias("dropoff_local"),
        F.col("PULocationID").cast("int").alias("pickup_zone_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )
    seconds = F.col("dropoff_local").cast("long") - F.col("pickup_local").cast("long")
    reason = (
        F.when(F.col("pickup_local").isNull() | F.col("dropoff_local").isNull()
               | F.col("pickup_zone_id").isNull() | F.col("dropoff_zone_id").isNull(), "missing_field")
        .when((F.col("dropoff_local") < F.lit(start).cast("timestamp"))
              | (F.col("dropoff_local") >= F.lit(end).cast("timestamp")), "dropoff_outside_file_month")
        .when(seconds <= 0, "dropoff_not_after_pickup")
        .when(seconds > MAX_TRIP_SECONDS, "trip_longer_than_6h")
        .when((F.col("trip_distance") <= 0) | (F.col("trip_distance") > MAX_TRIP_MILES), "invalid_distance")
        .when(F.col("total_amount") <= 0, "nonpositive_amount")
        .otherwise("ok")
    )
    return trips.withColumn("quality", reason)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-months", required=True)
    parser.add_argument("--replay-month", required=True)
    parser.add_argument("--prepare-version", type=int, required=True)
    parser.add_argument("--source-sha256", required=True)
    args = parser.parse_args()
    train_months = args.train_months.split(",")

    spark = SparkSession.builder.appName("br7_prepare_tlc").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    labelled = None
    for month in train_months + [args.replay_month]:
        frame = load_month(spark, month)
        labelled = frame if labelled is None else labelled.unionByName(frame)
    # One pass over all rows for the quality report; no cache, because a year of
    # trips does not fit the 2g driver used on small machines.
    quality = {
        f"{row['source_month']}:{row['quality']}": row["count"]
        for row in labelled.groupBy("source_month", "quality").count().collect()
    }

    zones = list(TOURIST_ZONES)
    trips = (
        labelled.filter(F.col("dropoff_zone_id").isin(zones))
        .filter(F.col("quality") == "ok")
        .withColumn("dropoff_time", F.to_utc_timestamp("dropoff_local", LOCAL_TZ))
        .withColumn("pickup_time", F.to_utc_timestamp("pickup_local", LOCAL_TZ))
    )
    # TLC files contain exact duplicate rows; keep one per natural trip key.
    key = ["vendor_id", "pickup_time", "dropoff_time", "pickup_zone_id",
           "dropoff_zone_id", "trip_distance", "total_amount"]
    before_dedup = trips.groupBy("source_month").count().collect()
    trips = trips.dropDuplicates(key).withColumn(
        "event_id",
        F.sha2(F.concat_ws("|",
                           F.date_format("pickup_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                           F.date_format("dropoff_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                           "pickup_zone_id", "dropoff_zone_id", "trip_distance", "total_amount"), 256),
    ).cache()
    after_dedup = {row["source_month"]: row["count"] for row in trips.groupBy("source_month").count().collect()}
    duplicates = {row["source_month"]: row["count"] - after_dedup.get(row["source_month"], 0) for row in before_dedup}

    # Hourly arrivals for the training window, zero-filled so quiet hours count.
    train_start, _ = month_bounds(train_months[0])
    _, train_end = month_bounds(train_months[-1])
    hours = spark.sql(
        f"SELECT explode(sequence(to_utc_timestamp(timestamp'{train_start}', '{LOCAL_TZ}'), "
        f"to_utc_timestamp(timestamp'{train_end}', '{LOCAL_TZ}') - INTERVAL 1 HOUR, INTERVAL 1 HOUR)) AS hour_start"
    )
    zone_frame = spark.createDataFrame([(zone,) for zone in zones], ["zone_id"])
    grid = zone_frame.crossJoin(hours)
    counted = (
        trips.filter(F.col("source_month").isin(train_months))
        .groupBy(F.col("dropoff_zone_id").alias("zone_id"),
                 F.date_trunc("hour", "dropoff_time").alias("hour_start"))
        .agg(F.count("*").alias("arrivals"))
    )
    hourly = grid.join(counted, ["zone_id", "hour_start"], "left").fillna(0, ["arrivals"])
    baseline = (
        hourly.withColumn("hour_of_week", local_hour_of_week(F.col("hour_start")))
        .groupBy("zone_id", "hour_of_week")
        .agg(
            F.round(F.avg("arrivals"), 4).alias("mean_arrivals"),
            F.round(F.expr("percentile(arrivals, 0.9)"), 4).alias("p90_arrivals"),
            F.count("*").alias("training_hours"),
        )
        .filter(F.col("training_hours") >= MIN_TRAINING_HOURS)
    )

    lookup = spark.read.option("header", True).csv(f"{DATA}/raw/taxi_zone_lookup.csv")
    landmarks = spark.createDataFrame(list(TOURIST_ZONES.items()), ["zone_id", "landmark"])
    zone_dim = landmarks.join(
        lookup.select(F.col("LocationID").cast("int").alias("zone_id"),
                      F.col("Zone").alias("zone_name"), F.col("Borough").alias("borough")),
        "zone_id", "left",
    ).select("zone_id", "zone_name", "borough", "landmark")

    replay = (
        trips.filter(F.col("source_month") == args.replay_month)
        .select(
            "event_id", "dropoff_zone_id", "pickup_zone_id",
            F.date_format("dropoff_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("dropoff_time"),
            F.coalesce("passenger_count", F.lit(0)).alias("passenger_count"),
        )
    )
    out = f"{DATA}/prepared"
    # One sorted file keeps the replay in event-time order.
    replay.coalesce(1).sortWithinPartitions("dropoff_time", "event_id").write.mode("overwrite") \
        .option("header", True).csv(f"{out}/replay")
    baseline.coalesce(1).orderBy("zone_id", "hour_of_week").write.mode("overwrite") \
        .option("header", True).csv(f"{out}/baseline")
    zone_dim.coalesce(1).orderBy("zone_id").write.mode("overwrite").option("header", True).csv(f"{out}/zones")

    replay_counts = (
        trips.filter(F.col("source_month") == args.replay_month)
        .groupBy("dropoff_zone_id").count().orderBy("dropoff_zone_id").collect()
    )
    missing_zones = [row["zone_id"] for row in zone_dim.filter(F.col("zone_name").isNull()).collect()]
    if missing_zones:
        raise ValueError(f"Tourist zones missing from taxi_zone_lookup.csv: {missing_zones}")
    summary = {
        "source": "NYC TLC Yellow Taxi Trip Records",
        "prepare_version": args.prepare_version,
        "train_months": train_months,
        "replay_month": args.replay_month,
        "source_sha256": json.loads(args.source_sha256),
        "timezone": LOCAL_TZ,
        "tourist_zones": {str(k): v for k, v in TOURIST_ZONES.items()},
        "rows_by_quality": dict(sorted(quality.items())),
        "duplicate_trips_removed_in_tourist_zones": dict(sorted(duplicates.items())),
        "tourist_zone_trips_by_month": dict(sorted(after_dedup.items())),
        "replay_events": sum(row["count"] for row in replay_counts),
        "replay_events_by_zone": {str(row["dropoff_zone_id"]): row["count"] for row in replay_counts},
        "baseline_cells": baseline.count(),
        "min_training_hours": MIN_TRAINING_HOURS,
    }
    with open(f"{out}/summary.json", "w") as stream:
        stream.write(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
