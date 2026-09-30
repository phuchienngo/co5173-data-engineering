#!/usr/bin/env python3
"""Estimate nightly Airbnb unavailable guest capacity."""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.clickhouse_io import truncate, write_df
from common.config import settings
from common.destinations import DESTINATIONS
from common.pipeline_log import job_run
from common.sources import read_all, read_calendar, read_listings
from common.spark_session import build_spark

OCC_COLUMNS = ["city_id", "neighbourhood", "neighbourhood_group", "snapshot_date", "night_date",
               "nights_ahead", "active_listings", "booked_listings", "capacity_guests",
               "booked_guests", "occupancy_pct"]
NB_COLUMNS = ["city_id", "neighbourhood", "neighbourhood_group", "latitude", "longitude",
              "snapshot_date", "listings", "active_listings", "capacity_guests"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("br9_04_occupancy", br="BR9", step="Airbnb calendar → unavailable capacity") as run:
        spark = build_spark("br9_04_occupancy", local=args.local, shuffle_partitions=12)

        listings = (read_all(spark, read_listings)
                    .withColumn("neighbourhood", F.coalesce("neighbourhood", F.lit("(unknown)")))
                    .withColumn("neighbourhood_group", F.coalesce("neighbourhood_group", F.lit("")))
                    .withColumn("is_active", ((F.col("availability_365") > 0)
                                              & (F.col("number_of_reviews_ltm") > 0)
                                              & (F.col("accommodates") > 0)).cast("int")))
        listings.write.mode("overwrite").partitionBy("city_id", "snapshot_date") \
            .parquet(f"{settings.lake_dir}/airbnb_listings")
        listings = spark.read.parquet(f"{settings.lake_dir}/airbnb_listings").cache()

        active = listings.filter("is_active = 1").select(
            "listing_id", "city_id", "snapshot_date", "neighbourhood", "neighbourhood_group", "accommodates")

        calendar = read_all(spark, read_calendar)
        occ = (calendar.join(active, ["listing_id", "city_id", "snapshot_date"])
               .groupBy("city_id", "neighbourhood", "neighbourhood_group", "snapshot_date", "night_date")
               .agg(F.count("*").alias("active_listings"),
                    F.sum("is_unavailable").alias("booked_listings"),
                    F.sum("accommodates").alias("capacity_guests"),
                    F.sum(F.col("is_unavailable") * F.col("accommodates")).alias("booked_guests"))
               .withColumn("nights_ahead", F.datediff("night_date", "snapshot_date"))
               .filter(F.col("nights_ahead") >= 0)
               .withColumn("occupancy_pct",
                           F.round(100.0 * F.col("booked_guests") / F.col("capacity_guests"), 1))
               .cache())

        truncate("fact_occupancy_night")
        n_occ = write_df(occ, "fact_occupancy_night", columns=OCC_COLUMNS, max_write_partitions=4)


        latest = F.max("snapshot_date").over(Window.partitionBy("city_id"))
        newest = listings.withColumn("latest", latest).filter(F.col("snapshot_date") == F.col("latest"))
        nb = (newest.groupBy("city_id", "neighbourhood")
              .agg(F.first("neighbourhood_group").alias("neighbourhood_group"),
                   F.avg("latitude").alias("latitude"),
                   F.avg("longitude").alias("longitude"),
                   F.max("snapshot_date").alias("snapshot_date"),
                   F.count("*").alias("listings"),
                   F.sum("is_active").alias("active_listings"),
                   F.sum(F.col("is_active") * F.col("accommodates")).alias("capacity_guests")))
        truncate("dim_neighbourhood")
        n_nb = write_df(nb, "dim_neighbourhood", columns=NB_COLUMNS, max_write_partitions=1)


        print("\n  Destination · snapshot · active listings · capacity · unavailable next 30 nights")
        summary = (occ.filter(F.col("nights_ahead") < 30)
                   .groupBy("city_id", "snapshot_date")
                   .agg(F.round(100 * F.sum("booked_guests") / F.sum("capacity_guests"), 1).alias("occ30"))
                   .join(listings.groupBy("city_id", "snapshot_date")
                         .agg(F.sum("is_active").alias("active"),
                              F.sum(F.col("is_active") * F.col("accommodates")).alias("cap")),
                         ["city_id", "snapshot_date"])
                   .orderBy("city_id", "snapshot_date").collect())
        for r in summary:
            print(f"    {r['city_id']:14s} {r['snapshot_date']}  {r['active']:>7,}  {r['cap']:>8,} guests  {r['occ30']:>5} %")

        expected = sum(len(d["airbnb_snapshots"]) for d in DESTINATIONS)
        if len(summary) < expected:
            print(f"  ⚠️ Only {len(summary)}/{expected} snapshots — check ./scripts/run_job.sh airbnb")

        run.rows = n_occ + n_nb
        spark.stop()


if __name__ == "__main__":
    main()
