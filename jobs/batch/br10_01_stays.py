#!/usr/bin/env python3
"""Derive stays and trips from Inside Airbnb reviews."""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.clickhouse_io import truncate, write_df
from common.config import settings
from common.pipeline_log import job_run
from common.sources import read_all, read_reviews
from common.spark_session import build_spark

STAY_COLUMNS = ["review_id", "reviewer_id", "listing_id", "city_id", "neighbourhood", "room_type",
                "accommodates", "review_date", "trip_no", "is_new_trip"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("br10_01_stays", br="BR10", step="review → stay → trip") as run:
        spark = build_spark("br10_01_stays", local=args.local, shuffle_partitions=16)

        reviews = read_all(spark, read_reviews, snapshots="latest").dropDuplicates(["review_id"])


        listings = spark.read.parquet(f"{settings.lake_dir}/airbnb_listings")
        latest = Window.partitionBy("listing_id").orderBy(F.desc("snapshot_date"))
        attrs = (listings.withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
                 .select("listing_id", "neighbourhood", "room_type", "accommodates"))

        w = Window.partitionBy("reviewer_id").orderBy("review_date", "review_id")
        stays = (reviews.join(attrs, "listing_id", "left")
                 .withColumn("neighbourhood", F.coalesce("neighbourhood", F.lit("(unknown)")))
                 .withColumn("room_type", F.coalesce("room_type", F.lit("(unknown)")))
                 .withColumn("accommodates", F.least(F.coalesce("accommodates", F.lit(0)), F.lit(255)))
                 .withColumn("gap_days", F.datediff("review_date", F.lag("review_date").over(w)))
                 .withColumn("is_new_trip", F.when(F.col("gap_days").isNull()
                                                   | (F.col("gap_days") > settings.same_trip_days), 1).otherwise(0))
                 .withColumn("trip_no", F.sum("is_new_trip").over(
                     w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
                 .drop("gap_days"))

        stays.write.mode("overwrite").partitionBy("city_id").parquet(f"{settings.lake_dir}/stays")
        stays = spark.read.parquet(f"{settings.lake_dir}/stays").cache()

        unmatched = stays.filter(F.col("room_type") == "(unknown)").count()
        total = stays.count()
        print(f"  {total:,} stays · {unmatched:,} without a matching listing "
              f"({100 * unmatched / max(total, 1):.2f} %)")

        stays.groupBy("city_id").agg(
            F.count("*").alias("stays"),
            F.countDistinct("reviewer_id").alias("guests"),
            F.sum("is_new_trip").alias("trips"),
            F.min("review_date").alias("from_date"),
            F.max("review_date").alias("to_date"),
        ).orderBy("city_id").show(truncate=False)

        truncate("fact_stay")
        run.rows = write_df(stays, "fact_stay", columns=STAY_COLUMNS, max_write_partitions=6)
        spark.stop()


if __name__ == "__main__":
    main()
