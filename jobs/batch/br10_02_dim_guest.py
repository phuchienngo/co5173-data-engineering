#!/usr/bin/env python3
"""Build the guest dimension and rule-based RFM segments."""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.clickhouse_io import truncate, write_df
from common.config import settings
from common.pipeline_log import job_run
from common.spark_session import build_spark

GUEST_COLUMNS = [
    "reviewer_id", "first_trip_date", "last_trip_date", "cohort_month", "first_city", "last_city",
    "n_reviews", "n_trips", "n_cities", "cities", "max_trips_one_city", "is_repeat",
    "is_repeat_same_city", "is_multi_city", "days_to_second_trip", "avg_gap_days", "lifespan_days",
    "recency_days", "guest_tier", "r_score", "f_score", "m_score", "rfm_segment", "as_of_date",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("br10_02_dim_guest", br="BR10", step="stays → guest dimension + RFM") as run:
        spark = build_spark("br10_02_dim_guest", local=args.local, shuffle_partitions=16)
        stays = spark.read.parquet(f"{settings.lake_dir}/stays")
        as_of = stays.agg(F.max("review_date")).first()[0]
        print(f"  as_of_date = {as_of}")

        trips = stays.filter("is_new_trip = 1").select("reviewer_id", "city_id", "review_date", "trip_no")


        second = (trips.filter("trip_no = 2")
                  .select("reviewer_id", F.col("review_date").alias("second_trip_date")))


        wc = Window.partitionBy("reviewer_id", "city_id").orderBy("review_date")
        city_trips = (stays
                      .withColumn("gap", F.datediff("review_date", F.lag("review_date").over(wc)))
                      .withColumn("new_city_trip", F.when(F.col("gap").isNull()
                                                          | (F.col("gap") > settings.same_trip_days), 1).otherwise(0))
                      .groupBy("reviewer_id", "city_id").agg(F.sum("new_city_trip").alias("trips_in_city"))
                      .groupBy("reviewer_id").agg(F.max("trips_in_city").alias("max_trips_one_city")))

        first_last = Window.partitionBy("reviewer_id").orderBy("review_date", "trip_no")
        ends = (trips
                .withColumn("rn_first", F.row_number().over(first_last))
                .withColumn("rn_last", F.row_number().over(
                    Window.partitionBy("reviewer_id").orderBy(F.desc("review_date"), F.desc("trip_no"))))
                .groupBy("reviewer_id")
                .agg(F.max(F.when(F.col("rn_first") == 1, F.col("city_id"))).alias("first_city"),
                     F.max(F.when(F.col("rn_last") == 1, F.col("city_id"))).alias("last_city")))

        g = (stays.groupBy("reviewer_id")
             .agg(F.min("review_date").alias("first_trip_date"),
                  F.max("review_date").alias("last_trip_date"),
                  F.count("*").alias("n_reviews"),
                  F.max("trip_no").alias("n_trips"),
                  F.countDistinct("city_id").alias("n_cities"),
                  F.array_sort(F.collect_set("city_id")).alias("cities"))
             .join(second, "reviewer_id", "left")
             .join(city_trips, "reviewer_id", "left")
             .join(ends, "reviewer_id", "left"))

        lit_asof = F.lit(as_of).cast("date")
        g = (g
             .withColumn("cohort_month", F.trunc("first_trip_date", "month"))
             .withColumn("is_repeat", (F.col("n_trips") >= 2).cast("int"))
             .withColumn("is_repeat_same_city", (F.col("max_trips_one_city") >= 2).cast("int"))
             .withColumn("is_multi_city", (F.col("n_cities") >= 2).cast("int"))
             .withColumn("days_to_second_trip", F.datediff("second_trip_date", "first_trip_date"))
             .withColumn("lifespan_days", F.datediff("last_trip_date", "first_trip_date"))
             .withColumn("avg_gap_days", F.when(F.col("n_trips") >= 2,
                                                F.round(F.col("lifespan_days") / (F.col("n_trips") - 1), 1)))
             .withColumn("recency_days", F.datediff(lit_asof, "last_trip_date"))
             .withColumn("as_of_date", lit_asof))

        g = (g
             .withColumn("guest_tier", F.when(F.col("n_trips") >= 5, "Champion")
                                        .when(F.col("n_trips") >= 3, "Loyal")
                                        .when(F.col("n_trips") == 2, "Returning")
                                        .otherwise("One-time"))
             .withColumn("r_score", F.when(F.col("recency_days") <= 180, 5).when(F.col("recency_days") <= 365, 4)
                                      .when(F.col("recency_days") <= 730, 3).when(F.col("recency_days") <= 1460, 2)
                                      .otherwise(1))
             .withColumn("f_score", F.when(F.col("n_trips") >= 5, 5).when(F.col("n_trips") >= 3, 4)
                                      .when(F.col("n_trips") == 2, 3).otherwise(1))
             .withColumn("m_score", F.when(F.col("n_cities") >= 4, 5).when(F.col("n_cities") == 3, 4)
                                      .when(F.col("n_cities") == 2, 3).otherwise(1)))


        g = g.withColumn("rfm_segment",
            F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champions")
             .when(F.col("m_score") >= 4, "Explorers")
             .when((F.col("r_score") >= 4) & (F.col("f_score") == 3), "Recent returners")
             .when(F.col("f_score") >= 4, "Loyal · inactive")
             .when((F.col("f_score") >= 3) & (F.col("r_score") <= 2), "At risk")
             .when((F.col("r_score") >= 4) & (F.col("f_score") == 1), "New")
             .otherwise("Hibernating"))

        g = g.cache()
        s = g.agg(F.count("*").alias("guests"),
                  F.round(100 * F.avg("is_repeat"), 2).alias("repeat_pct"),
                  F.round(100 * F.avg("is_repeat_same_city"), 2).alias("repeat_same_city_pct"),
                  F.round(100 * F.avg("is_multi_city"), 2).alias("multi_city_pct")).first()
        print(f"  {s['guests']:,} guests · repeat (any destination) {s['repeat_pct']} % · "
              f"repeat in same destination {s['repeat_same_city_pct']} % · multiple destinations {s['multi_city_pct']} %")
        g.groupBy("rfm_segment").count().orderBy(F.desc("count")).show(truncate=False)

        truncate("dim_guest")
        run.rows = write_df(g, "dim_guest", columns=GUEST_COLUMNS, max_write_partitions=6)
        spark.stop()


if __name__ == "__main__":
    main()
