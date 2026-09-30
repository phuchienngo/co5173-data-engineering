#!/usr/bin/env python3
"""Build five BR10 reporting marts."""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.clickhouse_io import query_rows, scalar, truncate, write_df
from common.config import settings
from common.destinations import DESTINATIONS
from common.pipeline_log import job_run
from common.spark_session import build_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("br10_03_marts", br="BR10", step="five marts") as run:
        spark = build_spark("br10_03_marts", local=args.local, shuffle_partitions=16)
        lake = settings.lake_dir
        stays = spark.read.parquet(f"{lake}/stays").cache()
        as_of = scalar("SELECT max(as_of_date) FROM tourism.dim_guest")
        total = 0

        names = spark.createDataFrame([(d["city_id"], d["city_name"]) for d in DESTINATIONS] + [("ALL", "All six destinations")],
                                      "city_id string, city_name string")

        # ── 1. KPI theo destinations ───────────────────────────────────
        wc = Window.partitionBy("reviewer_id", "city_id").orderBy("review_date")
        city_trips = (stays
                      .withColumn("gap", F.datediff("review_date", F.lag("review_date").over(wc)))
                      .filter(F.col("gap").isNull() | (F.col("gap") > settings.same_trip_days))
                      .withColumn("k", F.row_number().over(wc)))
        per_guest_city = (city_trips.groupBy("reviewer_id", "city_id")
                          .agg(F.count("*").alias("trips"),
                               F.datediff(F.max(F.when(F.col("k") == 2, F.col("review_date"))),
                                          F.min("review_date")).alias("days_to_return")))
        n_cities = stays.groupBy("reviewer_id").agg(F.countDistinct("city_id").alias("n_cities"))
        pgc = per_guest_city.join(n_cities, "reviewer_id")

        reviews_city = stays.groupBy("city_id").agg(F.count("*").alias("reviews"))
        by_city = (pgc.groupBy("city_id")
                   .agg(F.count("*").alias("guests"),
                        F.sum("trips").alias("trips"),
                        F.sum((F.col("trips") >= 2).cast("int")).alias("repeat_guests"),
                        F.sum((F.col("n_cities") >= 2).cast("int")).alias("multi_city_guests"),
                        F.percentile_approx("days_to_return", 0.5).cast("double").alias("median_days_to_return"))
                   .join(reviews_city, "city_id"))

        all_row = (stays.groupBy("reviewer_id")
                   .agg(F.max("trip_no").alias("trips"), F.count("*").alias("reviews"),
                        F.countDistinct("city_id").alias("n_cities"),
                        F.min("review_date").alias("d1"),
                        F.min(F.when(F.col("trip_no") == 2, F.col("review_date"))).alias("d2"))
                   .agg(F.lit("ALL").alias("city_id"),
                        F.count("*").alias("guests"),
                        F.sum("trips").alias("trips"),
                        F.sum((F.col("trips") >= 2).cast("int")).alias("repeat_guests"),
                        F.sum((F.col("n_cities") >= 2).cast("int")).alias("multi_city_guests"),
                        F.percentile_approx(F.datediff("d2", "d1"), 0.5).cast("double").alias("median_days_to_return"),
                        F.sum("reviews").alias("reviews")))

        overview = (by_city.unionByName(all_row)
                    .withColumn("repeat_rate_pct", F.round(100 * F.col("repeat_guests") / F.col("guests"), 2))
                    .withColumn("multi_city_pct", F.round(100 * F.col("multi_city_guests") / F.col("guests"), 2))
                    .join(names, "city_id", "left")
                    .withColumn("as_of_date", F.lit(as_of).cast("date")))
        cols = ["city_id", "city_name", "guests", "trips", "reviews", "repeat_guests", "repeat_rate_pct",
                "multi_city_guests", "multi_city_pct", "median_days_to_return", "as_of_date"]
        truncate("mart_br10_city_overview")
        total += write_df(overview, "mart_br10_city_overview", columns=cols, max_write_partitions=1)
        print("\n  Destination KPIs:")
        overview.select("city_id", "guests", "repeat_rate_pct", "multi_city_pct", "median_days_to_return") \
            .orderBy(F.desc("guests")).show(truncate=False)

        # ── 2. Cohort ──────────────────────────────────────────────
        trip_starts = stays.filter("is_new_trip = 1").select("reviewer_id", "review_date", "trip_no")
        first = (trip_starts.filter("trip_no = 1")
                 .select("reviewer_id", F.trunc("review_date", "month").alias("cohort_month")))
        last_month = F.add_months(F.trunc(F.lit(as_of).cast("date"), "month"), -settings.cohort_drop_last_months)
        activity = (trip_starts.join(first, "reviewer_id")
                    .withColumn("act_month", F.trunc("review_date", "month"))
                    .filter((F.col("cohort_month") >= F.lit(settings.cohort_start).cast("date"))
                            & (F.col("act_month") <= last_month))
                    .withColumn("month_offset", F.months_between("act_month", "cohort_month").cast("int")))

        sizes = activity.filter("month_offset = 0").groupBy("cohort_month") \
            .agg(F.countDistinct("reviewer_id").alias("cohort_size"))
        active = activity.filter("month_offset >= 1").groupBy("cohort_month", "month_offset") \
            .agg(F.countDistinct("reviewer_id").alias("active_guests"))
        first_return = (activity.filter("month_offset >= 1").groupBy("reviewer_id", "cohort_month")
                        .agg(F.min("month_offset").alias("m")))
        returned = first_return.groupBy("cohort_month", F.col("m").alias("month_offset")) \
            .agg(F.count("*").alias("newly_returned"))

        grid = (sizes.withColumn("max_off", F.months_between(last_month, "cohort_month").cast("int"))
                .withColumn("month_offset", F.explode(F.sequence(F.lit(0), F.col("max_off"))))
                .drop("max_off"))
        cw = Window.partitionBy("cohort_month").orderBy("month_offset").rowsBetween(Window.unboundedPreceding, 0)
        cohort = (grid.join(active, ["cohort_month", "month_offset"], "left")
                  .join(returned, ["cohort_month", "month_offset"], "left")
                  .fillna(0, ["active_guests", "newly_returned"])
                  .withColumn("active_guests", F.when(F.col("month_offset") == 0, F.col("cohort_size"))
                              .otherwise(F.col("active_guests")))
                  .withColumn("cumulative_returned", F.sum("newly_returned").over(cw))
                  .withColumn("retention_pct", F.round(100 * F.col("active_guests") / F.col("cohort_size"), 2))
                  .withColumn("cumulative_return_pct",
                              F.round(100 * F.col("cumulative_returned") / F.col("cohort_size"), 2))
                  .filter(F.col("cohort_size") >= settings.min_cohort_size))
        cols = ["cohort_month", "month_offset", "cohort_size", "active_guests", "retention_pct",
                "cumulative_returned", "cumulative_return_pct"]
        truncate("mart_br10_cohort")
        total += write_df(cohort, "mart_br10_cohort", columns=cols, max_write_partitions=1)


        wt = Window.partitionBy("reviewer_id").orderBy("trip_no")
        flow = (stays.filter("is_new_trip = 1").select("reviewer_id", "trip_no", "city_id")
                .withColumn("to_city", F.lead("city_id").over(wt))
                .filter(F.col("to_city").isNotNull())
                .groupBy(F.col("city_id").alias("from_city"), "to_city")
                .agg(F.count("*").alias("transitions"), F.countDistinct("reviewer_id").alias("guests")))
        truncate("mart_br10_city_flow")
        total += write_df(flow, "mart_br10_city_flow",
                          columns=["from_city", "to_city", "transitions", "guests"], max_write_partitions=1)

        # ── 4. RFM ─────────────────────────────────────────────────
        rows = query_rows("""
            SELECT rfm_segment, count() AS guests,
                   round(100 * count() / sum(count()) OVER (), 2) AS share_pct,
                   round(avg(recency_days), 1), round(avg(n_trips), 2), round(avg(n_cities), 2)
            FROM tourism.dim_guest FINAL GROUP BY rfm_segment
        """)
        seg = spark.createDataFrame(
            [(r[0], int(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in rows],
            "rfm_segment string, guests long, share_pct double, avg_recency_days double, "
            "avg_trips double, avg_cities double")
        truncate("mart_br10_rfm_segment")
        total += write_df(seg, "mart_br10_rfm_segment", columns=seg.columns, max_write_partitions=1)


        first_date = stays.groupBy("reviewer_id").agg(F.min("review_date").alias("first_date"))
        yearly_base = (stays.join(first_date, "reviewer_id")
                       .withColumn("year", F.year("review_date"))
                       .withColumn("is_returning", (F.year("first_date") < F.col("year")).cast("int")))
        per_city = yearly_base.groupBy("year", "city_id").agg(
            F.count("*").alias("reviews"),
            F.countDistinct("reviewer_id").alias("guests"),
            F.countDistinct(F.when(F.col("is_returning") == 1, F.col("reviewer_id"))).alias("returning_guests"))
        all_city = yearly_base.groupBy("year").agg(
            F.lit("ALL").alias("city_id"),
            F.count("*").alias("reviews"),
            F.countDistinct("reviewer_id").alias("guests"),
            F.countDistinct(F.when(F.col("is_returning") == 1, F.col("reviewer_id"))).alias("returning_guests"))
        yearly = (per_city.unionByName(all_city)
                  .withColumn("returning_pct", F.round(100 * F.col("returning_guests") / F.col("guests"), 2)))
        truncate("mart_br10_yearly")
        total += write_df(yearly, "mart_br10_yearly",
                          columns=["year", "city_id", "reviews", "guests", "returning_guests", "returning_pct"],
                          max_write_partitions=1)

        run.rows = total
        spark.stop()


if __name__ == "__main__":
    main()
