#!/usr/bin/env python3
"""Build historical flight baselines and scored 15-minute facts."""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.clickhouse_io import truncate, write_df
from common.config import ALERT_LEVELS, settings
from common.destinations import monitored_airports
from common.flights import FLOW_FACT_COLUMNS, aggregate_exprs, score_windows
from common.pipeline_log import job_run
from common.sources import read_airports
from common.spark_session import build_spark

BASELINE_COLUMNS = ["airport", "dow_local", "slot_local", "n_days", "baseline_mean",
                    "baseline_p50", "baseline_p95", "baseline_max", "history_from", "history_to"]
ALERT_COLUMNS = ["window_ts", "airport", "city_id", "window_local", "flow_severity", "prev_severity",
                 "alert_level", "arrivals", "capacity", "load_pct", "delayed_share", "message", "source"]


def build_baseline(events, tz):
    """p50/p95 arrival counts per dense 15-minute slot."""
    arr = (events.filter(F.col("event_type") == "arrival")
           .join(F.broadcast(tz), "airport")
           .withColumn("local", F.from_utc_timestamp("event_ts", F.col("timezone")))
           .withColumn("local_date", F.to_date("local"))
           .withColumn("slot_local", (F.hour("local") * 4 + F.floor(F.minute("local") / 15)).cast("int")))

    counts = arr.groupBy("airport", "local_date", "slot_local").agg(F.count("*").alias("n"))


    span = arr.groupBy("airport").agg(F.min("local_date").alias("d0"), F.max("local_date").alias("d1"))
    grid = (span
            .withColumn("local_date", F.explode(F.sequence("d0", "d1")))
            .withColumn("slot_local", F.explode(F.sequence(F.lit(0), F.lit(95))))
            .select("airport", "local_date", "slot_local"))

    dense = (grid.join(counts, ["airport", "local_date", "slot_local"], "left")
             .fillna(0, ["n"])
             .withColumn("dow_local", ((F.dayofweek("local_date") + 5) % 7 + 1).cast("int")))

    return (dense.groupBy("airport", "dow_local", "slot_local")
            .agg(F.count("*").alias("n_days"),
                 F.round(F.avg("n"), 2).alias("baseline_mean"),
                 F.percentile_approx("n", 0.5, 1000).cast("double").alias("baseline_p50"),
                 F.percentile_approx("n", 0.95, 1000).cast("double").alias("baseline_p95"),
                 F.max("n").alias("baseline_max"),
                 F.min("local_date").alias("history_from"),
                 F.max("local_date").alias("history_to")))


def build_windows(events, tz):
    w = F.window("event_ts", f"{settings.window_minutes} minutes")
    return (events.groupBy(w.alias("w"), "airport", "city_id")
            .agg(*aggregate_exprs())
            .withColumn("window_ts", F.col("w.start"))
            .drop("w")
            .join(F.broadcast(tz), "airport"))


def build_alerts(scored):
    win = Window.partitionBy("airport").orderBy("window_ts")
    df = (scored
          .withColumn("prev_severity", F.coalesce(F.lag("flow_severity").over(win), F.lit(0)))
          .filter((F.col("flow_severity") >= 3) & (F.col("prev_severity") < 3)))
    msg = F.format_string(
        "%s · %s local time: %d arrivals / typical high %.0f (%.0f%%), %.0f%% delayed at least 15 minutes",
        F.col("airport"), F.date_format("window_local", "yyyy-MM-dd HH:mm"),
        F.col("arrivals"), F.col("capacity"), F.col("load_pct"), 100 * F.col("delayed_share"))
    return df.withColumn("message", msg)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay-month", help="YYYY-MM reserved for streaming (default: latest month)")
    ap.add_argument("--include-replay-month", action="store_true",
                    help="also write replay month to batch facts (still excluded from baseline)")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("br9_03_flow_baseline", br="BR9", step="p95 baseline + 15-minute facts + alerts") as run:
        spark = build_spark("br9_03_flow_baseline", local=args.local, shuffle_partitions=12)

        events = spark.read.parquet(f"{settings.lake_dir}/flight_events")
        months = sorted(r[0] for r in events.select("year_month").distinct().collect())
        replay = args.replay_month or months[-1]
        history_months = [m for m in months if m < replay]
        if len(history_months) < 3:
            raise ValueError(f"Only {len(history_months)} historical months before {replay}. "
                             "Need at least 3 (6 recommended): ./scripts/run_job.sh bts --months 12")
        print(f"  Historical baseline months: {history_months[0]} → {history_months[-1]}  ({len(history_months)} months)")
        print(f"  Replay month:          {replay}"
              f"{'  (also written by batch)' if args.include_replay_month else '  (reserved for streaming)'}")

        mon = monitored_airports()
        tz = (read_airports(spark).filter(F.col("iata").isin(list(mon)))
              .select(F.col("iata").alias("airport"), "timezone"))

        history = events.filter(F.col("year_month").isin(history_months)).cache()


        baseline = build_baseline(history, tz).cache()
        truncate("baseline_airport_slot")
        n_base = write_df(baseline, "baseline_airport_slot", columns=BASELINE_COLUMNS, max_write_partitions=2)

        peak = (baseline.groupBy("airport").agg(F.max("baseline_p95").alias("max_p95"),
                                                F.round(F.avg("baseline_mean") * 96, 0).alias("arrivals_per_day"))
                .orderBy(F.desc("arrivals_per_day")).collect())
        print("\n  Airport baseline (highest weekly p95 · arrivals/day):")
        for r in peak:
            print(f"    {r['airport']}  p95 max {r['max_p95']:>4.0f}   {r['arrivals_per_day']:>5.0f}/day")


        fact_months = history_months + ([replay] if args.include_replay_month else [])
        scope = events.filter(F.col("year_month").isin(fact_months))
        scored = (score_windows(build_windows(scope, tz), baseline.select(
                      "airport", "dow_local", "slot_local", "baseline_p50", "baseline_p95"))
                  .withColumn("source", F.lit("batch"))
                  .cache())
        n_fact = write_df(scored, "fact_airport_flow_15min", columns=FLOW_FACT_COLUMNS, max_write_partitions=4)

        dist = scored.groupBy("alert_level").count().collect()
        order = {v: k for k, v in ALERT_LEVELS.items()}
        print("\n  Alert-level distribution (windows with flights):")
        for r in sorted(dist, key=lambda r: order[r["alert_level"]]):
            print(f"    {r['alert_level']:10s} {r['count']:>9,}  ({100 * r['count'] / max(n_fact, 1):5.2f} %)")


        alerts = build_alerts(scored).withColumn("source", F.lit("batch"))
        truncate("alert_airport_events")
        n_alert = write_df(alerts, "alert_airport_events", columns=ALERT_COLUMNS, max_write_partitions=1)

        run.rows = n_base + n_fact + n_alert
        spark.stop()


if __name__ == "__main__":
    main()
