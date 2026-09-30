#!/usr/bin/env python3
"""Score Kafka flight events using Spark Structured Streaming."""

from __future__ import annotations

import argparse
import shutil
import signal
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.clickhouse_io import read_spark_df, write_df
from common.config import settings
from common.flights import FLOW_FACT_COLUMNS, aggregate_exprs, score_windows
from common.pipeline_log import job_run
from common.spark_session import build_spark

CHECKPOINT = Path(settings.checkpoint_dir) / "br9_stream_arrivals"

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("flight_key", T.StringType()),
    T.StructField("airport", T.StringType()),
    T.StructField("city_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("scheduled_ts", T.StringType()),
    T.StructField("delay_min", T.IntegerType()),
    T.StructField("is_delayed", T.IntegerType()),
    T.StructField("carrier", T.StringType()),
    T.StructField("flight_number", T.StringType()),
    T.StructField("origin", T.StringType()),
    T.StructField("dest", T.StringType()),
    T.StructField("year_month", T.StringType()),
    T.StructField("produced_at", T.StringType()),
])


def make_batch_handler(airports: DataFrame, baseline: DataFrame):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            print(f"  [batch {batch_id}] empty")
            return
        windows = batch_df.join(F.broadcast(airports), "airport", "inner")
        scored = (score_windows(windows, baseline)
                  .withColumn("source", F.lit("stream"))
                  .cache())
        n = write_df(scored, "fact_airport_flow_15min", columns=FLOW_FACT_COLUMNS, max_write_partitions=1)

        hot = (scored.orderBy(F.desc("flow_severity"), F.desc("load_pct"))
               .select("airport", "window_local", "arrivals", "capacity", "load_pct", "alert_level")
               .first())
        span = scored.agg(F.min("window_ts").alias("a"), F.max("window_ts").alias("b")).first()
        print(f"  [batch {batch_id}] {n:>3} windows · {span['a']:%m-%d %H:%M} → {span['b']:%m-%d %H:%M} UTC · "
              f"highest {hot['airport']} {hot['window_local']:%H:%M} local time: "
              f"{hot['arrivals']} flights / {hot['capacity']:.0f} = {hot['load_pct']:.0f}% {hot['alert_level']}")
        scored.unpersist()

    return handle


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    ap.add_argument("--reset", action="store_true", help="delete checkpoint")
    ap.add_argument("--from-beginning", action="store_true", help="startingOffsets = earliest")
    ap.add_argument("--trigger-seconds", type=int, default=10)
    ap.add_argument("--watermark-minutes", type=int, default=30)
    args = ap.parse_args()

    if args.reset and CHECKPOINT.exists():
        shutil.rmtree(CHECKPOINT)
        print(f"Deleted checkpoint {CHECKPOINT}")

    with job_run("br9_05_stream_arrivals", br="BR9", step="Kafka → 15-minute windows → ClickHouse"):
        spark = build_spark("br9_05_stream_arrivals", local=args.local, shuffle_partitions=4)
        try:
            airports = read_spark_df(spark, """
                SELECT airport, timezone FROM tourism.dim_airport FINAL
            """).cache()
            baseline = read_spark_df(spark, """
                SELECT airport, toInt32(dow_local) AS dow_local, toInt32(slot_local) AS slot_local,
                       toFloat64(baseline_p50) AS baseline_p50, toFloat64(baseline_p95) AS baseline_p95
                FROM tourism.baseline_airport_slot FINAL
            """).cache()
            print(f"Airports: {airports.count()} · baseline slots: {baseline.count():,}")

            raw = (spark.readStream.format("kafka")
                   .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
                   .option("subscribe", settings.kafka_topic_flights)
                   .option("startingOffsets", "earliest" if args.from_beginning else "latest")
                   .option("failOnDataLoss", "false")
                   .option("maxOffsetsPerTrigger", 50_000)
                   .load())

            events = (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
                      .select("e.*")
                      .withColumn("event_ts", F.to_timestamp("event_ts"))
                      .filter(F.col("event_ts").isNotNull() & F.col("airport").isNotNull()))

            windowed = (events
                        .withWatermark("event_ts", f"{args.watermark_minutes} minutes")
                        .groupBy(F.window("event_ts", f"{settings.window_minutes} minutes").alias("w"),
                                 "airport", "city_id")
                        .agg(*aggregate_exprs())
                        .withColumn("window_ts", F.col("w.start"))
                        .drop("w"))

            print(f"\nNghe {settings.kafka_bootstrap} / {settings.kafka_topic_flights} · "
                  f"window {settings.window_minutes}' · watermark {args.watermark_minutes}' · "
                  f"trigger {args.trigger_seconds}s · Ctrl+C to stop\n")

            query = (windowed.writeStream
                     .outputMode("update")
                     .foreachBatch(make_batch_handler(airports, baseline))
                     .option("checkpointLocation", str(CHECKPOINT))
                     .trigger(processingTime=f"{args.trigger_seconds} seconds")
                     .start())





            stop_requested = {"flag": False}

            def _request_stop(signum, _frame):
                stop_requested["flag"] = True
                print(f"\nReceived signal {signal.Signals(signum).name} — stopping after the current micro-batch…")

            signal.signal(signal.SIGINT, _request_stop)
            signal.signal(signal.SIGTERM, _request_stop)

            while query.isActive and not stop_requested["flag"]:
                query.awaitTermination(5)

            if query.isActive:
                query.stop()
            print("Streaming stopped. Checkpoint retained; the next run resumes from the processed offset.")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
