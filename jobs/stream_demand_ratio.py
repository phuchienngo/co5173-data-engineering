"""BR1 streaming: Kafka trips -> hourly demand ratio per landmark -> ClickHouse pricing suggestions."""
# env MAX_OFFSETS = trips per 10 s micro-batch (default 20000); it is the replay-speed knob.
import os
from pyspark.sql import SparkSession, functions as F
from common import CAPACITY_PATH, CHECKPOINT_DIR, KAFKA_BOOTSTRAP, LANDMARKS, TOPIC, write_ratios

TRIP_SCHEMA = "service string, pickup_datetime timestamp, pu_location_id int, do_location_id int, fare_amount double"


def main():
    # cap at 1 core so batch/publish jobs can still run on the 2-core worker while the stream is live
    spark = SparkSession.builder.appName("br1-stream-demand-ratio").config("spark.cores.max", "1").getOrCreate()

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
        hourly.writeStream.outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .foreachBatch(lambda batch, _batch_id: write_ratios(batch, capacity, "stream"))
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
