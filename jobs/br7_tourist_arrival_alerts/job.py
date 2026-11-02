"""BR7 streaming: hourly taxi arrivals per tourist zone and next-hour warnings.

Kafka (one message per drop-off) -> 1-hour event-time windows with watermark
-> when a window closes, forecast the next hour against the zone's baseline
-> ClickHouse fact_br7_zone_hourly (idempotent on zone + hour).
"""

import csv
import glob
import os
import shutil
import uuid
from datetime import datetime, timezone

import clickhouse_connect
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from br7_rules import TOURIST_ZONES, forecast_rows
from spark_session import get_spark_session

DATA = os.environ.get("BR7_DATA_DIR", "/opt/data/br7")
TOPIC = "br7_taxi_dropoffs"
WATERMARK = "15 minutes"
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("dropoff_zone_id", IntegerType()),
    StructField("pickup_zone_id", IntegerType()),
    StructField("dropoff_time", StringType()),
    StructField("passenger_count", IntegerType()),
])
FACT_COLUMNS = [
    "zone_id", "observed_hour", "target_hour", "observed_arrivals", "observed_passengers",
    "usual_arrivals", "predicted_arrivals", "warning_threshold", "alert_level",
]


def clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
    )


def read_csv(name):
    files = glob.glob(f"{DATA}/prepared/{name}/part-*.csv")
    if len(files) != 1:
        raise ValueError(f"Expected one prepared {name} CSV; run scripts/prepare_br7.py")
    with open(files[0], newline="") as stream:
        return list(csv.DictReader(stream))


def load_dimensions():
    # Landmark labels come from the code so they can change without re-preparing data.
    zones = [[int(r["zone_id"]), r["zone_name"], r["borough"], TOURIST_ZONES[int(r["zone_id"])]]
             for r in read_csv("zones")]
    baseline_rows = read_csv("baseline")
    baseline = {
        (int(r["zone_id"]), int(r["hour_of_week"])): (float(r["mean_arrivals"]), float(r["p90_arrivals"]))
        for r in baseline_rows
    }
    client = clickhouse()
    try:
        client.command("TRUNCATE TABLE dim_br7_zones")
        client.insert("dim_br7_zones", zones, column_names=["zone_id", "zone_name", "borough", "landmark"])
        client.command("TRUNCATE TABLE dim_br7_zone_hour_baseline")
        client.insert(
            "dim_br7_zone_hour_baseline",
            [[int(r["zone_id"]), int(r["hour_of_week"]), float(r["mean_arrivals"]),
              float(r["p90_arrivals"]), int(r["training_hours"])] for r in baseline_rows],
            column_names=["zone_id", "hour_of_week", "mean_arrivals", "p90_arrivals", "training_hours"],
        )
    finally:
        client.close()
    return baseline


def reset_checkpoints():
    # Every demo run resets the topic and fact table, so old offsets are useless.
    # State-store commits chmod their files, which the Windows/WSL bind mount
    # under /opt/data rejects, so checkpoints live on the containers' own disk.
    # With one Spark worker, executors always reread their own state.
    root = os.environ.get("BR7_CHECKPOINT_ROOT", "/tmp/br7-checkpoints")
    shutil.rmtree(root, ignore_errors=True)
    return f"{root}/run-{uuid.uuid4().hex}"


def main():
    spark = get_spark_session("br7_tourist_arrival_alerts")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    baseline = load_dimensions()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.environ.get("BR7_MAX_OFFSETS_PER_TRIGGER", "50000"))
        .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")
        .withColumn("event_time", F.to_timestamp("dropoff_time"))
        .filter(F.col("event_time").isNotNull() & F.col("event_id").isNotNull())
        .filter(F.col("dropoff_zone_id").isin(*TOURIST_ZONES))
    )
    hourly = (
        events.withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", "1 hour"), F.col("dropoff_zone_id").alias("zone_id"))
        .agg(F.count("*").alias("arrivals"),
             F.sum(F.coalesce("passenger_count", F.lit(0))).alias("passengers"))
        .select("zone_id", F.col("window.start").cast("long").alias("observed_epoch"), "arrivals", "passengers")
    )

    def write_batch(batch, batch_id):
        # Closed windows only (append mode): at most zones x hours per batch.
        rows = [
            (r["zone_id"], datetime.fromtimestamp(r["observed_epoch"], timezone.utc), int(r["arrivals"]), int(r["passengers"]))
            for r in batch.collect()
        ]
        if rows:
            client = clickhouse()
            try:
                client.insert("fact_br7_zone_hourly", forecast_rows(rows, baseline), column_names=FACT_COLUMNS)
            finally:
                client.close()
        print(f"BR7_BATCH_COMMITTED {batch_id} closed_hours={len(rows)}", flush=True)

    query = (
        hourly.writeStream.outputMode("append").foreachBatch(write_batch)
        .option("checkpointLocation", reset_checkpoints())
        .trigger(processingTime="5 seconds").start()
    )
    print("BR7_STREAM_READY", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
