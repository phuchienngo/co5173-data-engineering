"""Consume BR8 daily visits and expose repeat-visit retention in ClickHouse."""

import os
from functools import partial

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from clickhouse_io import get_client
from sink import write_partition
from spark_session import get_spark_session

SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("user_id", LongType()),
    StructField("place_id", StringType()), StructField("visit_date", StringType()),
    StructField("visit_time", StringType()), StructField("raw_checkins", LongType()),
])


def main():
    spark = get_spark_session("br8_tourist_retention")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    data = os.environ.get("BR8_DATA_DIR", "/opt/data/br8")
    places = spark.read.option("header", True).csv(f"{data}/prepared/places")
    columns = ["place_id", "place_name", "category", "region_id", "city_name", "country_name", "latitude", "longitude"]
    # Preparation defaults to 20 sites. Null coordinates do not invalidate visits.
    dim_rows = []
    for row in places.select(*columns).toLocalIterator():
        dim_rows.append([*(row[c] or "" for c in columns[:6]), *(
            float(row[c]) if row[c] is not None else None for c in columns[6:]
        )])
    if not dim_rows:
        raise ValueError("BR8 has no prepared place dimensions")
    client = get_client()
    try:
        client.command("TRUNCATE TABLE dim_br8_places")
        client.insert("dim_br8_places", dim_rows, column_names=columns)
    finally:
        client.close()
    raw = spark.readStream.format("kafka").option(
        "kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    ).option("subscribe", "br8_tourist_visits").option("startingOffsets", "earliest").option(
        "maxOffsetsPerTrigger", "2000"
    ).load()
    parsed = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("event")).select("event.*")
    parsed = parsed.withColumn("visit_time", F.to_timestamp("visit_time")).withColumn("visit_date", F.to_date("visit_date"))
    expected = F.sha2(F.concat_ws("|", "user_id", "place_id", "visit_date"), 256)
    valid = parsed.filter(
        (F.col("user_id") > 0) & (F.col("raw_checkins") > 0)
        & (F.col("event_id") == expected)
        & (F.col("visit_date") == F.to_date("visit_time"))
    ).join(F.broadcast(places.select("place_id")), "place_id", "inner")

    # Executor containers do not inherit the driver's environment variables.
    connection = {
        "host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        "port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        "username": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
        "database": os.environ.get("CLICKHOUSE_DB", "default"),
    }

    def write_batch(batch, batch_id):
        batch.dropDuplicates(["event_id"]).foreachPartition(partial(write_partition, config=connection))
        print(f"BR8_BATCH_COMMITTED {batch_id}", flush=True)

    query = valid.writeStream.foreachBatch(write_batch).option(
        "checkpointLocation", os.environ.get("BR8_CHECKPOINT_DIR", f"{data}/checkpoints/stream")
    ).trigger(processingTime="5 seconds").start()
    print("BR8_STREAM_READY", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
