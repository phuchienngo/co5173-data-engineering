import os

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from bucket import bucket_revenue
from clickhouse_io import truncate_table, write_dataframe
from spark_session import get_spark_session

BOOKING_SCHEMA = StructType(
    [
        StructField("listing_id", StringType()),
        StructField("id", StringType()),
        StructField("revenue", StringType()),
        StructField("nights_booked", StringType()),
    ]
)

bucket_udf = F.udf(bucket_revenue, StringType())

spark = get_spark_session("br4_revenue_streaming")
spark.sparkContext.setLogLevel("WARN")

dim_pricing = (
    spark.read.csv("/opt/data/br4/Dim_Property.csv", header=True)
    .select(
        F.col("Property_ID").alias("listing_id"),
        F.col("Location_City").alias("neighbourhood"),
        F.col("Room_Type").alias("room_type"),
        F.col("Price_per_Night").cast("double").alias("price"),
        F.lit(1).cast("int").alias("minimum_nights"),
    )
    .na.drop()
)
truncate_table("dim_listing_pricing")
write_dataframe(dim_pricing.toPandas(), "dim_listing_pricing")

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", os.environ.get("KAFKA_TOPIC", "bookings"))
    .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "earliest"))
    .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "500"))
    .load()
)

bookings = raw.select(
    F.from_json(F.col("value").cast("string"), BOOKING_SCHEMA).alias("data"),
    F.col("timestamp").alias("event_time"),
).select(
    F.col("data.id").alias("booking_id"),
    F.col("data.listing_id").alias("listing_id"),
    F.col("data.revenue").cast("double").alias("event_revenue"),
    F.col("data.nights_booked").cast("int").alias("nights_booked"),
    "event_time",
)

fact = (
    bookings.join(F.broadcast(dim_pricing), "listing_id", "inner")
    .withColumn(
        "revenue",
        F.coalesce(
            F.col("event_revenue"),
            F.col("price") * F.coalesce(F.col("nights_booked"), F.lit(1)),
        ),
    )
    .withColumn("revenue_bucket", bucket_udf(F.col("revenue")))
    .select("booking_id", "listing_id", "revenue", "revenue_bucket", "event_time")
)

fact.explain()


def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        write_dataframe(batch_df.toPandas(), "fact_bookings")


fact.writeStream.queryName("bookings").foreachBatch(write_batch).trigger(processingTime="5 seconds").start().awaitTermination()
