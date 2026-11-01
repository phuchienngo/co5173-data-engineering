import os

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from clickhouse_io import truncate_table, write_dataframe
from spark_session import get_spark_session

# Matches the local demo review event shape. The producer derives these
# fields from data/br3/listings.csv and sends them to Kafka.
REVIEW_SCHEMA = StructType(
    [
        StructField("listing_id", StringType()),
        StructField("id", StringType()),
        StructField("comments", StringType()),
    ]
)

analyzer = SentimentIntensityAnalyzer()


def sentiment_label(text):
    score = analyzer.polarity_scores(text or "")["compound"]
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"


sentiment_udf = F.udf(sentiment_label, StringType())

spark = get_spark_session("br3_sentiment_streaming")
spark.sparkContext.setLogLevel("WARN")

# multiLine: some free-text columns contain literal newlines inside quotes,
# which otherwise splits those rows apart and misaligns columns. No
# inferSchema: cast() below returns null instead of crashing on the columns
# that are still misaligned, and .na.drop() removes those rows.
dim_listing = (
    spark.read.csv("/opt/data/br3/listings.csv", header=True, multiLine=True)
    .select(
        F.col("id").alias("listing_id"),
        "name",
        F.col("neighbourhood_cleansed").alias("neighbourhood"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        "room_type",
    )
    .na.drop()
)
# truncate first: insert_df only appends, so a restart would duplicate rows.
truncate_table("dim_airbnb_listing")
write_dataframe(dim_listing.toPandas(), "dim_airbnb_listing")

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", "reviews")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

reviews = raw.select(
    F.from_json(F.col("value").cast("string"), REVIEW_SCHEMA).alias("data")
).select(
    F.col("data.id").alias("review_id"),
    F.col("data.listing_id").alias("listing_id"),
    F.col("data.comments").alias("review_text"),
)

fact = (
    reviews.withColumn("sentiment_label", sentiment_udf(F.col("review_text")))
    .join(F.broadcast(dim_listing.select("listing_id")), "listing_id", "inner")
    .select("review_id", "listing_id", "sentiment_label")
)

fact.explain()


def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        write_dataframe(batch_df.toPandas(), "fact_reviews")


fact.writeStream.foreachBatch(write_batch).trigger(processingTime="5 seconds").start().awaitTermination()
