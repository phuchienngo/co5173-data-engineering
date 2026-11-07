import csv
import os
import re

import pandas as pd
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from clickhouse_io import truncate_table, write_dataframe
from spark_session import get_spark_session

# Matches the raw Inside Airbnb reviews.csv columns the producer sends
# (data/br3/reviews.csv.gz).
REVIEW_SCHEMA = StructType(
    [
        StructField("listing_id", StringType()),
        StructField("id", StringType()),
        StructField("comments", StringType()),
    ]
)

analyzer = SentimentIntensityAnalyzer()
EN_STOP_SHARE = 0.2


def sentiment_label(text, en_stop):
    words = re.findall(r"[a-z]+", (text or "").lower())
    if not words or sum(w in en_stop for w in words) / len(words) < EN_STOP_SHARE:
        return "non_english"
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"


spark = get_spark_session("br3_sentiment_streaming")
spark.sparkContext.setLogLevel("WARN")

# VADER only scores English. Reviews whose share of English stop words is
# below EN_STOP_SHARE are labelled non_english instead of being misread
# (e.g. German "die" scored as negative).
en_stop = set(StopWordsRemover.loadDefaultStopWords("english"))
sentiment_udf = F.udf(lambda text: sentiment_label(text, en_stop), StringType())

# Read listings with Python's csv module: Spark's multiLine CSV reader
# misaligns columns after free-text fields, which put listing descriptions
# into room_type and silently dropped ~1.6k of the 31,069 listings.
rows = []
with open("/opt/data/br3/listings.csv", newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        try:
            rows.append(
                {
                    "listing_id": row["id"],
                    "name": row["name"] or "",
                    "neighbourhood": row["neighbourhood_cleansed"],
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "room_type": row["room_type"],
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
dim_pdf = pd.DataFrame(rows)
del rows
# truncate first: insert_df only appends, so a restart would duplicate rows.
truncate_table("dim_airbnb_listing")
write_dataframe(dim_pdf, "dim_airbnb_listing")
dim_listing = spark.createDataFrame(dim_pdf[["listing_id"]])
del dim_pdf

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", os.environ.get("KAFKA_TOPIC", "reviews"))
    .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "earliest"))
    .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "500"))
    .load()
)

reviews = raw.select(
    F.from_json(F.col("value").cast("string"), REVIEW_SCHEMA).alias("data"),
    F.col("timestamp").alias("event_time"),
).select(
    F.col("data.id").alias("review_id"),
    F.col("data.listing_id").alias("listing_id"),
    F.col("data.comments").alias("review_text"),
    "event_time",
)

# Keyword extraction for the complaint-keyword panel: lowercase words of 3+
# letters, minus Spark's built-in English stop words.
tokenizer = RegexTokenizer(inputCol="review_text", outputCol="tokens", pattern="[a-z]{3,}", gaps=False)
remover = StopWordsRemover(inputCol="tokens", outputCol="keywords")

fact = (
    remover.transform(tokenizer.transform(reviews.na.fill({"review_text": ""})))
    .withColumn("sentiment_label", sentiment_udf(F.col("review_text")))
    .join(F.broadcast(dim_listing), "listing_id", "inner")
    .select("review_id", "listing_id", "sentiment_label", "keywords", "event_time")
)

fact.explain()


def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        write_dataframe(batch_df.toPandas(), "fact_reviews")


fact.writeStream.queryName("reviews").foreachBatch(write_batch).trigger(processingTime="5 seconds").start().awaitTermination()
