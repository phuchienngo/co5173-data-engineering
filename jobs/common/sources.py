"""Shared Spark source readers."""

from __future__ import annotations

import glob
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import settings

# ══════════════════════════════════════════════════════════════════════
#  BTS On-Time Performance
# ══════════════════════════════════════════════════════════════════════



BTS_INT_COLS = [
    "OriginCityMarketID", "DestCityMarketID",
    "CRSDepTime", "DepTime", "CRSArrTime", "ArrTime",
    "DepDel15", "ArrDel15", "Cancelled", "Diverted",
]
BTS_DOUBLE_COLS = [
    "DepDelay", "ArrDelay", "CRSElapsedTime", "ActualElapsedTime", "Distance",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]
BTS_STR_COLS = [
    "FlightDate", "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline",
    "Origin", "OriginCityName", "Dest", "DestCityName", "CancellationCode",
]


def bts_csv_files() -> list[str]:
    return sorted(glob.glob(f"{settings.raw_dir}/bts/csv/ontime_*.csv"))


def read_bts_csv(spark: SparkSession, paths: list[str] | None = None) -> DataFrame:
    """Read bts csv."""
    paths = paths or bts_csv_files()
    if not paths:
        raise FileNotFoundError(
            f"No CSV files found at {settings.raw_dir}/bts/csv/ — run ./scripts/run_job.sh bts")

    raw = (spark.read
           .option("header", True)
           .option("inferSchema", False)
           .csv(paths)
           .withColumn("source_file", F.input_file_name()))

    cols = [F.col(c).cast("int").alias(c) for c in BTS_INT_COLS]
    cols += [F.col(c).cast("double").alias(c) for c in BTS_DOUBLE_COLS]
    cols += [F.trim(F.col(c)).alias(c) for c in BTS_STR_COLS]
    df = raw.select(*cols, "source_file")
    return df.withColumn("FlightDate", F.to_date("FlightDate"))


# ══════════════════════════════════════════════════════════════════════
#  OpenFlights airports.dat
# ══════════════════════════════════════════════════════════════════════

AIRPORTS_SCHEMA = T.StructType([
    T.StructField("of_id", T.IntegerType()),
    T.StructField("airport_name", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("iata", T.StringType()),
    T.StructField("icao", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("altitude_ft", T.IntegerType()),
    T.StructField("utc_offset", T.StringType()),
    T.StructField("dst", T.StringType()),
    T.StructField("timezone", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("source", T.StringType()),
])





AIRPORT_OVERRIDES = [
    ("BIH", "Eastern Sierra Regional Airport", "Bishop", 37.3731, -118.3636, "America/Los_Angeles"),
    ("EAR", "Kearney Regional Airport", "Kearney", 40.7270, -99.0068, "America/Chicago"),
    ("XWA", "Williston Basin International Airport", "Williston", 48.2584, -103.7486, "America/Chicago"),
]


def read_airports(spark: SparkSession) -> DataFrame:
    """Read airports."""
    path = f"{settings.raw_dir}/reference/airports.dat"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing {path} — run ./scripts/run_job.sh airports")
    df = (spark.read.schema(AIRPORTS_SCHEMA)
          .option("nullValue", "\\N")
          .option("quote", '"')
          .csv(path))
    base = (df.filter(F.length("iata") == 3)
              .filter(F.col("timezone").contains("/"))
              .dropDuplicates(["iata"])
              .select("iata", "airport_name", "city", "country",
                      "latitude", "longitude", "timezone"))
    extra = spark.createDataFrame(
        [(i, n, c, "United States", la, lo, tz) for i, n, c, la, lo, tz in AIRPORT_OVERRIDES],
        "iata string, airport_name string, city string, country string, "
        "latitude double, longitude double, timezone string")
    return base.unionByName(extra.join(base.select("iata"), "iata", "left_anti"))


# ══════════════════════════════════════════════════════════════════════
#  Inside Airbnb
# ══════════════════════════════════════════════════════════════════════

def airbnb_path(city_id: str, snapshot: str, name: str) -> str:
    return f"{settings.raw_dir}/airbnb/{city_id}/{snapshot}/{name}"


def _read_airbnb_csv(spark: SparkSession, path: str) -> DataFrame:
    """ read airbnb csv."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing {path} — run ./scripts/run_job.sh airbnb")
    return (spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .csv(path))


def read_listings(spark: SparkSession, city_id: str, snapshot: str) -> DataFrame:
    df = _read_airbnb_csv(spark, airbnb_path(city_id, snapshot, "listings.csv.gz"))
    return df.select(
        F.col("id").cast("long").alias("listing_id"),
        F.lit(city_id).alias("city_id"),
        F.to_date(F.lit(snapshot)).alias("snapshot_date"),
        F.col("neighbourhood_cleansed").alias("neighbourhood"),
        F.col("neighbourhood_group_cleansed").alias("neighbourhood_group"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("room_type"),
        F.col("accommodates").cast("int").alias("accommodates"),
        F.regexp_replace("price", r"[$,]", "").cast("double").alias("price"),
        F.col("minimum_nights").cast("int").alias("minimum_nights"),
        F.col("availability_365").cast("int").alias("availability_365"),
        F.col("number_of_reviews").cast("int").alias("number_of_reviews"),
        F.col("number_of_reviews_ltm").cast("int").alias("number_of_reviews_ltm"),
    ).filter(F.col("listing_id").isNotNull())


def read_calendar(spark: SparkSession, city_id: str, snapshot: str) -> DataFrame:
    df = _read_airbnb_csv(spark, airbnb_path(city_id, snapshot, "calendar.csv.gz"))
    return df.select(
        F.col("listing_id").cast("long").alias("listing_id"),
        F.lit(city_id).alias("city_id"),
        F.to_date(F.lit(snapshot)).alias("snapshot_date"),
        F.to_date("date").alias("night_date"),
        (F.col("available") == "f").cast("int").alias("is_unavailable"),
    ).filter(F.col("listing_id").isNotNull() & F.col("night_date").isNotNull())


def read_reviews(spark: SparkSession, city_id: str, snapshot: str) -> DataFrame:
    """Read reviews."""
    df = _read_airbnb_csv(spark, airbnb_path(city_id, snapshot, "reviews.csv.gz"))
    return df.select(
        F.col("id").cast("long").alias("review_id"),
        F.col("listing_id").cast("long").alias("listing_id"),
        F.col("reviewer_id").cast("long").alias("reviewer_id"),
        F.to_date("date").alias("review_date"),
        F.lit(city_id).alias("city_id"),
    ).filter(F.col("review_id").isNotNull()
             & F.col("reviewer_id").isNotNull()
             & F.col("review_date").isNotNull())


def read_all(spark: SparkSession, reader, snapshots: str = "all") -> DataFrame:
    """Read all."""
    from common.destinations import DESTINATIONS, latest_snapshot

    frames = []
    for d in DESTINATIONS:
        snaps = [latest_snapshot(d)] if snapshots == "latest" else sorted(d["airbnb_snapshots"])
        for s in snaps:
            frames.append(reader(spark, d["city_id"], s))
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out
