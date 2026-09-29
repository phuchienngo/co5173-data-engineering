"""Shared constants and pricing logic for TLC demand-ratio pipeline."""

import os
import re
import urllib.error
import urllib.parse
import urllib.request
from functools import reduce
from itertools import islice
from pyspark.sql import DataFrame, SparkSession, functions as F

# Data paths
DATA_DIR = "/opt/data"
RAW_DIR = f"{DATA_DIR}/raw"
FACT_PATH = f"{DATA_DIR}/lake/fact_trips"
CAPACITY_PATH = f"{DATA_DIR}/lake/dim_capacity"
CHECKPOINT_DIR = f"{DATA_DIR}/checkpoints/demand_ratio"

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "tlc.trips"

# ClickHouse
CH_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CH_TABLE = "pricing.demand_ratio_hourly"

# Pricing
SURGE_THRESHOLD_PCT = 80
SURGE_UPLIFT = 0.15
ACTION_SURGE = "Nhu cầu > 80%: Gợi ý tăng giá 15%"
ACTION_HOLD = "Giữ giá"

# TLC LocationID -> (zone name in taxi_zone_lookup.csv, landmark label)
LANDMARKS = {
    12: ("Battery Park", "Statue of Liberty ferry"),
    43: ("Central Park", "Central Park"),
    45: ("Chinatown", "Chinatown"),
    66: ("DUMBO/Vinegar Hill", "Brooklyn Bridge / DUMBO"),
    132: ("JFK Airport", "JFK Airport"),
    138: ("LaGuardia Airport", "LaGuardia Airport"),
    144: ("Little Italy/NoLiTa", "Little Italy"),
    161: ("Midtown Center", "Rockefeller Center"),
    186: ("Penn Station/Madison Sq West", "Madison Square Garden"),
    209: ("Seaport", "South Street Seaport"),
    211: ("SoHo", "SoHo"),
    230: ("Times Sq/Theatre District", "Times Square & Broadway"),
    234: ("Union Sq", "Union Square"),
    246: ("West Chelsea/Hudson Yards", "High Line & Hudson Yards"),
    249: ("West Village", "West Village"),
    261: ("World Trade Center", "9/11 Memorial & One WTC"),
}

# TLC service -> (pickup column, fare column)
SERVICE_COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "fare_amount"),
    "fhvhv": ("pickup_datetime", "base_passenger_fare")
}

CH_COLUMNS = [
    "window_start", "location_id", "zone", "landmark",
    "active_bookings", "avg_fare", "capacity",
    "demand_ratio_pct", "suggested_fare", "price_action"
]


def read_trips(spark, service, files):
    """Normalise TLC monthly parquet files of one service into the fact schema."""
    pickup_col, fare_col = SERVICE_COLUMNS[service]
    frames = []

    # TLC column types drift between months, so each file is read and cast separately instead of one multi-file read.
    for path in sorted(files):
        month = re.search(r"(\d{4}-\d{2})\.parquet$", path).group(1)
        frame = spark.read.parquet(path).select(
            F.lit(service).alias("service"),
            F.col(pickup_col).cast("timestamp").alias("pickup_datetime"),
            F.col("PULocationID").cast("int").alias("pu_location_id"),
            F.col("DOLocationID").cast("int").alias("do_location_id"),
            F.col(fare_col).cast("double").alias("fare_amount")
        ).where(
            F.date_format("pickup_datetime", "yyyy-MM") == month
        ).where(
            (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500)
        ).where(
            F.col("pu_location_id").between(1, 263) & F.col("do_location_id").between(1, 263)
        ).withColumn(
            "pickup_month", F.lit(month)
        )
        frames.append(frame)

    return reduce(DataFrame.unionByName, frames)


def with_slot_keys(df, ts_col):
    """Adds capacity join keys: day_type and hour_of_day."""
    day_type = F.when(F.dayofweek(ts_col).isin(1, 7), "weekend").otherwise("weekday")
    hour_of_day = F.hour(ts_col)
    return df.withColumn("day_type", day_type).withColumn("hour_of_day", hour_of_day)


def pricing_columns(df):
    """Adds demand_ratio_pct, price_action, and suggested_fare."""
    # multiply before dividing so 80/100 is exactly 80.0; null capacity -> null ratio
    ratio = F.when(F.col("capacity") > 0, F.col("active_bookings") * 100.0 / F.col("capacity"))
    surge = F.coalesce(ratio > SURGE_THRESHOLD_PCT, F.lit(False))
    return df.withColumn(
        "demand_ratio_pct", ratio
    ).withColumn(
        "price_action", F.when(surge, ACTION_SURGE).otherwise(ACTION_HOLD)
    ).withColumn(
        "suggested_fare", F.when(surge, F.col("avg_fare") * (1 + SURGE_UPLIFT)).otherwise(F.col("avg_fare"))
    )


def landmarks_df(spark):
    """Create DataFrame of landmark locations."""
    return spark.createDataFrame(
        [(i, zone, label) for i, (zone, label) in LANDMARKS.items()],
        "location_id int, zone string, landmark string"
    )


def ch_insert(json_lines, table=CH_TABLE, chunk=50_000):
    """POST JSON lines to ClickHouse's HTTP interface in chunks."""
    query = urllib.parse.urlencode({
        "query": f"INSERT INTO {table} FORMAT JSONEachRow",
        "date_time_input_format": "best_effort"
    })
    it = iter(json_lines)
    while batch := list(islice(it, chunk)):
        req = urllib.request.Request(
            f"{CH_URL}/?{query}",
            data="\n".join(batch).encode("utf-8"),
            headers={
                "X-ClickHouse-User": CH_USER,
                "X-ClickHouse-Key": CH_PASSWORD
            }
        )
        try:
            urllib.request.urlopen(req, timeout=120).read()
        except urllib.error.HTTPError as e:
            raise RuntimeError(e.read().decode()) from e


def write_ratios(hourly, capacity, source):
    """Joins hourly bookings with landmarks and capacity, adds pricing, inserts to ClickHouse."""
    rows = (with_slot_keys(hourly, "window_start")
            .join(F.broadcast(landmarks_df(hourly.sparkSession)), "location_id")
            .join(F.broadcast(capacity), ["location_id", "day_type", "hour_of_day"], "left"))
    rows = pricing_columns(rows).select(*CH_COLUMNS, F.lit(source).alias("source"))
    ch_insert(rows.toJSON().toLocalIterator())


if __name__ == "__main__":
    spark = SparkSession.builder.appName("br1-pricing-self-check").getOrCreate()
    df = spark.createDataFrame(
        [(80, 100, 10.0), (81, 100, 10.0), (801, 1000, 10.0), (5, None, 10.0), (3, 4, 20.0)],
        "active_bookings long, capacity int, avg_fare double"
    )
    got = {(r.active_bookings, r.capacity): r for r in pricing_columns(df).collect()}

    assert got[(80, 100)].demand_ratio_pct == 80.0
    assert got[(80, 100)].price_action == ACTION_HOLD
    assert got[(80, 100)].suggested_fare == 10.0

    assert got[(81, 100)].price_action == ACTION_SURGE
    assert abs(got[(81, 100)].suggested_fare - 11.5) < 1e-9

    assert got[(801, 1000)].price_action == ACTION_SURGE

    assert got[(5, None)].demand_ratio_pct is None
    assert got[(5, None)].price_action == ACTION_HOLD
    assert got[(5, None)].suggested_fare == 10.0

    assert got[(3, 4)].demand_ratio_pct == 75.0
    assert got[(3, 4)].price_action == ACTION_HOLD

    print("pricing self-check OK")
    spark.stop()
