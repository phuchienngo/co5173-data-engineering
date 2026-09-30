#!/usr/bin/env python3
"""Build airport and destination dimensions."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.clickhouse_io import write_df
from common.config import settings
from common.destinations import DESTINATIONS, monitored_airports
from common.pipeline_log import job_run
from common.sources import read_airports
from common.spark_session import build_spark

AIRPORT_COLUMNS = ["airport", "city_id", "airport_name", "bts_city_name", "bts_city_market_id",
                   "latitude", "longitude", "timezone", "arrivals_per_day"]
DEST_COLUMNS = ["city_id", "city_name", "state", "timezone", "airbnb_path",
                "airbnb_snapshots", "airports", "latitude", "longitude"]


def build_dim_airport(spark: SparkSession) -> DataFrame:
    mon = monitored_airports()
    airports = read_airports(spark).filter(F.col("iata").isin(list(mon)))
    missing = set(mon) - {r["iata"] for r in airports.select("iata").collect()}
    if missing:
        raise ValueError(f"OpenFlights does not include airport {sorted(missing)} — cannot determine time zone.")

    city = F.create_map(*[x for kv in mon.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    dim = airports.select(
        F.col("iata").alias("airport"),
        city[F.col("iata")].alias("city_id"),
        "airport_name", "latitude", "longitude", "timezone")

    flights_path = f"{settings.lake_dir}/flights"
    if os.path.exists(flights_path):
        stats = (spark.read.parquet(flights_path)
                 .filter(F.col("Dest").isin(list(mon)))
                 .groupBy(F.col("Dest").alias("airport"))
                 .agg(F.first("DestCityName").alias("bts_city_name"),
                      F.first("DestCityMarketID").alias("bts_city_market_id"),
                      (F.count("*") / F.countDistinct("FlightDate")).alias("arrivals_per_day")))
        dim = dim.join(stats, "airport", "left")
    else:
        dim = (dim.withColumn("bts_city_name", F.lit(""))
                  .withColumn("bts_city_market_id", F.lit(0))
                  .withColumn("arrivals_per_day", F.lit(0.0)))

    return (dim
            .withColumn("bts_city_name", F.coalesce("bts_city_name", F.lit("")))
            .withColumn("bts_city_market_id", F.coalesce(F.col("bts_city_market_id").cast("long"), F.lit(0)))
            .withColumn("arrivals_per_day", F.round(F.coalesce("arrivals_per_day", F.lit(0.0)), 1)))


def build_dim_destination(spark: SparkSession, dim_airport: DataFrame) -> DataFrame:
    rows = [(d["city_id"], d["city_name"], d["state"], d["timezone"], d["airbnb_path"],
             sorted(d["airbnb_snapshots"]), d["airports"]) for d in DESTINATIONS]
    df = spark.createDataFrame(rows, "city_id string, city_name string, state string, timezone string, "
                                     "airbnb_path string, snaps array<string>, airports array<string>")
    centre = dim_airport.groupBy("city_id").agg(F.avg("latitude").alias("latitude"),
                                                 F.avg("longitude").alias("longitude"))
    return (df.join(centre, "city_id", "left")
              .withColumn("airbnb_snapshots", F.transform("snaps", lambda s: F.to_date(s)))
              .drop("snaps"))


def write_dims(spark: SparkSession) -> int:
    dim_airport = build_dim_airport(spark).cache()
    n = write_df(dim_airport, "dim_airport", columns=AIRPORT_COLUMNS, max_write_partitions=1)
    dim_dest = build_dim_destination(spark, dim_airport)
    n += write_df(dim_dest, "dim_destination", columns=DEST_COLUMNS, max_write_partitions=1)

    print("\n  Monitored airports:")
    for r in dim_airport.orderBy("city_id", "airport").collect():
        print(f"    {r['airport']}  {r['city_id']:14s} {r['timezone']:22s} "
              f"{r['bts_city_name'] or '(BTS not loaded yet)':24s} {r['arrivals_per_day']:>6} arrivals/day")
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("br9_01_dim", br="BR9", step="dim_destination + dim_airport") as run:
        spark = build_spark("br9_01_dim", local=args.local, shuffle_partitions=4)
        run.rows = write_dims(spark)
        spark.stop()


if __name__ == "__main__":
    main()
