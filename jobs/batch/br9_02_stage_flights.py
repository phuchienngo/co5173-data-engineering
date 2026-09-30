#!/usr/bin/env python3
"""Normalize BTS flights and derive monitored-airport events."""

from __future__ import annotations

import argparse
import os
import re

from pyspark.sql import functions as F

from batch.br9_01_dim import write_dims
from common.config import settings
from common.destinations import monitored_airports
from common.flights import add_utc_times, build_events
from common.pipeline_log import job_run
from common.sources import bts_csv_files, read_airports, read_bts_csv
from common.spark_session import build_spark

FLIGHT_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline",
    "Origin", "OriginCityName", "OriginCityMarketID", "Dest", "DestCityName", "DestCityMarketID",
    "CRSDepTime", "DepTime", "DepDelay", "DepDel15", "CRSArrTime", "ArrTime", "ArrDelay", "ArrDel15",
    "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "Distance",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "origin_tz", "dest_tz", "sched_dep_utc", "dep_utc", "sched_arr_utc", "arr_utc",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", help="comma-separated YYYY-MM values (default: all CSV files)")
    ap.add_argument("--events-only", action="store_true")
    ap.add_argument("--delete-csv", action="store_true")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    flights_dir = f"{settings.lake_dir}/flights"
    events_dir = f"{settings.lake_dir}/flight_events"
    mon = monitored_airports()

    with job_run("br9_02_stage_flights", br="BR9", step="BTS CSV → UTC Parquet → events") as run:
        spark = build_spark("br9_02_stage_flights", local=args.local, shuffle_partitions=12)
        airports = read_airports(spark).cache()

        if not args.events_only:
            files = bts_csv_files()
            if args.months:
                wanted = {m.strip().replace("-", "_") for m in args.months.split(",")}
                files = [f for f in files if re.search(r"ontime_(\d{4}_\d{2})\.csv$", f).group(1) in wanted]
            if not files:
                raise FileNotFoundError("No BTS CSV files found — run ./scripts/run_job.sh bts")
            print(f"  Reading {len(files)} file CSV:")
            for f in files:
                print(f"    {os.path.basename(f)}")

            flights = add_utc_times(read_bts_csv(spark, files), airports)
            flights = (flights.select(*FLIGHT_COLUMNS)
                       .withColumn("year_month", F.date_format("FlightDate", "yyyy-MM")))



            (flights.repartition("year_month")
                    .write.mode("overwrite").partitionBy("year_month").parquet(flights_dir))
            print(f"  ✓ Parquet: {flights_dir}")

            if args.delete_csv:
                for f in files:
                    os.remove(f)
                print(f"  ✓ Deleted {len(files)} CSV files (original ZIPs remain in raw/bts/zip)")

        flights = spark.read.parquet(flights_dir)
        if args.months:
            flights = flights.filter(F.col("year_month").isin([m.strip() for m in args.months.split(",")]))



        no_tz = flights.filter((F.col("Dest").isin(list(mon)) & F.col("origin_tz").isNull())
                               | (F.col("Origin").isin(list(mon)) & F.col("origin_tz").isNull())).count()
        if no_tz:
            print(f"  ⚠️ {no_tz:,} flights touching monitored airports lack an origin time zone and are skipped")

        events = build_events(flights, mon)
        (events.repartition("year_month")
               .write.mode("overwrite").partitionBy("year_month").parquet(events_dir))

        summary = (spark.read.parquet(events_dir)
                   .groupBy("year_month").pivot("event_type").count()
                   .orderBy("year_month").fillna(0))
        print(f"\n  ✓ Events: {events_dir}")
        summary.show(30, truncate=False)
        run.rows = spark.read.parquet(events_dir).count()


        write_dims(spark)
        spark.stop()


if __name__ == "__main__":
    main()
