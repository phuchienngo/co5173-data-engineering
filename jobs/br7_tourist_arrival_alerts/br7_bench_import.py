"""BR7 benchmark import: the 13 TLC files scripts/prepare_br7.py downloads ->
br7.raw_trip, and taxi_zone_lookup.csv -> br7.zone (docs/plan/br7/plan-benchmark.md, D1).

Each month is read with prepare.py's own load_month() (same columns and casts);
its quality label is dropped, because labelling is part of a run. Built in
br7_staging, checked, then swapped into br7 table by table, as BR8's import.

    spark-submit br7_bench_import.py [--replace] [--writer pandas] --settings-json '{...}'
"""

import argparse
import json
import os
import sys
import uuid

from pyspark.sql import functions as F

import br7_bench_runs as runs
import br7_bench_settings
import prepare
import run_figures
from clickhouse_io import get_client, write_partitions
from spark_session import get_spark_session

CATALOG = br7_bench_settings.CATALOG
RAW = os.path.join(prepare.DATA, "raw")
LOOKUP = "taxi_zone_lookup.csv"
TRIP_FILE = "yellow_tripdata_{}.parquet"
# Rows per insert: 50.7 M trips in 50k-row inserts would make ~1,000 parts to merge.
BATCH_ROWS = 200_000


def parse_args():
    parser = argparse.ArgumentParser(description="Import BR7 TLC data into ClickHouse")
    when = parser.add_mutually_exclusive_group()
    when.add_argument("--if-empty", action="store_true", help="Import only if no data is stored (default)")
    when.add_argument("--replace", action="store_true", help="Import and replace the stored data")
    parser.add_argument("--writer", choices=("partitions", "pandas"), help="Default: the writer setting")
    parser.add_argument("--run-id")
    parser.add_argument("--settings-json")
    parser.add_argument("--settings-label", default="current settings")
    return parser.parse_args()


def log(message):
    print(f"{runs.PREFIX} {message}", flush=True)


def write(df, table, writer, partitions):
    if writer == "partitions":
        # coalesce, not repartition: each file split streams into ClickHouse without a shuffle.
        write_partitions(df.coalesce(partitions), table, database=runs.STAGING_DB, batch_rows=BATCH_ROWS)
        return
    if table == "raw_trip":
        # 50.7 M rows through toPandas would need ~10 GB on the driver.
        raise ValueError("The pandas writer can't import the trips; use the partitions writer.")
    get_client(database=runs.STAGING_DB).insert_df(table, df.toPandas())


def prepare_staging(client):
    client.command(f"DROP DATABASE IF EXISTS {runs.STAGING_DB} SYNC")
    client.command(f"CREATE DATABASE {runs.STAGING_DB}")
    for table in runs.DATA_TABLES:
        client.command(f"CREATE TABLE {runs.STAGING_DB}.{table} AS {runs.DB}.{table}")


def swap(client):
    for table in runs.DATA_TABLES:
        client.command(f"EXCHANGE TABLES {runs.DB}.{table} AND {runs.STAGING_DB}.{table}")
    client.command(f"DROP DATABASE {runs.STAGING_DB} SYNC")


def verify(client, expected_months, expected_zones):
    """Stored rows per month equal what Spark read, and timestamps arrived unshifted."""
    problems = []
    stored = {r[0]: tuple(int(x) for x in r[1:]) for r in client.query(
        f"SELECT source_month, count(), toInt64(min(toUnixTimestamp64Micro(dropoff_local))), "
        f"toInt64(max(toUnixTimestamp64Micro(dropoff_local))), toInt64(sum(toUnixTimestamp64Micro(pickup_local) % 1000003)) "
        f"FROM {runs.STAGING_DB}.raw_trip GROUP BY source_month").result_rows}
    for month, figures in expected_months.items():
        if stored.get(month) != figures:
            problems.append(f"{month}: stored {stored.get(month)}, read {figures} (rows, dropoff range, pickup checksum)")
    zones = int(client.command(f"SELECT count() FROM {runs.STAGING_DB}.zone"))
    if zones != expected_zones:
        problems.append(f"zone: {zones} rows stored, {expected_zones} read")
    if problems:
        raise ValueError("Import check failed: " + "; ".join(problems))


def import_data(spark, client, run, writer, partitions):
    with run.step("read"):
        trips = None
        for month in runs.ALL_MONTHS:
            frame = prepare.load_month(spark, month).drop("quality")
            trips = frame if trips is None else trips.unionByName(frame)
        # A spread-out UInt32 per row for the parallel reader's slices.
        trips = trips.withColumn("source_row", F.pmod(F.xxhash64(*trips.columns), F.lit(2**32)).cast("long")) \
                     .withColumn("import_id", F.lit(run.row["import_id"]))
        lookup = spark.read.option("header", True).csv(os.path.join(RAW, LOOKUP)).select(
            F.col("LocationID").cast("int").alias("zone_id"), F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone_name"), F.col("service_zone").alias("service_zone"))
        micros = F.unix_micros
        expected = {r["source_month"]: (r["n"], r["lo"], r["hi"], r["ps"]) for r in trips.groupBy(
            "source_month").agg(F.count("*").alias("n"), F.min(micros("dropoff_local")).alias("lo"),
                                F.max(micros("dropoff_local")).alias("hi"),
                                F.sum(micros("pickup_local") % 1000003).alias("ps")).collect()}
        zones = lookup.count()
        total = sum(v[0] for v in expected.values())
        run.update(rows_read=total + zones, rows_expected=total + zones)
        log(f"read {total:,} trips in {len(expected)} months, {zones} zones")
        prepare_staging(client)

    with run.step("write"):
        write(lookup, "zone", writer, partitions)
        write(trips, "raw_trip", writer, partitions)

    with run.step("verify"):
        verify(client, expected, zones)
        run.update(rows_written=total + zones,
                   result_summary=json.dumps({"trips": total, "zones": zones,
                                              "trips_by_month": {m: v[0] for m, v in sorted(expected.items())}}))

    with run.step("swap"):
        swap(client)
    return total


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "raw_trip"):
        sys.exit("BR7 tables are missing: run `make br7-up`.")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(runs.busy_message(busy))
    names = [LOOKUP] + [TRIP_FILE.format(m) for m in runs.ALL_MONTHS]
    missing = [name for name in names if not os.path.exists(os.path.join(RAW, name))]
    if missing:
        sys.exit(f"Missing source files in {RAW}: {missing}. Run `python3 scripts/prepare_br7.py --download-only`.")
    if not args.replace:
        stored = runs.row_count(client, runs.DB, "raw_trip")
        if stored:
            print(f"BR7 data already imported ({stored:,} trips); nothing to do. Use --replace to re-import.")
            sys.exit(0)

    values = CATALOG.resolve(json.loads(args.settings_json) if args.settings_json
                             else runs.load_settings(client, runs.CURRENT))
    pipeline = CATALOG.options(values)["pipeline"]
    writer = args.writer or pipeline["writer"]
    values["pipeline.writer"] = writer
    cluster = runs.run_store.cluster_info()

    run = runs.Run(
        get_client(autogenerate_session_id=False), "import", run_id=args.run_id, import_id=str(uuid.uuid4()),
        writer=writer, criteria={"source": "NYC TLC yellow taxi"},
        settings=CATALOG.snapshot(values, cluster, label=args.settings_label),
        source_file=RAW, source_bytes=sum(os.path.getsize(os.path.join(RAW, n)) for n in names),
    )
    run.start()
    runs.stop_on_sigterm()
    log(f"import run {run.run_id} started, writer {writer}")
    spark = None
    ui = None
    try:
        conf = br7_bench_settings.spark_conf(values)
        run_figures.prepare_event_log(conf)
        spark = get_spark_session("BR7 import", conf=conf)
        spark.sparkContext.setLogLevel("WARN")
        ui = run_figures.spark_ui(spark)
        run.update(settings=CATALOG.snapshot(values, cluster, spark, args.settings_label), spark_app_id=ui[1])
        run.on_step = lambda name: spark.sparkContext.setJobGroup(name, name)
        import_data(spark, client, run, writer, pipeline["write_partitions"])
        run.finish("succeeded")
    except BaseException as exc:
        message = runs.failure_message(exc)
        log(f"import failed: {message}")
        try:
            client.command(f"DROP DATABASE IF EXISTS {runs.STAGING_DB} SYNC")
        finally:
            run.finish("failed", error=message[:4000])
        raise
    finally:
        run_figures.record(run, client, runs.STAGING_DB, ui)
        if spark is not None:
            spark.stop()
    log(f"import succeeded: {run.row['rows_written']:,} rows in {run.row['duration_ms'] / 1000:.1f}s")


if __name__ == "__main__":
    main()
