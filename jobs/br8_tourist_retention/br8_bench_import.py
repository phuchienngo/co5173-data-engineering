"""BR8 benchmark import: the TripWorld files prepare_br8.py downloads ->
br8.region, br8.place, br8.raw_checkin (docs/plan/br7/plan-benchmark.md, D1).

Built in br8_staging, checked, then swapped into br8 table by table, as BR6's
import. Nothing is cleaned or filtered here except what prepare_br8.py does
before its own cleaning: destination records only (r_h != r_o), c_o exploded.

    spark-submit br8_bench_import.py [--replace] [--writer pandas] --settings-json '{...}'
"""

import argparse
import json
import os
import sys
import uuid

from pyspark.sql import functions as F

import br8_bench_runs as runs
import br8_bench_settings
import run_figures
from clickhouse_io import get_client, write_partitions
from spark_session import get_spark_session

CATALOG = br8_bench_settings.CATALOG
RAW = os.path.join(os.environ.get("BR8_DATA_DIR", "/opt/data/br8"), "raw")
FILES = {
    "travel": "travel_behaviors.parquet",
    "place": "metadata/metadata_all.parquet",
    "region": "region_labels.parquet",
}
NULLABLE_FLOATS = {"latitude", "longitude"}


def parse_args():
    parser = argparse.ArgumentParser(description="Import BR8 TripWorld data into ClickHouse")
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


def slice_key(*columns):
    """A spread-out UInt32 per row for the parallel reader's slices."""
    return F.pmod(F.xxhash64(*columns), F.lit(2**32)).cast("long")


def write(df, table, writer, partitions):
    if writer == "partitions":
        # Hash on the slice key: a round-robin repartition sorts each partition first,
        # which ran a 512 MB executor out of memory on 2.76 M check-ins.
        keyed = df.repartition(partitions, "source_row") if "source_row" in df.columns else df.repartition(partitions)
        write_partitions(keyed, table, database=runs.STAGING_DB)
        return
    pdf = df.toPandas()  # baseline: every row through the driver
    for column in NULLABLE_FLOATS & set(pdf.columns):
        pdf[column] = pdf[column].astype(object).where(pdf[column].notna(), None)
    get_client(database=runs.STAGING_DB).insert_df(table, pdf)


def prepare_staging(client):
    client.command(f"DROP DATABASE IF EXISTS {runs.STAGING_DB} SYNC")
    client.command(f"CREATE DATABASE {runs.STAGING_DB}")
    for table in runs.DATA_TABLES:
        client.command(f"CREATE TABLE {runs.STAGING_DB}.{table} AS {runs.DB}.{table}")


def swap(client):
    for table in runs.DATA_TABLES:
        client.command(f"EXCHANGE TABLES {runs.DB}.{table} AND {runs.STAGING_DB}.{table}")
    client.command(f"DROP DATABASE {runs.STAGING_DB} SYNC")


def verify(client, expected):
    """Stored rows equal what Spark read, and timestamps arrived unshifted."""
    problems = []
    for table, (rows, extra) in expected.items():
        stored = int(client.command(f"SELECT count() FROM {runs.STAGING_DB}.{table}"))
        if stored != rows:
            problems.append(f"{table}: {stored:,} rows stored, {rows:,} expected")
        if extra:
            got = client.query(f"SELECT toInt64(min(toUnixTimestamp64Micro(visit_time))), "
                               f"toInt64(max(toUnixTimestamp64Micro(visit_time))) FROM {runs.STAGING_DB}.{table}"
                               ).result_rows[0]
            if tuple(got) != extra:
                problems.append(f"{table}: visit_time range {got} stored, {extra} read (time zone shift?)")
    if problems:
        raise ValueError("Import check failed: " + "; ".join(problems))


def import_data(spark, client, run, writer, partitions):
    with run.step("read"):
        travel = spark.read.parquet(os.path.join(RAW, FILES["travel"]))
        checkins = (
            travel.filter(F.col("r_h") != F.col("r_o"))
            .select(F.col("user_id").cast("long").alias("user_id"), F.col("r_o").alias("region_id"),
                    F.posexplode("c_o").alias("pos", "ci"))
            .select("region_id", "user_id",
                    F.col("ci.venue_id").alias("place_id"),
                    F.col("ci.ts").cast("timestamp").alias("visit_time"),
                    F.col("ci.venue_schema").alias("venue_schema"),
                    slice_key("user_id", "region_id", "pos").alias("source_row"),
                    F.lit(run.row["import_id"]).alias("import_id"))
        )  # not cached: re-reading the parquet is cheaper than holding it in a 512 MB executor
        metadata = spark.read.parquet(os.path.join(RAW, FILES["place"]))
        places = metadata.select(
            F.col("fsq_place_id").alias("place_id"),
            F.col("fsq_name").alias("fsq_name"),
            F.col("google_name").alias("google_name"),
            F.coalesce(F.col("fsq_category_labels"), F.array().cast("array<string>")).alias("categories"),
            F.col("fsq_latitude").cast("double").alias("latitude"),
            F.col("fsq_longitude").cast("double").alias("longitude"),
            slice_key("fsq_place_id").alias("source_row"),
        )
        regions = spark.read.parquet(os.path.join(RAW, FILES["region"])).select(
            "region_id", "city_name", "country_name")
        stats = checkins.agg(F.count("*").alias("n"),
                             F.min(F.unix_micros("visit_time")).alias("lo"),
                             F.max(F.unix_micros("visit_time")).alias("hi")).first()
        counts = {"raw_checkin": stats["n"], "place": places.count(), "region": regions.count()}
        distinct_places = places.select("place_id").distinct().count()
        if distinct_places != counts["place"]:
            raise ValueError(f"metadata_all.parquet repeats place ids ({counts['place'] - distinct_places})")
        run.update(rows_read=sum(counts.values()), rows_expected=sum(counts.values()))
        log(f"read {counts['raw_checkin']:,} destination check-ins, {counts['place']:,} places, "
            f"{counts['region']:,} regions")
        prepare_staging(client)

    with run.step("write"):
        write(regions, "region", writer, partitions)
        write(places, "place", writer, partitions)
        write(checkins, "raw_checkin", writer, partitions)

    with run.step("verify"):
        verify(client, {"region": (counts["region"], None), "place": (counts["place"], None),
                        "raw_checkin": (counts["raw_checkin"], (stats["lo"], stats["hi"]))})
        run.update(rows_written=sum(counts.values()),
                   result_summary=json.dumps({"checkins": counts["raw_checkin"], "places": counts["place"],
                                              "regions": counts["region"]}))

    with run.step("swap"):
        swap(client)
    return counts


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "raw_checkin"):
        sys.exit("BR8 tables are missing: run `make br8-up`.")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(runs.busy_message(busy))
    missing = [name for name in FILES.values() if not os.path.exists(os.path.join(RAW, name))]
    if missing:
        sys.exit(f"Missing source files in {RAW}: {missing}. Run `python3 scripts/prepare_br8.py --download-only`.")
    if not args.replace:
        stored = runs.row_count(client, runs.DB, "raw_checkin")
        if stored:
            print(f"BR8 data already imported ({stored:,} check-ins); nothing to do. Use --replace to re-import.")
            sys.exit(0)

    values = CATALOG.resolve(json.loads(args.settings_json) if args.settings_json
                             else runs.load_settings(client, runs.CURRENT))
    pipeline = CATALOG.options(values)["pipeline"]
    writer = args.writer or pipeline["writer"]
    values["pipeline.writer"] = writer
    cluster = runs.run_store.cluster_info()

    run = runs.Run(
        get_client(autogenerate_session_id=False), "import", run_id=args.run_id, import_id=str(uuid.uuid4()),
        writer=writer, criteria={"source": "TripWorld"},
        settings=CATALOG.snapshot(values, cluster, label=args.settings_label),
        source_file=RAW, source_bytes=sum(os.path.getsize(os.path.join(RAW, n)) for n in FILES.values()),
    )
    run.start()
    runs.stop_on_sigterm()
    log(f"import run {run.run_id} started, writer {writer}")
    spark = None
    ui = None
    try:
        conf = br8_bench_settings.spark_conf(values)
        run_figures.prepare_event_log(conf)
        spark = get_spark_session("BR8 import", conf=conf)
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
