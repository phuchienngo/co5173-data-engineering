"""BR5 import: hotel_bookings.csv -> br5.raw_booking + star schema.

Plan: docs/plan/br5/plan-data-import.md. Everything is built in the
br5_staging database, checked, then swapped into br5 table by table, so the
live data is untouched until the new data is verified.

Run by start.py with no arguments (= --if-empty), by scripts/br5.py and by the
BR5 app.
"""

import argparse
import json
import os
import sys
import uuid

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

import run_settings
import runs
import source
import star
from clickhouse_io import get_client, write_partitions
from spark_session import get_spark_session

MODE = "batch"

WRITERS = ("partitions", "pandas")
# Nullable integer columns: toPandas turns them into floats with NaN, which
# ClickHouse integer columns don't accept.
NULLABLE_INTS = {"children", "agent", "company", "agent_id", "company_id"}


def parse_args():
    parser = argparse.ArgumentParser(description="Import BR5 hotel bookings into ClickHouse")
    when = parser.add_mutually_exclusive_group()
    when.add_argument("--if-empty", action="store_true", help="Import only if no data is stored (default)")
    when.add_argument("--replace", action="store_true", help="Import and replace the stored data")
    parser.add_argument("--multiplier", type=int, default=1, help="Repeat the dataset N times (synthetic data)")
    parser.add_argument("--writer", choices=WRITERS, help="Default: the writer setting")
    parser.add_argument("--write-partitions", type=int, help="Spark partitions used to write. Default: the setting")
    parser.add_argument("--run-id", help="Run id to record (the app passes its own)")
    parser.add_argument("--source", default=source.SOURCE_FILE)
    parser.add_argument("--settings-json", help="Resolved settings (launch.py passes them); default: the current "
                                                "settings stored in br5.settings")
    parser.add_argument("--settings-label", default="current settings", help="Where the settings came from")
    args = parser.parse_args()
    if args.multiplier < 1:
        parser.error("--multiplier must be at least 1")
    return args


def write(df, table, writer, partitions):
    if writer == "partitions":
        write_partitions(df.repartition(partitions), table, database=runs.STAGING_DB)
        return
    # Baseline: every row goes through the driver.
    pdf = df.toPandas()
    for column in NULLABLE_INTS & set(pdf.columns):
        values = pdf[column].astype("Int64").astype(object)
        pdf[column] = values.where(values.notna(), None)
    get_client(database=runs.STAGING_DB).insert_df(table, pdf)


def prepare_staging(client):
    client.command(f"DROP DATABASE IF EXISTS {runs.STAGING_DB} SYNC")
    client.command(f"CREATE DATABASE {runs.STAGING_DB}")
    for table in runs.DATA_TABLES:
        client.command(f"CREATE TABLE {runs.STAGING_DB}.{table} AS {runs.DB}.{table}")


def verify(client, rows_expected):
    s = runs.STAGING_DB
    checks = client.query(f"""
        SELECT
            (SELECT count() FROM {s}.raw_booking) AS raw_rows,
            (SELECT count() FROM {s}.fact_booking) AS fact_rows,
            (SELECT countIf(arrival_date_key NOT IN (SELECT date_key FROM {s}.dim_date)
                         OR booking_date_key NOT IN (SELECT date_key FROM {s}.dim_date)
                         OR status_date_key NOT IN (SELECT date_key FROM {s}.dim_date)
                         OR hotel_key NOT IN (SELECT hotel_key FROM {s}.dim_hotel)
                         OR country_key NOT IN (SELECT country_key FROM {s}.dim_country)
                         OR reserved_room_type_key NOT IN (SELECT room_type_key FROM {s}.dim_room_type)
                         OR assigned_room_type_key NOT IN (SELECT room_type_key FROM {s}.dim_room_type)
                         OR channel_key NOT IN (SELECT channel_key FROM {s}.dim_channel)
                         OR profile_key NOT IN (SELECT profile_key FROM {s}.dim_booking_profile))
             FROM {s}.fact_booking) AS orphan_keys,
            (SELECT countIf(country IS NULL) FROM {s}.raw_booking) AS raw_no_country,
            (SELECT countIf(country_key = 0) FROM {s}.fact_booking) AS fact_unknown_country,
            (SELECT sum(adr) FROM {s}.raw_booking) AS raw_adr,
            (SELECT sum(adr) FROM {s}.fact_booking) AS fact_adr,
            (SELECT sum(stays_in_weekend_nights + stays_in_week_nights) FROM {s}.raw_booking) AS raw_nights,
            (SELECT sum(stay_nights) FROM {s}.fact_booking) AS fact_nights,
            (SELECT countIf(is_duplicate = 1) FROM {s}.fact_booking) AS duplicates
    """).first_item
    print(f"[br5] checks: {checks}", flush=True)
    problems = []
    if checks["raw_rows"] != rows_expected or checks["fact_rows"] != rows_expected:
        problems.append(f"row counts raw={checks['raw_rows']} fact={checks['fact_rows']}, expected {rows_expected}")
    if checks["orphan_keys"]:
        problems.append(f"{checks['orphan_keys']} fact rows point to a missing dimension row")
    if checks["raw_no_country"] != checks["fact_unknown_country"]:
        problems.append(
            f"missing countries raw={checks['raw_no_country']} vs Unknown in fact={checks['fact_unknown_country']}"
        )
    if abs(checks["raw_adr"] - checks["fact_adr"]) > 1e-6 * max(1.0, abs(checks["raw_adr"])):
        problems.append(f"sum(adr) raw={checks['raw_adr']} fact={checks['fact_adr']}")
    if checks["raw_nights"] != checks["fact_nights"]:
        problems.append(f"nights raw={checks['raw_nights']} fact={checks['fact_nights']}")
    if problems:
        raise RuntimeError("Import checks failed: " + "; ".join(problems))
    return checks


def swap(client):
    for table in runs.DATA_TABLES:
        client.command(f"EXCHANGE TABLES {runs.DB}.{table} AND {runs.STAGING_DB}.{table}")
    client.command(f"DROP DATABASE {runs.STAGING_DB} SYNC")


def import_data(spark, client, run, args):
    import_id = str(uuid.uuid4())
    run.update(import_id=import_id)

    with run.step("read"):
        source.check_file(args.source)
        text = source.read_text(spark, args.source).cache()
        rows_read = text.count()
        run.update(rows_read=rows_read, rows_expected=rows_read * args.multiplier)

    with run.step("check"):
        problems = source.problem_counts(text)
        if problems:
            raise source.SourceError(f"Values that can't be stored as-is, per column: {problems}")

    with run.step("derive"):
        # Row number in the file, in file order (1 = first data row).
        numbered = text.rdd.zipWithIndex().map(lambda pair: (*pair[0], pair[1] + 1))
        typed = (
            spark.createDataFrame(numbered, StructType([*source.TEXT_SCHEMA.fields, StructField("source_row", LongType())]))
            .select(*source.typed_columns(), F.col("source_row").cast("int"))
        )
        same_row = Window.partitionBy(*source.COLUMN_NAMES).orderBy("source_row")
        base = star.with_dates(
            typed.withColumn("is_duplicate", (F.row_number().over(same_row) > 1).cast("int"))
            .withColumn("import_id", F.lit(import_id))
        ).cache()
        stats = base.agg(
            F.sum("is_duplicate").alias("duplicates"),
            F.sum(F.col("arrival_date").isNull().cast("int")).alias("bad_dates"),
        ).first()
        text.unpersist()
        if stats["bad_dates"]:
            raise source.SourceError(f"{stats['bad_dates']} rows have an arrival year/month/day that isn't a date")
        print(f"[br5] {rows_read} rows read, {stats['duplicates']} exact duplicates", flush=True)

        if args.multiplier > 1:
            copies = spark.range(args.multiplier).select(F.col("id").cast("int").alias("copy_no"))
            rows = base.crossJoin(F.broadcast(copies))
        else:
            rows = base.withColumn("copy_no", F.lit(0))
        prepare_staging(client)

    with run.step("write_raw"):
        write(rows.select(*source.COLUMN_NAMES, "import_id", "source_row", "copy_no"),
              "raw_booking", args.writer, args.write_partitions)

    with run.step("write_dims"):
        dims = star.build_dimensions(base, star.load_country_names())
        staging = get_client(database=runs.STAGING_DB)
        for table, dim_rows in dims.items():
            columns = list(dim_rows[0])
            staging.insert(table, [[r[c] for c in columns] for r in dim_rows], column_names=columns)
        print("[br5] dimensions: " + ", ".join(f"{t}={len(r)}" for t, r in dims.items()), flush=True)

    with run.step("write_fact"):
        fact = star.build_fact(spark, rows, dims)
        fact.explain(mode="formatted")
        write(fact, "fact_booking", args.writer, args.write_partitions)

    with run.step("verify"):
        checks = verify(client, rows_read * args.multiplier)
        run.update(rows_written=checks["fact_rows"])

    with run.step("swap"):
        swap(client)


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)

    if not runs.table_exists(client, runs.DB, "fact_booking"):
        sys.exit("BR5 tables are missing: create them first (make br5-up).")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(f"Busy: a BR5 {busy['run_type']} run is in progress (step: {busy['step']}).")
    if not args.replace:
        stored = runs.row_count(client, runs.DB, "fact_booking")
        if stored:
            print(f"BR5 data already imported ({stored} bookings); nothing to do. Use --replace to re-import.")
            return

    if args.settings_json:
        values = run_settings.resolve(json.loads(args.settings_json))
    else:
        values = run_settings.resolve(runs.load_settings(client, run_settings.CURRENT))
    pipeline = run_settings.options(values)["pipeline"]
    args.writer = args.writer or pipeline["writer"]
    args.write_partitions = args.write_partitions or pipeline["write_partitions"]
    values.update({"pipeline.writer": args.writer, "pipeline.write_partitions": str(args.write_partitions)})
    cluster = runs.cluster_info()

    run = runs.Run(
        get_client(autogenerate_session_id=False),
        "import",
        run_id=args.run_id,
        multiplier=args.multiplier,
        writer=args.writer,
        settings=run_settings.snapshot(values, cluster, label=args.settings_label),
        source_file=args.source,
        source_bytes=os.path.getsize(args.source) if os.path.exists(args.source) else 0,
    )
    run.start()
    runs.stop_on_sigterm()
    print(f"[br5] import run {run.run_id} started", flush=True)

    spark = None
    try:
        spark = get_spark_session("br5_import", conf=run_settings.spark_conf(values))
        spark.sparkContext.setLogLevel("WARN")
        run.update(settings=run_settings.snapshot(values, cluster, spark, args.settings_label))
        import_data(spark, client, run, args)
    except BaseException as exc:
        message = runs.failure_message(exc)
        print(f"[br5] import failed: {message}", flush=True)
        try:
            client.command(f"DROP DATABASE IF EXISTS {runs.STAGING_DB} SYNC")
        finally:
            run.finish("failed", error=message[:4000])
        raise
    finally:
        if spark is not None:
            spark.stop()

    run.finish("succeeded")
    print(f"[br5] import succeeded: {run.row['rows_written']} bookings in "
          f"{run.row['duration_ms'] / 1000:.1f}s", flush=True)


if __name__ == "__main__":
    main()
