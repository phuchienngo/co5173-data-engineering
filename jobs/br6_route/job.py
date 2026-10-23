"""BR6 import: data/br6/<city>_checkins.csv -> br6.raw_checkin + star schema.

Plan: docs/plan/br6/plan-data-import.md. Everything is built in the
br6_staging database, checked, then swapped into br6 table by table, so the
live data is untouched until the new data is verified.

Run by start.py with no arguments (= --if-empty), and by route_launch.py
(make br6-import, the app's Data screen).
"""

import argparse
import json
import os
import sys
import uuid

from pyspark.sql import functions as F

import checkin_source as source
import route_runs as runs
import route_settings
import route_star as star
import run_figures
from clickhouse_io import get_client, write_partitions
from spark_session import get_spark_session

MODE = "batch"

WRITERS = ("partitions", "pandas")
# Nullable floats: toPandas turns an empty value into NaN, which ClickHouse
# would store as NaN instead of NULL.
NULLABLE_FLOATS = {"latitude", "longitude"}


def parse_args():
    parser = argparse.ArgumentParser(description="Import BR6 check-ins into ClickHouse")
    when = parser.add_mutually_exclusive_group()
    when.add_argument("--if-empty", action="store_true", help="Import only if no data is stored (default)")
    when.add_argument("--replace", action="store_true", help="Import and replace the stored data")
    parser.add_argument("--cities", default="", help="Comma-separated city file names; default: every file")
    parser.add_argument("--multiplier", type=int, default=1, help="Repeat the check-ins N times (synthetic data)")
    parser.add_argument("--writer", choices=WRITERS, help="Default: the writer setting")
    parser.add_argument("--write-partitions", type=int, help="Spark partitions used to write. Default: the setting")
    parser.add_argument("--run-id", help="Run id to record (the app passes its own)")
    parser.add_argument("--data-dir", default=source.DATA_DIR)
    parser.add_argument("--settings-json", help="Resolved settings (route_launch.py passes them); default: the "
                                                "current settings stored in br6.settings")
    parser.add_argument("--settings-label", default="current settings", help="Where the settings came from")
    args = parser.parse_args()
    if args.multiplier < 1:
        parser.error("--multiplier must be at least 1")
    args.cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    return args


def log(message):
    print(f"{runs.PREFIX} {message}", flush=True)


def write(df, table, writer, partitions):
    if writer == "partitions":
        write_partitions(df.repartition(partitions), table, database=runs.STAGING_DB)
        return
    # Baseline: every row goes through the driver.
    pdf = df.toPandas()
    for column in NULLABLE_FLOATS & set(pdf.columns):
        values = pdf[column].astype(object)
        pdf[column] = values.where(pdf[column].notna(), None)
    get_client(database=runs.STAGING_DB).insert_df(table, pdf)


def insert_rows(table, rows):
    columns = list(rows[0])
    get_client(database=runs.STAGING_DB).insert(table, [[r[c] for c in columns] for r in rows],
                                               column_names=columns)


def prepare_staging(client):
    client.command(f"DROP DATABASE IF EXISTS {runs.STAGING_DB} SYNC")
    client.command(f"CREATE DATABASE {runs.STAGING_DB}")
    for table in runs.DATA_TABLES:
        client.command(f"CREATE TABLE {runs.STAGING_DB}.{table} AS {runs.DB}.{table}")


def check_source(text, files, rows_per_city):
    """Row counts, first and last rows against Python's own read of each
    file, lost values and place consistency. Raises SourceError."""
    problems = []
    edges = {
        (r["city"], r["source_row"]): r
        for r in text.where(F.col("source_row") == 1)
        .unionByName(text.join(F.broadcast(text.sparkSession.createDataFrame(
            [(c, n) for c, n in rows_per_city.items()], "city string, source_row int")), ["city", "source_row"]))
        .collect()
    }
    for city, path in files.items():
        lines, first, last = source.file_profile(path)
        if rows_per_city.get(city, 0) != lines:
            problems.append(f"{city}: {rows_per_city.get(city, 0)} rows read, the file has {lines}")
            continue
        for row_no, expected in ((1, first), (lines, last)):
            got = edges.get((city, row_no))
            got_values = [got[c] or "" for c in source.COLUMN_NAMES] if got else None
            if got_values != expected:
                problems.append(f"{city}: row {row_no} read as {got_values}, the file has {expected}")
    if problems:
        raise source.SourceError("The files were not read as Python reads them: " + "; ".join(problems))

    lost = source.problem_counts(text)
    if lost:
        raise source.SourceError(f"Values that can't be stored as-is, per column: {lost}")
    inconsistent = source.inconsistent_places(text)
    if inconsistent:
        raise source.SourceError(f"{inconsistent} places have more than one name, address or position")


def verify(client, rows_per_city, multiplier):
    s = runs.STAGING_DB
    rows_read = sum(rows_per_city.values())
    checks = client.query(f"""
        SELECT
            (SELECT count() FROM {s}.raw_checkin) AS raw_rows,
            (SELECT count() FROM {s}.fact_checkin) AS fact_rows,
            (SELECT countIf(city_key NOT IN (SELECT city_key FROM {s}.dim_city)
                         OR place_key NOT IN (SELECT place_key FROM {s}.dim_place)
                         OR visitor_key NOT IN (SELECT visitor_key FROM {s}.dim_visitor)
                         OR category_key NOT IN (SELECT category_key FROM {s}.dim_category)
                         OR area_key NOT IN (SELECT area_key FROM {s}.dim_area)
                         OR date_key NOT IN (SELECT date_key FROM {s}.dim_date))
             FROM {s}.fact_checkin) AS orphan_keys,
            (SELECT count() FROM {s}.dim_place) AS places,
            (SELECT uniqExact(city, venue_id) FROM {s}.raw_checkin) AS raw_places,
            (SELECT countIf(has_location = 0) FROM {s}.dim_place) AS places_no_location,
            (SELECT countIf(latitude IS NULL) FROM {s}.raw_checkin) AS raw_no_location,
            (SELECT count() FROM {s}.fact_checkin
             WHERE place_key IN (SELECT place_key FROM {s}.dim_place WHERE has_location = 0)) AS fact_no_location,
            (SELECT count() FROM {s}.dim_visitor) AS visitors,
            (SELECT uniqExact(city, user_id) FROM {s}.raw_checkin) AS raw_visitors,
            (SELECT count() FROM (
                SELECT venue_category_id AS id, count() * {int(multiplier)} AS n FROM {s}.raw_checkin GROUP BY id
             ) AS r FULL JOIN (
                SELECT d.category_id AS id, count() AS n FROM {s}.fact_checkin AS f
                INNER JOIN {s}.dim_category AS d USING (category_key) GROUP BY id
             ) AS f USING (id) WHERE r.n != f.n) AS category_mismatches,
            (SELECT count() FROM {s}.dim_category) AS categories,
            (SELECT count() FROM {s}.category_place_type) AS grouped,
            (SELECT countIf(main_category_key NOT IN (SELECT category_key FROM {s}.dim_category))
             FROM {s}.dim_place) AS places_without_category,
            (SELECT countIf(categories_seen > 1) FROM {s}.dim_place) AS places_several_categories
    """).first_item
    log(f"checks: {checks}")
    per_city = dict(client.query(
        f"SELECT c.city_code, count() FROM {s}.fact_checkin AS f "
        f"INNER JOIN {s}.dim_city AS c USING (city_key) GROUP BY c.city_code"
    ).result_rows)
    raw_city = dict(client.query(f"SELECT city, count() FROM {s}.raw_checkin GROUP BY city").result_rows)

    problems = []
    if checks["raw_rows"] != rows_read or checks["fact_rows"] != rows_read * multiplier:
        problems.append(f"row counts raw={checks['raw_rows']} fact={checks['fact_rows']}, "
                        f"expected {rows_read} and {rows_read * multiplier}")
    wrong_cities = [c for c, n in rows_per_city.items()
                    if raw_city.get(c) != n or per_city.get(c) != n * multiplier]
    if wrong_cities:
        problems.append(f"row counts differ per city for {wrong_cities}")
    if checks["orphan_keys"]:
        problems.append(f"{checks['orphan_keys']} fact rows point to a missing dimension row")
    if checks["places"] != checks["raw_places"] or checks["visitors"] != checks["raw_visitors"]:
        problems.append(f"places {checks['places']} vs {checks['raw_places']}, "
                        f"visitors {checks['visitors']} vs {checks['raw_visitors']}")
    if checks["fact_no_location"] != checks["raw_no_location"] * multiplier:
        problems.append(f"check-ins without location: fact {checks['fact_no_location']}, "
                        f"raw {checks['raw_no_location']} ×{multiplier}")
    if checks["category_mismatches"]:
        problems.append(f"{checks['category_mismatches']} categories have different counts in raw and fact")
    if checks["grouped"] != checks["categories"] or checks["places_without_category"]:
        problems.append(f"grouping rows {checks['grouped']} for {checks['categories']} categories; "
                        f"{checks['places_without_category']} places without a main category")
    if problems:
        raise RuntimeError("Import checks failed: " + "; ".join(problems))
    return checks


def swap(client):
    for table in runs.DATA_TABLES:
        client.command(f"EXCHANGE TABLES {runs.DB}.{table} AND {runs.STAGING_DB}.{table}")
    client.command(f"DROP DATABASE {runs.STAGING_DB} SYNC")


def import_data(spark, client, run, args, files):
    import_id = str(uuid.uuid4())
    run.update(import_id=import_id)
    grouping = star.load_grouping()
    reference = star.load_cities()

    with run.step("read"):
        text = source.read_text(spark, files).cache()
        rows_per_city = {r["city"]: r["n"] for r in text.groupBy("city").agg(F.count("*").alias("n")).collect()}
        rows_read = sum(rows_per_city.values())
        run.update(rows_read=rows_read, rows_expected=rows_read * args.multiplier)
        log(f"{rows_read:,} rows read from {len(files)} files")

    with run.step("check"):
        check_source(text, files, rows_per_city)

    with run.step("derive"):
        typed = text.select("city", *source.typed_columns(), "source_row", F.lit(import_id).alias("import_id"))
        centres = typed.groupBy("city").agg(
            F.collect_set("venue_country").alias("countries"),
            F.median("latitude").alias("lat"), F.median("longitude").alias("lon"),
        ).collect()
        several = [r["city"] for r in centres if len(r["countries"]) != 1]
        if several:
            raise source.SourceError(f"More than one country in the files of {several}")
        cities = star.city_rows([(r["city"], r["countries"][0], r["lat"], r["lon"]) for r in centres], reference)
        rows = star.with_city(typed, cities).cache()
        rows.count()
        text.unpersist()
        prepare_staging(client)

    with run.step("write_raw"):
        write(rows.select("city", *source.COLUMN_NAMES, "import_id", "source_row"),
              "raw_checkin", args.writer, args.write_partitions)

    with run.step("write_dims"):
        dims, ungrouped = star.small_dimensions(rows, cities, grouping)
        for table, dim_rows in dims.items():
            insert_rows(table, dim_rows)
        if ungrouped:
            log(f"{len(ungrouped)} categories are not in the grouping file (Not offered): {ungrouped[:20]}")
        places = star.build_places(rows, dims["dim_category"]).cache()
        visitors = star.build_visitors(rows).cache()
        write(places, "dim_place", args.writer, args.write_partitions)
        write(visitors, "dim_visitor", args.writer, args.write_partitions)
        log("dimensions: " + ", ".join(f"{t}={len(r)}" for t, r in dims.items())
            + f", dim_place={places.count()}, dim_visitor={visitors.count()}")

    with run.step("write_fact"):
        fact = star.build_fact(spark, rows, places, visitors, dims, args.multiplier)
        fact.explain(mode="formatted")
        write(fact, "fact_checkin", args.writer, args.write_partitions)

    with run.step("verify"):
        checks = verify(client, rows_per_city, args.multiplier)
        run.update(rows_written=checks["fact_rows"], result_summary=json.dumps({
            "cities": sorted(files),
            "ungrouped_categories": ungrouped,
            "places_several_categories": checks["places_several_categories"],
        }))

    with run.step("swap"):
        swap(client)


def preflight(client, args):
    """Why the import can't start (or has nothing to do), or None; and the files."""
    if not runs.table_exists(client, runs.DB, "fact_checkin"):
        sys.exit("BR6 tables are missing: create them first (make br6-up).")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(runs.busy_message(busy))
    try:
        files = source.city_files(args.cities, args.data_dir)
        reference = star.load_cities()
        for city, path in files.items():
            source.check_file(path)
            if city not in reference:
                raise source.SourceError(f"City '{city}' is not in {star.CITIES_FILE}")
        star.load_grouping()
    except (source.SourceError, star.ReferenceDataError, OSError) as exc:
        sys.exit(str(exc))
    if not args.replace:
        stored = runs.row_count(client, runs.DB, "fact_checkin")
        if stored:
            print(f"BR6 data already imported ({stored:,} check-ins); nothing to do. Use --replace to re-import.")
            sys.exit(0)
    return files


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    files = preflight(client, args)
    catalog = route_settings.CATALOG

    if args.settings_json:
        values = catalog.resolve(json.loads(args.settings_json))
    else:
        values = catalog.resolve(runs.load_settings(client, runs.CURRENT))
    pipeline = catalog.options(values)["pipeline"]
    args.writer = args.writer or pipeline["writer"]
    args.write_partitions = args.write_partitions or pipeline["write_partitions"]
    values.update({"pipeline.writer": args.writer, "pipeline.write_partitions": str(args.write_partitions)})
    cluster = runs.run_store.cluster_info()

    run = runs.Run(
        get_client(autogenerate_session_id=False),
        "import",
        run_id=args.run_id,
        multiplier=args.multiplier,
        writer=args.writer,
        criteria={"cities": ",".join(sorted(files)) if args.cities else "all"},
        settings=catalog.snapshot(values, cluster, label=args.settings_label),
        source_file=f"{args.data_dir}/ ({len(files)} files)",
        source_bytes=sum(os.path.getsize(p) for p in files.values()),
    )
    run.start()
    runs.stop_on_sigterm()
    log(f"import run {run.run_id} started: {len(files)} cities, ×{args.multiplier}, writer {args.writer}")

    spark = None
    ui = None
    try:
        conf = route_settings.spark_conf(values)
        run_figures.prepare_event_log(conf)
        spark = get_spark_session("BR6 import", conf=conf)
        spark.sparkContext.setLogLevel("WARN")
        ui = run_figures.spark_ui(spark)
        run.update(settings=catalog.snapshot(values, cluster, spark, args.settings_label), spark_app_id=ui[1])
        # Spark jobs are grouped by step, for the per-step figures and the Spark page.
        run.on_step = lambda name: spark.sparkContext.setJobGroup(name, name)
        import_data(spark, client, run, args, files)
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
        # After finish (not part of the run time), before Spark stops.
        run_figures.record(run, client, runs.STAGING_DB, ui)
        if spark is not None:
            spark.stop()

    log(f"import succeeded: {run.row['rows_written']:,} check-ins in {run.row['duration_ms'] / 1000:.1f}s")


if __name__ == "__main__":
    main()
