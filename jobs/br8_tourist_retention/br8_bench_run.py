"""BR8 benchmark run: prepare_br8.py's processing on the imported data, with
Spark or ClickHouse SQL, recorded in br8.pipeline_runs (docs/plan/br7/plan-benchmark.md).

Started by br8_bench_launch.py (scripts/br8.py run, tests/br8/benchmark.py):
    spark-submit br8_bench_run.py --region ALL --settings-json '{...}' [--tag bench_block=S2 --tag bench_cell=I1-L2]
With the ClickHouse engine it runs as plain python3, without Spark.
"""

import argparse
import json
import sys

import br8_bench_runs as runs
import br8_bench_settings
import run_figures
from clickhouse_io import get_client

CATALOG = br8_bench_settings.CATALOG


def parse_args():
    parser = argparse.ArgumentParser(description="BR8 benchmark run")
    parser.add_argument("--region", required=True, help="Destination region id (Wikidata), or ALL")
    parser.add_argument("--run-id")
    parser.add_argument("--settings-json")
    parser.add_argument("--settings-label", default="current settings")
    parser.add_argument("--tag", action="append", default=[], help="key=value stored in criteria (benchmark cell)")
    return parser.parse_args()


def log(message):
    print(f"{runs.PREFIX} {message}", flush=True)


class Context:
    def __init__(self, client, region, values):
        self.client = client
        self.region = region
        self.where = runs.region_where(region)
        self.opts = CATALOG.options(values)
        self.query_settings = br8_bench_settings.clickhouse_query_settings(values)
        # Arrow strings, not binary, for the Spark readers.
        self.query_settings["output_format_arrow_string_as_string"] = 1


def process(engine, run):
    with run.step("read"):
        read = engine.read()
        log(f"{read:,} check-ins read")
    with run.step("clean"):
        excluded, used = engine.clean()
    with run.step("match"):
        named_rows, attraction_rows = engine.match()
        excluded["unnamed_place"] = used - named_rows
        excluded["not_attraction"] = named_rows - attraction_rows
        run.update(rows_read=read, rows_used=attraction_rows, rows_excluded=excluded)
        log(f"{attraction_rows:,} attraction check-ins used; excluded {excluded}")
    with run.step("aggregate"):
        visits = engine.aggregate(run.run_id)
        log(f"{visits:,} daily visits")
    with run.step("write"):
        engine.write(run.run_id) if engine.name == "clickhouse" else engine.write()


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "run_visit"):
        sys.exit("BR8 tables are missing: run `make br8-up`.")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(runs.busy_message(busy))
    import_id = runs.stored_import(client)
    if import_id is None:
        sys.exit("No data yet: import it first (`make br8-import`).")
    try:
        runs.region_where(args.region)
    except ValueError as exc:
        sys.exit(f"Invalid choice: {exc}")
    if args.region != "ALL" and not int(client.command(
            f"SELECT count() FROM {runs.DB}.region WHERE region_id = {{r:String}}", parameters={"r": args.region})):
        sys.exit(f"Invalid choice: region {args.region} is not in the stored data.")

    values = CATALOG.resolve(json.loads(args.settings_json) if args.settings_json
                             else runs.load_settings(client, runs.CURRENT))
    cluster = runs.run_store.cluster_info()
    errors, _ = CATALOG.validate(values, cluster)
    if errors:
        sys.exit("Settings can't be used: " + "; ".join(f"{CATALOG.by_key[k].label} {v}" for k, v in errors.items()))
    threads = client.command("SELECT value FROM system.settings WHERE name = 'max_threads'")
    cluster = {**(cluster or {}), "clickhouse_max_threads": str(threads).strip("'")}
    engine_name = CATALOG.options(values)["pipeline"]["engine"]
    criteria = {"region": args.region, **dict(t.split("=", 1) for t in args.tag)}

    run = runs.Run(get_client(autogenerate_session_id=False), "run", run_id=args.run_id, criteria=criteria,
                   settings=CATALOG.snapshot(values, cluster, label=args.settings_label), import_id=import_id,
                   writer=values["pipeline.writer"])
    run.start()
    runs.stop_on_sigterm()
    log(f"run {run.run_id} started: region {args.region}, engine {engine_name}")

    spark = engine = ui = None
    try:
        ctx = Context(client, args.region, values)
        ctx.query_settings["log_comment"] = run.run_id  # tags the run's queries in system.query_log
        if engine_name == "spark":
            from br8_bench_spark import SparkEngine
            from spark_session import get_spark_session

            conf = br8_bench_settings.spark_conf(values)
            run_figures.prepare_event_log(conf)
            spark = get_spark_session("BR8 benchmark run", conf=conf)
            spark.sparkContext.setLogLevel("WARN")
            ui = run_figures.spark_ui(spark)
            run.update(settings=CATALOG.snapshot(values, cluster, spark, args.settings_label), spark_app_id=ui[1])
            run.on_step = lambda name: spark.sparkContext.setJobGroup(name, name)
            engine = SparkEngine(spark, ctx)
        else:
            from br8_bench_sql import SqlEngine

            engine = SqlEngine(ctx)
        process(engine, run)
        run.finish("succeeded")
        # After finish, so the result check is not part of the run time.
        summary = runs.result_summary(client, run.run_id)
        summary["fingerprint"] = runs.fingerprint(client, run.run_id)
        run.update(result_count=min(summary["places"], 65535), rows_written=summary["visits"] + summary["places"],
                   result_summary=json.dumps(summary))
    except BaseException as exc:
        error = runs.failure_message(exc)
        log(f"run failed: {error}")
        run.finish("failed", error=error[:4000])
        raise
    finally:
        run_figures.record(run, client, runs.STAGING_DB, ui)
        if engine is not None:
            engine.close()
        if spark is not None:
            spark.stop()
    log(f"run succeeded in {run.row['duration_ms'] / 1000:.1f}s: {summary['visits']:,} visits, "
        f"{summary['places']:,} places, {summary['returning_pairs']:,} returning visitor/place pairs; "
        f"fingerprint {summary['fingerprint']}")


if __name__ == "__main__":
    main()
