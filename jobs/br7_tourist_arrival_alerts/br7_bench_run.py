"""BR7 benchmark run: prepare.py's processing on the imported trips, with Spark
or ClickHouse SQL, recorded in br7.pipeline_runs (docs/plan/br7/plan-benchmark.md).

Started by br7_bench_launch.py (scripts/br7.py run, tests/br7/benchmark.py):
    spark-submit br7_bench_run.py --first-month 2025-09 --settings-json '{...}' [--tag bench_block=S2 ...]
Training runs from --first-month to 2026-08; the replay month is 2026-09
(prepare_br7.py's months). With the ClickHouse engine it runs as plain python3, without Spark.
"""

import argparse
import json
import sys

import br7_bench_runs as runs
import br7_bench_settings
import run_figures
from clickhouse_io import get_client

CATALOG = br7_bench_settings.CATALOG


def parse_args():
    parser = argparse.ArgumentParser(description="BR7 benchmark run")
    parser.add_argument("--first-month", required=True, help="First training month, e.g. 2025-09")
    parser.add_argument("--run-id")
    parser.add_argument("--settings-json")
    parser.add_argument("--settings-label", default="current settings")
    parser.add_argument("--tag", action="append", default=[], help="key=value stored in criteria (benchmark cell)")
    return parser.parse_args()


def log(message):
    print(f"{runs.PREFIX} {message}", flush=True)


class Context:
    def __init__(self, client, first_month, values):
        self.client = client
        self.train_months = runs.train_months(first_month)
        self.replay_month = runs.REPLAY_MONTH
        self.where = runs.month_where(first_month)
        self.opts = CATALOG.options(values)
        self.query_settings = br7_bench_settings.clickhouse_query_settings(values)
        # Arrow strings, not binary, for the Spark readers.
        self.query_settings["output_format_arrow_string_as_string"] = 1


def process(engine, run):
    with run.step("quality"):
        quality = engine.quality()
        read = sum(quality.values())
        labels = {}
        for (_, label), n in quality.items():
            labels[label] = labels.get(label, 0) + n
        log(f"{read:,} trips read; labels {dict(sorted(labels.items()))}")
    with run.step("clean"):
        months = engine.clean()
        tourist = sum(a for a, _ in months.values())
        trips = sum(b for _, b in months.values())
        excluded = {k: v for k, v in labels.items() if k != "ok"}
        excluded["outside_tourist_zones"] = labels.get("ok", 0) - tourist
        excluded["duplicate"] = tourist - trips
        run.update(rows_read=read, rows_used=trips, rows_excluded=excluded)
        log(f"{trips:,} tourist-zone trips used; {tourist - trips:,} duplicates removed")
    with run.step("baseline"):
        cells = engine.baseline() if engine.name == "spark" else engine.baseline(run.run_id)
        log(f"{cells:,} baseline cells")
    with run.step("replay"):
        counts = engine.replay() if engine.name == "spark" else engine.replay(run.run_id)
        log(f"{sum(counts.values()):,} replay events in {len(counts)} zones")
    with run.step("write"):
        engine.write(run.run_id, quality, months)


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "run_replay"):
        sys.exit("BR7 tables are missing: run `make br7-up`.")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(runs.busy_message(busy))
    import_id = runs.stored_import(client)
    if import_id is None:
        sys.exit("No data yet: import it first (`make br7-import`).")
    try:
        runs.train_months(args.first_month)
    except ValueError as exc:
        sys.exit(f"Invalid choice: {exc}")

    values = CATALOG.resolve(json.loads(args.settings_json) if args.settings_json
                             else runs.load_settings(client, runs.CURRENT))
    cluster = runs.run_store.cluster_info()
    errors, _ = CATALOG.validate(values, cluster)
    if errors:
        sys.exit("Settings can't be used: " + "; ".join(f"{CATALOG.by_key[k].label} {v}" for k, v in errors.items()))
    threads = client.command("SELECT value FROM system.settings WHERE name = 'max_threads'")
    cluster = {**(cluster or {}), "clickhouse_max_threads": str(threads).strip("'")}
    engine_name = CATALOG.options(values)["pipeline"]["engine"]
    criteria = {"first_month": args.first_month, "replay_month": runs.REPLAY_MONTH,
                **dict(t.split("=", 1) for t in args.tag)}

    run = runs.Run(get_client(autogenerate_session_id=False), "run", run_id=args.run_id, criteria=criteria,
                   settings=CATALOG.snapshot(values, cluster, label=args.settings_label), import_id=import_id,
                   writer=values["pipeline.writer"])
    run.start()
    runs.stop_on_sigterm()
    log(f"run {run.run_id} started: training from {args.first_month}, replay {runs.REPLAY_MONTH}, "
        f"engine {engine_name}")

    spark = engine = ui = None
    try:
        ctx = Context(client, args.first_month, values)
        ctx.query_settings["log_comment"] = run.run_id  # tags the run's queries in system.query_log
        if engine_name == "spark":
            from br7_bench_spark import SparkEngine
            from spark_session import get_spark_session

            conf = br7_bench_settings.spark_conf(values)
            run_figures.prepare_event_log(conf)
            spark = get_spark_session("BR7 benchmark run", conf=conf)
            spark.sparkContext.setLogLevel("WARN")
            ui = run_figures.spark_ui(spark)
            run.update(settings=CATALOG.snapshot(values, cluster, spark, args.settings_label), spark_app_id=ui[1])
            run.on_step = lambda name: spark.sparkContext.setJobGroup(name, name)
            engine = SparkEngine(spark, ctx)
        else:
            from br7_bench_sql import SqlEngine

            engine = SqlEngine(ctx)
        process(engine, run)
        run.finish("succeeded")
        # After finish, so the result check is not part of the run time.
        summary = runs.result_summary(client, run.run_id)
        summary["fingerprint"] = runs.fingerprint(client, run.run_id)
        run.update(result_count=min(summary["baseline_cells"], 65535),
                   rows_written=summary["baseline_cells"] + summary["replay_events"],
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
    log(f"run succeeded in {run.row['duration_ms'] / 1000:.1f}s: {summary['rows_read']:,} trips read, "
        f"{summary['trips']:,} used, {summary['baseline_cells']:,} baseline cells, "
        f"{summary['replay_events']:,} replay events; fingerprint {summary['fingerprint']}")


if __name__ == "__main__":
    main()
