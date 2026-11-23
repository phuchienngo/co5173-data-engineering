"""Starts BR7 benchmark imports and runs with the stored settings or a preset.
Runs inside spark-local (no app container: Grafana is the only UI), called by
scripts/br7.py; built like br8_bench_launch.py.

    python3 br7_bench_launch.py import [--replace] [--writer pandas] [--preset NAME]
    python3 br7_bench_launch.py run --first-month 2025-09 [--preset NAME] [--tag bench_block=S2]
    python3 br7_bench_launch.py settings                 show the current settings and presets

The Spark driver runs in this container (client mode); the ClickHouse engine
runs as plain python3.
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

import br7_bench_runs as runs
import br7_bench_settings
import job_launch
from clickhouse_io import get_client

HERE = Path(__file__).resolve().parent
SCRIPTS = {"import": HERE / "br7_bench_import.py", "run": HERE / "br7_bench_run.py"}
LOG_DIR = Path(os.environ.get("BR7_LOG_DIR", "/opt/logs/br7"))
ECHO = (runs.PREFIX, "Busy", "BR7", "No data", "Settings", "Invalid", "Traceback", "Missing")
CATALOG = br7_bench_settings.CATALOG


def resolve_values(client, preset=None):
    if not preset:
        return CATALOG.resolve(runs.load_settings(client, runs.CURRENT)), "current settings"
    if preset in CATALOG.presets:
        return CATALOG.resolve(preset=preset), f"preset {preset}"
    stored = runs.load_settings(client, preset)
    if stored is None:
        raise ValueError(f"No preset named '{preset}'")
    return CATALOG.resolve(stored), f"preset {preset}"


def preflight(client, kind, values):
    if not runs.table_exists(client, runs.DB, "run_replay"):
        return "BR7 tables are missing: run `make br7-up`."
    busy = runs.busy_run(client)
    if busy:
        return runs.busy_message(busy)
    if kind == "run" and not runs.row_count(client, runs.DB, "raw_trip"):
        return "No data yet: import it first (`make br7-import`)."
    errors, _ = CATALOG.validate(values, runs.run_store.cluster_info())
    if errors:
        return "Settings can't be used: " + "; ".join(f"{CATALOG.by_key[k].label} {m}" for k, m in errors.items())
    return None


def command(kind, values, job_args, label):
    args = [*job_args, "--settings-json", json.dumps(values), "--settings-label", label]
    if kind == "run" and values["pipeline.engine"] == "clickhouse":
        return ["python3", str(SCRIPTS[kind]), *args]
    return job_launch.spark_submit(SCRIPTS[kind], args, values["spark.driver.memory"])


def show_settings(client):
    current = CATALOG.resolve(runs.load_settings(client, runs.CURRENT))
    print("Current settings that differ from the defaults:")
    for key in CATALOG.changed(current):
        print(f"  {key} = {current[key]}")
    print("Presets: " + ", ".join([*CATALOG.presets, *runs.run_store.preset_names(client, runs.DB)]))


def main():
    parser = argparse.ArgumentParser(description="Start a BR7 benchmark import or run")
    sub = parser.add_subparsers(dest="kind", required=True)
    imp = sub.add_parser("import")
    imp.add_argument("--replace", action="store_true")
    imp.add_argument("--writer", choices=("partitions", "pandas"))
    imp.add_argument("--preset")
    run = sub.add_parser("run")
    run.add_argument("--first-month", required=True)
    run.add_argument("--preset")
    run.add_argument("--tag", action="append", default=[])
    sub.add_parser("settings")
    args = parser.parse_args()

    client = get_client(autogenerate_session_id=False)
    if args.kind == "settings":
        show_settings(client)
        return
    try:
        values, label = resolve_values(client, args.preset)
    except ValueError as exc:
        sys.exit(str(exc))
    problem = preflight(client, args.kind, values)
    if problem:
        sys.exit(problem)
    if args.kind == "import":
        job_args = ["--replace" if args.replace else "--if-empty"] + (["--writer", args.writer] if args.writer else [])
    else:
        job_args = ["--first-month", args.first_month] + [x for tag in args.tag for x in ("--tag", tag)]

    run_id = str(uuid.uuid4())
    log = LOG_DIR / f"{args.kind}-{run_id}.log"
    proc = job_launch.start(command(args.kind, values, [*job_args, "--run-id", run_id], label), log, stream=True)
    print(f"{runs.PREFIX} {args.kind} {run_id} with {label}; log: {log}", flush=True)
    try:
        with log.open("w") as out:
            for line in proc.stdout:
                out.write(line)
                if line.startswith(ECHO):
                    print(line, end="", flush=True)
        code = proc.wait()
    except KeyboardInterrupt:
        job_launch.stop(proc, SCRIPTS.values())
        proc.wait()
        sys.exit(f"{args.kind.capitalize()} stopped.")
    if code != 0:
        sys.exit(f"{args.kind.capitalize()} failed (exit code {code}). Log: {log}")


if __name__ == "__main__":
    main()
