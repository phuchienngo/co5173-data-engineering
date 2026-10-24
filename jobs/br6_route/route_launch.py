"""Starts BR6 imports and Route Finder runs with the stored settings.

Used by the app (apps/br5/route_backend.py) and, as a CLI inside the br5-app
container, by scripts/br6.py, so a job started from the terminal is built
exactly like one started from the browser:

    python3 route_launch.py import [--cities jakarta,tokyo] [--multiplier 2] [--preset NAME] [--writer pandas]
    python3 route_launch.py run --city jakarta --start 123 --end 456 [--stops 7,8] [--types ..] [--visitor 42]

The Spark driver runs in this container (client mode); the ClickHouse engine
runs as plain python3, without Spark.
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

import job_launch
import route_runs as runs
import route_settings
from clickhouse_io import get_client

HERE = Path(__file__).resolve().parent
SCRIPTS = {"import": HERE / "job.py", "run": HERE / "route_run.py"}
LOG_DIR = Path(os.environ.get("BR6_LOG_DIR", "/opt/logs/br6"))
ECHO = ("[br6]", "Busy", "BR6", "No data", "Settings", "Invalid", "Traceback", "No file", "City", "Place")
CATALOG = route_settings.CATALOG


def resolve_values(client, preset=None):
    """(values, label): the current settings, or a built-in or saved preset."""
    if not preset:
        return CATALOG.resolve(runs.load_settings(client, runs.CURRENT)), "current settings"
    if preset in CATALOG.presets:
        return CATALOG.resolve(preset=preset), f"preset {preset}"
    stored = runs.load_settings(client, preset)
    if stored is None:
        raise ValueError(f"No preset named '{preset}'")
    return CATALOG.resolve(stored), f"preset {preset}"


def preflight(client, kind, values):
    """Why the job can't start now, or None."""
    if not runs.table_exists(client, runs.DB, "run_place"):
        return "BR6 tables are missing or outdated: run `make br6-up`."
    busy = runs.busy_run(client)
    if busy:
        return runs.busy_message(busy)
    if kind == "run":
        if not runs.row_count(client, runs.DB, "fact_checkin"):
            return "No data yet: import it first."
        if values["pipeline.place_stats"] == "import" and not runs.place_stats_ready(client):
            return ("Place statistics are missing: re-import, or set Place statistics to 'run' "
                    "in Advanced Settings.")
    errors, _ = CATALOG.validate(values, runs.run_store.cluster_info())
    if errors:
        return "Settings can't be used: " + "; ".join(f"{CATALOG.by_key[k].label} {msg}" for k, msg in errors.items())
    return None


def command(kind, values, job_args, label="current settings"):
    args = [*job_args, "--settings-json", json.dumps(values), "--settings-label", label]
    if kind == "run" and values["pipeline.engine"] == "clickhouse":
        return ["python3", str(SCRIPTS[kind]), *args]
    return job_launch.spark_submit(SCRIPTS[kind], args, values["spark.driver.memory"])


def start(kind, job_args, values, run_id, stream=False, label="current settings"):
    """Starts the job in its own process group. Returns (process, log path)."""
    log = LOG_DIR / f"{kind}-{run_id}.log"
    cmd = command(kind, values, [*job_args, "--run-id", run_id], label)
    return job_launch.start(cmd, log, stream), log


def stop(proc):
    job_launch.stop(proc, SCRIPTS.values())


def import_args(cities=(), multiplier=1, replace=True, writer=None, write_partitions=None):
    args = ["--replace" if replace else "--if-empty", "--multiplier", str(multiplier)]
    if cities:
        args += ["--cities", ",".join(cities)]
    if writer:
        args += ["--writer", writer]
    if write_partitions:
        args += ["--write-partitions", str(write_partitions)]
    return args


def run_args(city, start, end, stops=(), types=(), visitor=None):
    """Route Finder arguments: venue ids for the points, user id for the visitor."""
    args = ["--city", city, "--start", str(start), "--end", str(end)]
    if stops:
        args += ["--stops", ",".join(map(str, stops))]
    if types:
        args += ["--types", ",".join(types)]
    if visitor is not None:
        args += ["--visitor", str(visitor)]
    return args


def main():
    parser = argparse.ArgumentParser(description="Start a BR6 import or Route Finder run with the stored settings")
    sub = parser.add_subparsers(dest="kind", required=True)
    imp = sub.add_parser("import")
    imp.add_argument("--cities", default="", help="Comma-separated city file names; default: all")
    imp.add_argument("--multiplier", type=int, default=1)
    imp.add_argument("--if-empty", action="store_true")
    imp.add_argument("--writer", choices=("partitions", "pandas"), help="Overrides the setting")
    imp.add_argument("--write-partitions", type=int, help="Overrides the setting")
    imp.add_argument("--preset", help="Use this preset instead of the current settings")
    run = sub.add_parser("run")
    run.add_argument("--city", required=True)
    run.add_argument("--start", type=int, required=True, help="venue_id")
    run.add_argument("--end", type=int, required=True, help="venue_id")
    run.add_argument("--stops", default="", help="Comma-separated venue_ids, in order")
    run.add_argument("--types", default="", help="Comma-separated place types; default: all")
    run.add_argument("--visitor", type=int, help="user_id; default: General visitor")
    run.add_argument("--preset", help="Use this preset instead of the current settings")
    args = parser.parse_args()

    client = get_client(autogenerate_session_id=False)
    try:
        values, label = resolve_values(client, args.preset)
    except ValueError as exc:
        sys.exit(str(exc))
    problem = preflight(client, args.kind, values)
    if problem:
        sys.exit(problem)
    if args.kind == "import":
        cities = [c.strip() for c in args.cities.split(",") if c.strip()]
        job_args = import_args(cities, args.multiplier, not args.if_empty, args.writer, args.write_partitions)
    else:
        job_args = run_args(args.city, args.start, args.end, [s for s in args.stops.split(",") if s.strip()],
                            [t.strip() for t in args.types.split(",") if t.strip()], args.visitor)

    run_id = str(uuid.uuid4())
    proc, log = start(args.kind, job_args, values, run_id, stream=True, label=label)
    print(f"[br6] {args.kind} {run_id} with {label}; log: {log}", flush=True)
    try:
        with log.open("w") as out:
            for line in proc.stdout:
                out.write(line)
                if line.startswith(ECHO):
                    print(line, end="", flush=True)
        code = proc.wait()
    except KeyboardInterrupt:
        stop(proc)
        proc.wait()
        sys.exit(f"{args.kind.capitalize()} stopped.")
    if code != 0:
        sys.exit(f"{args.kind.capitalize()} failed (exit code {code}). Log: {log}")


if __name__ == "__main__":
    main()
