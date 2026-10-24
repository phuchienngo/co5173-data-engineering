"""BR6 launcher: start-up modes and data import.

    python3 scripts/br6.py up        start the stack and the shared app (data kept)
    python3 scripts/br6.py fresh     drop BR6 data, import, then start the app
    python3 scripts/br6.py import    import (replaces stored data after asking)
    python3 scripts/br6.py run       one Route Finder run (--city, --start, --end venue ids, ...)
    python3 scripts/br6.py status    show what is stored

Imports and runs are started by jobs/br6_route/route_launch.py inside the br5-app
container (the app shared by BR5 and BR6), the same code the app's buttons
use, with the stored settings (or --preset NAME).
Plans: docs/plan/br6/plan-data-import.md, plan-pipeline.md. Kafka is not started: BR6 has no
streaming input.
"""

import argparse
import subprocess
import sys

from start import (
    REPO_ROOT,
    build_image,
    clickhouse_query,
    container_health,
    create_clickhouse_tables,
    run,
    wait_healthy,
)

BR = "6"
SERVICES = ["clickhouse", "spark-master", "spark-worker", "spark-local", "prometheus", "grafana",
            "spark-history", "docker-stats"]
APP_SERVICE = "br5-app"  # shared with BR5
# Same list as jobs/br6_route/route_runs.py DATA_TABLES; pipeline_runs and settings are kept.
DATA_TABLES = [
    "dim_city",
    "dim_category",
    "dim_place_type",
    "category_place_type",
    "dim_area",
    "dim_date",
    "dim_place",
    "dim_visitor",
    "place_stats",
    "raw_checkin",
    "fact_checkin",
]
JOBS = "/opt/jobs/br6_route"
LAUNCH = ["docker", "exec", "-i", APP_SERVICE, "python3", f"{JOBS}/route_launch.py"]
URLS = {
    "Route Finder": "http://localhost:8501/route_finder",
    "BR6 Data": "http://localhost:8501/route_data",
    "BR6 Run History": "http://localhost:8501/route_run_history",
    "Grafana": "http://localhost:3000",
    "  Run performance": "http://localhost:3000/d/br6-run-performance",
    "  Resource use": "http://localhost:3000/d/br6-resource-use",
    "  Data and result": "http://localhost:3000/d/br6-data-result",
    "Spark master UI": "http://localhost:8082",
    "Spark history": "http://localhost:18080",
    "Prometheus": "http://localhost:9090",
}


def compose_up(services):
    run([
        "docker", "compose",
        "--profile", "full", "--profile", "monitoring", "--profile", "br6",
        "up", "-d", *services,
    ])


def start_services(with_app):
    build_image()
    compose_up(SERVICES + ([APP_SERVICE] if with_app else []))
    wait_healthy(["clickhouse"])
    create_clickhouse_tables(BR, truncate=False)


def clickhouse_value(sql):
    result = subprocess.run(
        ["docker", "exec", "-i", "clickhouse", "clickhouse-client",
         "--user", "default", "--password", "clickhouse", "--query", sql],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def busy_run():
    # Checked before touching any container: restarting Spark would kill a
    # running BR5 or BR6 job. Nothing can be running if ClickHouse is down.
    if container_health("clickhouse") != "healthy":
        return ""
    for db, br in (("br6", "BR6"), ("br5", "BR5")):
        try:
            busy = clickhouse_value(
                f"SELECT concat(run_type, ' (step: ', step, ')') FROM {db}.pipeline_runs FINAL "
                "WHERE status = 'running' AND heartbeat_at > now64(3) - INTERVAL 30 SECOND LIMIT 1"
            )
        except subprocess.CalledProcessError:  # the BR's tables don't exist yet
            continue
        if busy:
            return f"{br} {busy}"
    return ""


def refuse_if_busy():
    busy = busy_run()
    if busy:
        sys.exit(f"Busy: a {busy} is in progress. Try again when it has finished.")


def stored_checkins():
    return int(clickhouse_value("SELECT count() FROM br6.fact_checkin") or 0)


def launch(launch_args):
    """Runs route_launch.py in br5-app and shows its progress lines. The full
    job log is written to logs/br6/ by route_launch.py."""
    cmd = [*LAUNCH, *launch_args]
    print(f"$ {subprocess.list2cmdline(cmd)}")
    try:
        code = subprocess.run(cmd, cwd=REPO_ROOT).returncode
    except KeyboardInterrupt:
        sys.exit("Interrupted. The job may still be running in br5-app; stop it from the app.")
    if code != 0:
        sys.exit(code)


def run_import(args, replace=True):
    launch_args = ["import", "--multiplier", str(args.multiplier)]
    if args.cities:
        launch_args += ["--cities", args.cities]
    if not replace:
        launch_args.append("--if-empty")
    if args.writer:
        launch_args += ["--writer", args.writer]
    if args.write_partitions:
        launch_args += ["--write-partitions", str(args.write_partitions)]
    if args.preset:
        launch_args += ["--preset", args.preset]
    launch(launch_args)


def confirm_replace(args):
    stored = stored_checkins()
    if not stored or args.yes:
        return
    answer = input(f"Replace {stored:,} stored check-ins? [y/N] ").strip().lower()
    if answer != "y":
        sys.exit("Import cancelled.")


def print_urls():
    print()
    for name, url in URLS.items():
        print(f"{name:18} {url}")


def cmd_up(args):
    refuse_if_busy()
    start_services(with_app=True)
    cmd_status(args)


def cmd_import(args):
    refuse_if_busy()
    start_services(with_app=True)
    confirm_replace(args)
    run_import(args)


def cmd_fresh(args):
    refuse_if_busy()
    start_services(with_app=True)
    print("\nDropping BR6 data tables (run history and settings are kept)...")
    clickhouse_query("DROP DATABASE IF EXISTS br6_staging SYNC")
    for table in DATA_TABLES:
        clickhouse_query(f"DROP TABLE IF EXISTS br6.{table} SYNC")
    create_clickhouse_tables(BR, truncate=False)
    run_import(args)
    print_urls()


def cmd_run(args):
    refuse_if_busy()
    start_services(with_app=True)
    launch_args = ["run", "--city", args.city, "--start", str(args.start), "--end", str(args.end)]
    for option in ("stops", "types", "visitor", "preset"):
        value = getattr(args, option)
        if value not in (None, ""):
            launch_args += [f"--{option}", str(value)]
    launch(launch_args)


def cmd_status(args):
    run(["docker", "exec", "-i", APP_SERVICE, "python3", f"{JOBS}/route_status.py"])
    print_urls()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR6 start-up modes and data import")
    sub = parser.add_subparsers(dest="command", required=True)
    commands = (("up", cmd_up), ("fresh", cmd_fresh), ("import", cmd_import), ("run", cmd_run),
                ("status", cmd_status))
    for name, func in commands:
        p = sub.add_parser(name)
        p.set_defaults(func=func)
        if name in ("fresh", "import"):
            p.add_argument("--cities", default="", help="Comma-separated city file names, e.g. jakarta,tokyo")
            p.add_argument("--multiplier", type=int, default=1, help="Repeat the check-ins N times (synthetic)")
            p.add_argument("--writer", choices=("partitions", "pandas"), help="Default: the writer setting")
            p.add_argument("--write-partitions", type=int, help="Default: the write partitions setting")
            p.add_argument("--preset", help="Use this settings preset instead of the current settings")
            p.add_argument("--yes", action="store_true", help="Replace stored data without asking")
        if name == "run":
            p.add_argument("--city", required=True, help="City code, e.g. jakarta")
            p.add_argument("--start", type=int, required=True, help="venue_id of the start")
            p.add_argument("--end", type=int, required=True, help="venue_id of the end")
            p.add_argument("--stops", default="", help="Comma-separated venue_ids of up to 3 stops")
            p.add_argument("--types", default="", help="Comma-separated place types; default: all")
            p.add_argument("--visitor", type=int, help="user_id; default: General visitor")
            p.add_argument("--preset", help="Use this settings preset instead of the current settings")
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
