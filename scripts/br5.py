"""BR5 launcher: start-up modes and data import.

    python3 scripts/br5.py up        start the stack and the app (data kept)
    python3 scripts/br5.py fresh     drop BR5 data, import, then start the app
    python3 scripts/br5.py import    import (replaces stored data after asking)
    python3 scripts/br5.py run       run the promotion analysis (needs `up` first)
    python3 scripts/br5.py status    show what is stored

Imports and runs are started by jobs/br5_promotion/launch.py inside the br5-app
container, the same code the app's buttons use, with the stored settings (or
--preset NAME). Plans: docs/plan/br5/plan-data-import.md, plan-pipeline.md.
Kafka is not started: BR5 has no streaming input.
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

BR = "5"
SERVICES = ["clickhouse", "spark-master", "spark-worker", "spark-local", "prometheus", "grafana"]
APP_SERVICE = "br5-app"
# Same list as jobs/br5_promotion/runs.py DATA_TABLES; pipeline_runs is kept.
DATA_TABLES = [
    "dim_date",
    "dim_hotel",
    "dim_country",
    "dim_room_type",
    "dim_channel",
    "dim_booking_profile",
    "raw_booking",
    "fact_booking",
]
LAUNCH = ["docker", "exec", "-i", APP_SERVICE, "python3", "/opt/jobs/br5_promotion/launch.py"]
URLS = {
    "BR5 app": "http://localhost:8501",
    "Grafana": "http://localhost:3000",
    "Spark master UI": "http://localhost:8082",
}


def compose_up(services):
    run([
        "docker", "compose",
        "--profile", "full", "--profile", "monitoring", "--profile", "br5",
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
    # running import. Nothing can be running if ClickHouse or the table is down.
    if container_health("clickhouse") != "healthy":
        return ""
    try:
        return clickhouse_value(
            "SELECT concat(run_type, ' (step: ', step, ')') FROM br5.pipeline_runs FINAL "
            "WHERE status = 'running' AND heartbeat_at > now64(3) - INTERVAL 30 SECOND LIMIT 1"
        )
    except subprocess.CalledProcessError:
        return ""


def refuse_if_busy():
    busy = busy_run()
    if busy:
        sys.exit(f"Busy: a BR5 {busy} is in progress. Try again when it has finished.")


def stored_bookings():
    return int(clickhouse_value("SELECT count() FROM br5.fact_booking") or 0)


def launch(launch_args):
    """Runs launch.py in br5-app and shows its progress lines. The full job
    log is written to logs/br5/ by launch.py."""
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
    if not replace:
        launch_args.append("--if-empty")
    if args.writer:
        launch_args += ["--writer", args.writer]
    if args.write_partitions:
        launch_args += ["--write-partitions", str(args.write_partitions)]
    if args.preset:
        launch_args += ["--preset", args.preset]
    launch(launch_args)


def app_running():
    result = subprocess.run(["docker", "inspect", "--format", "{{.State.Running}}", APP_SERVICE],
                            cwd=REPO_ROOT, capture_output=True, text=True)
    return result.stdout.strip() == "true"


def confirm_replace(args):
    stored = stored_bookings()
    if not stored or args.yes:
        return
    answer = input(f"Replace {stored:,} stored bookings? [y/N] ").strip().lower()
    if answer != "y":
        sys.exit("Import cancelled.")


def print_urls():
    print()
    for name, url in URLS.items():
        print(f"{name:16} {url}")


def cmd_up(args):
    refuse_if_busy()
    start_services(with_app=True)
    cmd_status(args)
    print_urls()


def cmd_import(args):
    refuse_if_busy()
    start_services(with_app=True)
    confirm_replace(args)
    run_import(args)


def cmd_fresh(args):
    refuse_if_busy()
    start_services(with_app=True)
    print("\nDropping BR5 data tables (run history, settings and results are kept)...")
    clickhouse_query("DROP DATABASE IF EXISTS br5_staging SYNC")
    for table in DATA_TABLES:
        clickhouse_query(f"DROP TABLE IF EXISTS br5.{table} SYNC")
    create_clickhouse_tables(BR, truncate=False)
    run_import(args)
    print_urls()


def cmd_run(args):
    if not app_running():
        sys.exit("The BR5 app container is not running: start the stack with `make br5-up` first.")
    launch_args = ["run", "--hotel", args.hotel, "--country", args.country, "--room-type", args.room_type]
    if args.preset:
        launch_args += ["--preset", args.preset]
    launch(launch_args)


def cmd_status(args):
    run(["docker", "exec", "-i", "spark-local", "python3", "/opt/jobs/br5_promotion/status.py"])


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR5 start-up modes and data import")
    sub = parser.add_subparsers(dest="command", required=True)
    commands = (("up", cmd_up), ("fresh", cmd_fresh), ("import", cmd_import), ("run", cmd_run),
                ("status", cmd_status))
    for name, func in commands:
        p = sub.add_parser(name)
        p.set_defaults(func=func)
        if name in ("fresh", "import"):
            p.add_argument("--multiplier", type=int, default=1, help="Repeat the dataset N times (synthetic)")
            p.add_argument("--writer", choices=("partitions", "pandas"), help="Default: the writer setting")
            p.add_argument("--write-partitions", type=int, help="Default: the write partitions setting")
            p.add_argument("--yes", action="store_true", help="Replace stored data without asking")
        if name == "run":
            p.add_argument("--hotel", default="All", help='Hotel name, for example "City Hotel", or All')
            p.add_argument("--country", default="All", help="Country code, for example PRT, or All")
            p.add_argument("--room-type", default="All", help="Room type code, for example A, or All")
        if name in ("fresh", "import", "run"):
            p.add_argument("--preset", help="Use this settings preset instead of the current settings")
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
