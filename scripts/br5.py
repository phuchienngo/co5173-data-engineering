"""BR5 launcher: start-up modes and data import.

    python3 scripts/br5.py up        start the stack and the app (data kept)
    python3 scripts/br5.py fresh     drop BR5 data, import, then start the app
    python3 scripts/br5.py import    import (replaces stored data after asking)
    python3 scripts/br5.py status    show what is stored

Plan: docs/plan/br5/plan-data-import.md. Kafka is not started: BR5 has no
streaming input.
"""

import argparse
import os
import subprocess
import sys
import uuid
from pathlib import Path

from start import (
    REPO_ROOT,
    build_image,
    clickhouse_query,
    container_health,
    create_clickhouse_tables,
    run,
    spark_submit_cmd,
    wait_healthy,
)

BR = "5"
SPARK_MASTER = "spark://spark-master:7077"
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
LOG_DIR = Path(os.environ.get("BR5_LOG_DIR", REPO_ROOT / "logs" / "br5"))
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


def run_import(args, replace=True):
    job_args = [
        "--replace" if replace else "--if-empty",
        "--multiplier", str(args.multiplier),
        "--writer", args.writer,
        "--write-partitions", str(args.write_partitions),
    ]
    run_id = str(uuid.uuid4())
    job_args += ["--run-id", run_id]
    os.environ.setdefault("SPARK_CORES_MAX", "2")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"import-{run_id}.log"
    print(f"Import log: {log_file}")
    cmd = [str(part) for part in spark_submit_cmd(BR, SPARK_MASTER, job_args=job_args, packages=None)]
    print(f"$ {' '.join(cmd)}")
    with log_file.open("w", encoding="utf-8") as log:
        proc = subprocess.Popen(
            cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
        )
        try:
            for line in proc.stdout:
                log.write(line)
                # The job's own lines; Spark's output stays in the log file.
                if line.startswith("[br5]") or line.startswith("Busy") or line.startswith("BR5"):
                    print(line, end="", flush=True)
            code = proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
            proc.wait()
            sys.exit("Import interrupted.")
    if code != 0:
        sys.exit(f"Import failed (exit code {code}). See {log_file}")


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
    start_services(with_app=False)
    confirm_replace(args)
    run_import(args)


def cmd_fresh(args):
    refuse_if_busy()
    start_services(with_app=False)
    print("\nDropping BR5 data tables (run history is kept)...")
    clickhouse_query("DROP DATABASE IF EXISTS br5_staging SYNC")
    for table in DATA_TABLES:
        clickhouse_query(f"DROP TABLE IF EXISTS br5.{table} SYNC")
    create_clickhouse_tables(BR, truncate=False)
    run_import(args)
    compose_up([APP_SERVICE])
    print_urls()


def cmd_status(args):
    run(["docker", "exec", "-i", "spark-local", "python3", "/opt/jobs/br5_promotion/status.py"])


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR5 start-up modes and data import")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, func in (("up", cmd_up), ("fresh", cmd_fresh), ("import", cmd_import), ("status", cmd_status)):
        p = sub.add_parser(name)
        p.set_defaults(func=func)
        if name in ("fresh", "import"):
            p.add_argument("--multiplier", type=int, default=1, help="Repeat the dataset N times (synthetic)")
            p.add_argument("--writer", choices=("partitions", "pandas"), default="partitions")
            p.add_argument("--write-partitions", type=int, default=2)
            p.add_argument("--yes", action="store_true", help="Replace stored data without asking")
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
