"""BR8 benchmark launcher (docs/plan/br7/plan-benchmark.md). Built like
scripts/br6.py, without an app: Grafana is the only UI.

    python3 scripts/br8.py up                      start the stack for the benchmark (data kept)
    python3 scripts/br8.py import [--replace]      download the TripWorld files if needed, import them
    python3 scripts/br8.py run --input I1          one run (I1=ALL, I2=Q1490 Tokyo, I3=Q1865 Kuala Lumpur)
    python3 scripts/br8.py run --region Q35765 --preset bench-default
    python3 scripts/br8.py status                  what is stored, latest runs
    python3 scripts/br8.py settings                current settings and presets

Imports and runs are started by jobs/br8_tourist_retention/br8_bench_launch.py
inside spark-local. The stream demo (make demo-br BR=8) is separate and unchanged.
"""

import argparse
import subprocess
import sys

from start import REPO_ROOT, build_image, container_health, create_clickhouse_tables, run, wait_healthy

BR = "8"
SERVICES = ["clickhouse", "spark-master", "spark-worker", "spark-local", "prometheus", "grafana",
            "spark-history", "docker-stats"]
JOBS = "/opt/jobs/br8_tourist_retention"
# The Spark driver runs in spark-local (client mode): executors connect back to it by this name.
LAUNCH = ["docker", "exec", "-i", "-e", "SPARK_DRIVER_HOST=spark-local", "spark-local",
          "python3", f"{JOBS}/br8_bench_launch.py"]
INPUTS = {"I1": "ALL", "I2": "Q1490", "I3": "Q1865"}
URLS = {
    "Grafana BR8 benchmark": "http://localhost:3000/d/br8-benchmark",
    "Spark master UI": "http://localhost:8082",
    "Spark history": "http://localhost:18080",
    "Prometheus": "http://localhost:9090",
}


def compose_up(services):
    # Profile br5 holds spark-history and docker-stats (shared monitoring of the batch BRs).
    run(["docker", "compose", "--profile", "full", "--profile", "monitoring", "--profile", "br5",
         "up", "-d", *services])


def start_services():
    build_image()
    compose_up(SERVICES)
    wait_healthy(["clickhouse"])
    create_clickhouse_tables(BR, truncate=False)


def busy_run():
    if container_health("clickhouse") != "healthy":
        return ""
    for db, br in (("br8", "BR8"), ("br7", "BR7"), ("br6", "BR6"), ("br5", "BR5")):
        result = subprocess.run(
            ["docker", "exec", "-i", "clickhouse", "clickhouse-client", "--user", "default", "--password",
             "clickhouse", "--query",
             f"SELECT concat(run_type, ' (step: ', step, ')') FROM {db}.pipeline_runs FINAL "
             "WHERE status = 'running' AND heartbeat_at > now64(3) - INTERVAL 30 SECOND LIMIT 1"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            return f"{br} {result.stdout.strip()}"
    return ""


def refuse_if_busy():
    busy = busy_run()
    if busy:
        sys.exit(f"Busy: a {busy} is in progress. Try again when it has finished.")


def launch(launch_args):
    cmd = [*LAUNCH, *launch_args]
    print(f"$ {subprocess.list2cmdline(cmd)}", flush=True)
    try:
        code = subprocess.run(cmd, cwd=REPO_ROOT).returncode
    except KeyboardInterrupt:
        sys.exit("Interrupted. The job may still be running in spark-local.")
    if code != 0:
        sys.exit(code)


def print_urls():
    print()
    for name, url in URLS.items():
        print(f"{name:22} {url}")


def cmd_up(args):
    refuse_if_busy()
    start_services()
    cmd_status(args)


def cmd_import(args):
    refuse_if_busy()
    # Same pinned files and checksums as the stream demo; skipped when already downloaded.
    run([sys.executable, str(REPO_ROOT / "scripts" / "prepare_br8.py"), "--download-only"])
    start_services()
    launch_args = ["import"] + (["--replace"] if args.replace else [])
    if args.writer:
        launch_args += ["--writer", args.writer]
    if args.preset:
        launch_args += ["--preset", args.preset]
    launch(launch_args)


def cmd_run(args):
    refuse_if_busy()
    start_services()
    region = INPUTS[args.input] if args.input else args.region
    if not region:
        sys.exit("Give --input I1|I2|I3 or --region <id|ALL>.")
    launch_args = ["run", "--region", region]
    if args.preset:
        launch_args += ["--preset", args.preset]
    for tag in args.tag:
        launch_args += ["--tag", tag]
    launch(launch_args)


def cmd_status(args):
    run(["docker", "exec", "-i", "spark-local", "python3", f"{JOBS}/br8_bench_status.py"])
    print_urls()


def cmd_settings(args):
    launch(["settings"])


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR8 benchmark launcher")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, func in (("up", cmd_up), ("import", cmd_import), ("run", cmd_run), ("status", cmd_status),
                       ("settings", cmd_settings)):
        p = sub.add_parser(name)
        p.set_defaults(func=func)
        if name == "import":
            p.add_argument("--replace", action="store_true", help="Replace the stored data")
            p.add_argument("--writer", choices=("partitions", "pandas"))
            p.add_argument("--preset")
        if name == "run":
            p.add_argument("--input", choices=sorted(INPUTS))
            p.add_argument("--region", help="Region id (Wikidata) or ALL")
            p.add_argument("--preset", help="Use this preset instead of the current settings")
            p.add_argument("--tag", action="append", default=[], help="key=value stored with the run")
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
