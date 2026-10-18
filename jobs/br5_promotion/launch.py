"""Starts BR5 imports and analysis runs with the stored settings.

Used by the app (apps/br5/backend.py) and, as a CLI inside the br5-app
container, by scripts/br5.py, so a run started from the terminal is built
exactly like one started from the browser:

    python3 launch.py run --hotel "City Hotel" --country PRT --room-type A [--preset baseline]
    python3 launch.py import --multiplier 10 [--preset NAME] [--writer pandas]

The Spark driver runs in this container (client mode); the ClickHouse engine
runs as plain python3, without Spark.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import uuid
from pathlib import Path

import run_settings
import runs
from clickhouse_io import get_client

HERE = Path(__file__).resolve().parent
COMMON = HERE.parent / "common"
SCRIPTS = {"import": HERE / "job.py", "analysis": HERE / "analyze.py"}
LOG_DIR = Path(os.environ.get("BR5_LOG_DIR", "/opt/logs/br5"))
ECHO = ("[br5]", "Busy", "BR5", "No data", "Settings", "Invalid", "Traceback")


def resolve_values(client, preset=None):
    """(values, label): the current settings, or a built-in or saved preset."""
    if not preset:
        return run_settings.resolve(runs.load_settings(client, run_settings.CURRENT)), "current settings"
    if preset in run_settings.BUILTIN_PRESETS:
        return run_settings.resolve(preset=preset), f"preset {preset}"
    stored = runs.load_settings(client, preset)
    if stored is None:
        raise ValueError(f"No preset named '{preset}'")
    return run_settings.resolve(stored), f"preset {preset}"


def preflight(client, kind, values):
    """Why the job can't start now, or None."""
    if not runs.table_exists(client, runs.DB, "run_card"):
        return "BR5 tables are missing or outdated: run `make br5-up`."
    busy = runs.busy_run(client)
    if busy:
        return f"Busy: an {busy['run_type']} run is in progress (step: {busy['step']})."
    if kind == "analysis" and not runs.row_count(client, runs.DB, "fact_booking"):
        return "No data yet: import it first."
    errors, _ = run_settings.validate(values, runs.cluster_info())
    if errors:
        labels = run_settings.BY_KEY
        return "Settings can't be used: " + "; ".join(f"{labels[k].label} {msg}" for k, msg in errors.items())
    return None


def command(kind, values, job_args, label="current settings"):
    script = SCRIPTS[kind]
    args = [*job_args, "--settings-json", json.dumps(values), "--settings-label", label]
    if kind == "analysis" and values["pipeline.engine"] == "clickhouse":
        return ["python3", str(script), *args]
    py_files = sorted(COMMON.glob("*.py")) + sorted(p for p in HERE.glob("*.py") if p != script)
    return [
        "spark-submit",
        "--master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        # The driver JVM starts before the job can apply settings, so its memory goes here.
        "--driver-memory", values["spark.driver.memory"],
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", f"spark.driver.host={os.environ.get('SPARK_DRIVER_HOST', 'br5-app')}",
        "--py-files", ",".join(str(p) for p in py_files),
        str(script),
        *args,
    ]


def start(kind, job_args, values, run_id, stream=False, label="current settings"):
    """Starts the job in its own process group. Returns (process, log path).
    stream=True: the caller reads proc.stdout and writes the log itself."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"{kind}-{run_id}.log"
    cmd = command(kind, values, [*job_args, "--run-id", run_id], label)
    if stream:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                errors="replace", start_new_session=True)
    else:
        with log.open("w") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    return proc, log


def driver_pid(proc):
    """The job's Python process: proc itself (ClickHouse engine) or the Python
    driver that spark-submit's JVM started in the same process group."""
    scripts = {str(path) for path in SCRIPTS.values()}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            argv = Path(f"/proc/{entry}/cmdline").read_bytes().split(b"\0")
            if os.getpgid(int(entry)) != proc.pid:
                continue
        except OSError:
            continue
        args = [a.decode(errors="replace") for a in argv if a]
        if args and "python" in Path(args[0]).name and scripts & set(args):
            return int(entry)
    return None


def stop(proc, grace_seconds=20):
    """Stops a job: SIGTERM to its Python process first, so it records the run
    as stopped and shuts Spark down cleanly; the whole process group is killed
    if it is still running after the grace period."""
    if proc is None or proc.poll() is not None:
        return
    pid = driver_pid(proc)
    if pid is None:
        os.killpg(proc.pid, signal.SIGTERM)
        return
    os.kill(pid, signal.SIGTERM)

    def escalate():
        try:
            proc.wait(grace_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)

    threading.Thread(target=escalate, daemon=True).start()


def run_args(hotel, country, room_type):
    return ["--hotel", hotel, "--country", country, "--room-type", room_type]


def import_args(multiplier, replace=True, writer=None, write_partitions=None):
    args = ["--replace" if replace else "--if-empty", "--multiplier", str(multiplier)]
    if writer:
        args += ["--writer", writer]
    if write_partitions:
        args += ["--write-partitions", str(write_partitions)]
    return args


def main():
    parser = argparse.ArgumentParser(description="Start a BR5 import or analysis run with the stored settings")
    sub = parser.add_subparsers(dest="kind", required=True)
    run_p = sub.add_parser("run")
    run_p.add_argument("--hotel", default="All")
    run_p.add_argument("--country", default="All")
    run_p.add_argument("--room-type", default="All")
    imp = sub.add_parser("import")
    imp.add_argument("--multiplier", type=int, default=1)
    imp.add_argument("--if-empty", action="store_true")
    imp.add_argument("--writer", choices=("partitions", "pandas"), help="Overrides the setting")
    imp.add_argument("--write-partitions", type=int, help="Overrides the setting")
    for p in (run_p, imp):
        p.add_argument("--preset", help="Use this preset instead of the current settings")
    args = parser.parse_args()

    kind = "analysis" if args.kind == "run" else "import"
    client = get_client(autogenerate_session_id=False)
    try:
        values, label = resolve_values(client, args.preset)
    except ValueError as exc:
        sys.exit(str(exc))
    problem = preflight(client, kind, values)
    if problem:
        sys.exit(problem)
    if kind == "analysis":
        job_args = run_args(args.hotel, args.country, args.room_type)
    else:
        job_args = import_args(args.multiplier, not args.if_empty, args.writer, args.write_partitions)

    run_id = str(uuid.uuid4())
    proc, log = start(kind, job_args, values, run_id, stream=True, label=label)
    print(f"[br5] {kind} {run_id} with {label}; log: {log}", flush=True)
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
        sys.exit(f"{kind.capitalize()} stopped.")
    if code != 0:
        sys.exit(f"{kind.capitalize()} failed (exit code {code}). Log: {log}")


if __name__ == "__main__":
    main()
