"""What the BR5 app needs from ClickHouse and Spark.

The import itself is jobs/br5_promotion/job.py, launched here with
spark-submit (this container is built from the Spark image). Run state lives in
br5.pipeline_runs, so the app and `make br5-import` see the same busy lock.
"""

import os
import subprocess
import threading
import uuid
from pathlib import Path

import streamlit as st

import runs
from clickhouse_io import get_client

JOB = Path("/opt/jobs/br5_promotion/job.py")
COMMON = Path("/opt/jobs/common")
LOG_DIR = Path(os.environ.get("BR5_LOG_DIR", "/opt/logs/br5"))
WRITE_STEPS = {"write_raw": "raw_booking", "write_fact": "fact_booking"}


@st.cache_resource
def client():
    # No session id, so the shared client can serve concurrent page reruns.
    return get_client(autogenerate_session_id=False)


@st.cache_resource
def launcher():
    # Shared by every browser tab: the import process this app started.
    return {"lock": threading.Lock(), "proc": None, "run_id": None, "log": None}


def data_status():
    return runs.data_status(client())


def spark_submit_cmd(job_args):
    py_files = sorted(COMMON.glob("*.py")) + sorted(p for p in JOB.parent.glob("*.py") if p != JOB)
    return [
        "spark-submit",
        "--master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "--driver-memory", os.environ.get("SPARK_DRIVER_MEMORY", "512m"),
        "--executor-memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "512m"),
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", f"spark.cores.max={os.environ.get('SPARK_CORES_MAX', '2')}",
        "--conf", f"spark.sql.shuffle.partitions={os.environ.get('SPARK_SHUFFLE_PARTITIONS', '4')}",
        "--conf", f"spark.driver.host={os.environ.get('SPARK_DRIVER_HOST', 'br5-app')}",
        "--py-files", ",".join(str(p) for p in py_files),
        str(JOB),
        *job_args,
    ]


def launch_import(multiplier, writer, write_partitions):
    """Starts the import in the background. Returns an error message or None."""
    state = launcher()
    with state["lock"]:
        if state["proc"] is not None and state["proc"].poll() is None:
            return "An import started from the app is still running."
        busy = runs.busy_run(client())
        if busy:
            return f"Busy: a {busy['run_type']} run is in progress (step: {busy['step']})."
        run_id = str(uuid.uuid4())
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log = LOG_DIR / f"import-{run_id}.log"
        cmd = spark_submit_cmd([
            "--replace",
            "--multiplier", str(multiplier),
            "--writer", writer,
            "--write-partitions", str(write_partitions),
            "--run-id", run_id,
        ])
        with log.open("w") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        state.update(proc=proc, run_id=run_id, log=log)
    return None


def launched_job():
    """The import this app started last: process state, its run row, log tail."""
    state = launcher()
    proc = state["proc"]
    if proc is None:
        return None
    run = runs.get_run(client(), state["run_id"])
    return {
        "running": proc.poll() is None,
        "exit_code": proc.poll(),
        "run": run,
        "log_tail": log_tail(state["log"]),
        "log": state["log"],
    }


def log_tail(path, lines=15):
    try:
        return "\n".join(Path(path).read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return ""


def write_progress(run):
    """Rows written so far / expected, while a write step runs; else None."""
    table = WRITE_STEPS.get(run["step"]) if run else None
    if not table or not run["rows_expected"]:
        return None
    written = runs.row_count(client(), runs.STAGING_DB, table) or 0
    return min(written / run["rows_expected"], 1.0), written
