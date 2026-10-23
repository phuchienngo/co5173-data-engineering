"""What the app's BR6 (Route Finder) pages need from ClickHouse and Spark.

The BR6 counterpart of backend.py, which stays BR5's. Jobs are started through
jobs/br6_route/route_launch.py, the same code the CLI uses (`make br6-import`).
Run state lives in br6.pipeline_runs; the busy check also reads BR5's, because
the Spark cluster is shared (docs/plan/br6/plan-data-import.md, P6).
"""

import csv
import glob
import os
import threading
import uuid
from pathlib import Path

import streamlit as st

import route_launch
import route_runs as runs
from clickhouse_io import get_client

DATA_DIR = os.environ.get("BR6_DATA_DIR", "/opt/data/br6")
SUFFIX = "_checkins.csv"
WRITE_STEPS = {"write_raw": "raw_checkin", "write_fact": "fact_checkin"}

# Opened by the user's browser, so host addresses (docker-compose.yml ports).
GRAFANA_URL = os.environ.get("BR6_GRAFANA_URL", "http://localhost:3000")
SPARK_HISTORY_URL = os.environ.get("BR6_SPARK_HISTORY_URL", "http://localhost:18080")
# Added by the monitoring plan (docs/plan/br6, M6-M8).
DASHBOARDS = {}


def dashboard_url(name):
    return f"{GRAFANA_URL}/d/{DASHBOARDS[name]}"


def monitoring_buttons():
    for name in DASHBOARDS:
        st.sidebar.link_button(f"Grafana: {name}", dashboard_url(name), width="stretch", key=f"br6-{name}")
    st.sidebar.link_button("Spark history", SPARK_HISTORY_URL, width="stretch", key="br6-spark-history")


def monitoring_links():
    """Sidebar links on BR6 pages."""
    st.sidebar.markdown("**Monitoring**")
    monitoring_buttons()


@st.cache_resource
def client():
    # No session id, so the shared client can serve concurrent page reruns.
    return get_client(autogenerate_session_id=False)


@st.cache_resource
def launcher():
    # Shared by every browser tab: the BR6 job this app started last.
    return {"lock": threading.Lock(), "proc": None, "run_id": None, "log": None, "kind": None}


@st.cache_data(show_spinner=False)
def city_names():
    """{city_code: (city name, country name)} from data/br6/cities.csv."""
    with open(os.path.join(DATA_DIR, "cities.csv"), encoding="utf-8") as f:
        return {r["city_code"]: (r["city_name"], r["country_name"]) for r in csv.DictReader(f)}


def available_cities():
    return sorted(os.path.basename(p)[:-len(SUFFIX)] for p in glob.glob(os.path.join(DATA_DIR, "*" + SUFFIX)))


def data_status():
    return runs.data_status(client())


def busy():
    return runs.busy_run(client())


def busy_message(run):
    return runs.busy_message(run)


def current_values():
    return route_launch.CATALOG.resolve(runs.load_settings(client(), runs.CURRENT))


def import_history(limit=200):
    return list(client().query(
        f"SELECT run_id, status, step, started_at, duration_ms, multiplier, writer, criteria, rows_read, "
        f"rows_written, step_durations, spark_app_id, error FROM {runs.DB}.pipeline_runs FINAL "
        f"WHERE run_type = 'import' ORDER BY started_at DESC LIMIT {int(limit)}"
    ).named_results())


# --- Jobs -------------------------------------------------------------------

def launch_job(kind, job_args):
    """Starts an import in the background with the current settings. Returns
    an error message or None."""
    state = launcher()
    with state["lock"]:
        if state["proc"] is not None and state["proc"].poll() is None:
            return "A job started from the app is still running."
        values = current_values()
        problem = route_launch.preflight(client(), kind, values)
        if problem:
            return problem
        run_id = str(uuid.uuid4())
        proc, log = route_launch.start(kind, job_args, values, run_id)
        state.update(proc=proc, run_id=run_id, log=log, kind=kind)
    return None


def stop_job():
    route_launch.stop(launcher()["proc"])


def launched_job(kind=None):
    """The BR6 job this app started last: process state, its run row, log tail."""
    state = launcher()
    proc = state["proc"]
    if proc is None or (kind and state["kind"] != kind):
        return None
    return {
        "kind": state["kind"],
        "running": proc.poll() is None,
        "exit_code": proc.poll(),
        "run": runs.get_run(client(), state["run_id"]),
        "log_tail": log_tail(state["log"]),
        "log": state["log"],
    }


def log_tail(path, lines=15):
    try:
        return "\n".join(Path(path).read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return ""


def write_progress(run):
    """Rows written so far / expected, while an import write step runs; else
    None. The raw table holds the original rows, the fact table ×N."""
    table = WRITE_STEPS.get(run["step"]) if run else None
    if not table or not run["rows_expected"]:
        return None
    expected = run["rows_read"] if table == "raw_checkin" else run["rows_expected"]
    written = runs.row_count(client(), runs.STAGING_DB, table) or 0
    return min(written / expected, 1.0), written, expected
