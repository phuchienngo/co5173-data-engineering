"""What the BR5 app needs from ClickHouse and Spark.

Jobs are started through jobs/br5_promotion/launch.py, the same code the CLI
uses (`make br5-run`, `make br5-import`). Run state lives in br5.pipeline_runs,
so the app and the CLI see the same busy lock; settings live in br5.settings.
"""

import json
import threading
import uuid
from pathlib import Path

import streamlit as st

import launch
import run_settings
import runs
from clickhouse_io import get_client

WRITE_STEPS = {"write_raw": "raw_booking", "write_fact": "fact_booking"}


@st.cache_resource
def client():
    # No session id, so the shared client can serve concurrent page reruns.
    return get_client(autogenerate_session_id=False)


@st.cache_resource
def launcher():
    # Shared by every browser tab: the job this app started last.
    return {"lock": threading.Lock(), "proc": None, "run_id": None, "log": None, "kind": None}


def data_status():
    return runs.data_status(client())


# --- Settings ---------------------------------------------------------------

def current_values():
    return run_settings.resolve(runs.load_settings(client(), run_settings.CURRENT))


def save_current(values):
    runs.save_settings(client(), run_settings.CURRENT, values)


@st.cache_data(ttl=30, show_spinner=False)
def cluster():
    return runs.cluster_info()


def validation(values):
    return run_settings.validate(values, cluster())


def preset_names():
    saved = runs.preset_names(client(), run_settings.CURRENT)
    return list(run_settings.BUILTIN_PRESETS) + [n for n in saved if n not in run_settings.BUILTIN_PRESETS]


def preset_values(name):
    values, _ = launch.resolve_values(client(), name)
    return values


def save_preset(name, values):
    runs.save_settings(client(), name, values)


def delete_preset(name):
    runs.save_settings(client(), name, {}, deleted=True)


def server_limits():
    """Values that need a restart, read from the running stack (C7)."""
    limits = {"cluster": cluster()}
    try:
        metrics = dict(client().query(
            "SELECT metric, value FROM system.asynchronous_metrics "
            "WHERE metric IN ('CGroupMemoryTotal', 'CGroupMaxCPU', 'OSMemoryTotal', 'NumberOfCPUCores')"
        ).result_rows)
        server = dict(client().query(
            "SELECT name, value FROM system.server_settings "
            "WHERE name IN ('max_server_memory_usage', 'max_server_memory_usage_to_ram_ratio', "
            "'background_pool_size')"
        ).result_rows)
        limits["clickhouse"] = {**metrics, **server}
    except Exception as exc:  # shown as unavailable, never breaks the page
        limits["clickhouse_error"] = str(exc)
    return limits


# --- Jobs -------------------------------------------------------------------

def launch_job(kind, job_args):
    """Starts an import or analysis run in the background with the current
    settings. Returns an error message or None."""
    state = launcher()
    with state["lock"]:
        if state["proc"] is not None and state["proc"].poll() is None:
            return "A job started from the app is still running."
        values = current_values()
        problem = launch.preflight(client(), kind, values)
        if problem:
            return problem
        run_id = str(uuid.uuid4())
        proc, log = launch.start(kind, job_args, values, run_id)
        state.update(proc=proc, run_id=run_id, log=log, kind=kind)
    return None


def stop_job():
    launch.stop(launcher()["proc"])


def launched_job(kind=None):
    """The job this app started last: process state, its run row, log tail."""
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
    """Rows written so far / expected, while an import write step runs; else None."""
    table = WRITE_STEPS.get(run["step"]) if run else None
    if not table or not run["rows_expected"]:
        return None
    written = runs.row_count(client(), runs.STAGING_DB, table) or 0
    return min(written / run["rows_expected"], 1.0), written


# --- Promotion Finder -------------------------------------------------------

@st.cache_data(show_spinner=False)
def choices(import_id, min_bookings, remove_duplicates):
    """Hotels, and countries and room types with enough valid bookings
    (cleaning rules applied, divided by the multiplier). Cached per import and
    per the two settings that change the lists."""
    c = client()
    _, multiplier = runs.stored_import(c)
    valid = ("is_canceled = 0 AND stay_nights > 0 AND adults + ifNull(children, 0) + babies > 0"
             + (" AND is_duplicate = 0" if remove_duplicates else ""))
    params = {"m": multiplier, "min": min_bookings}
    hotels = [r[0] for r in c.query("SELECT hotel_name FROM br5.dim_hotel ORDER BY hotel_name").result_rows]
    countries = c.query(
        "SELECT d.country_code, d.country_name FROM br5.dim_country AS d INNER JOIN ("
        f"  SELECT country_key, count() / {{m:UInt32}} AS n FROM br5.fact_booking WHERE {valid} "
        "   GROUP BY country_key HAVING n >= {min:UInt32}) AS f USING (country_key) "
        "WHERE d.country_key != 0 ORDER BY d.country_name",
        parameters=params,
    ).result_rows
    rooms = c.query(
        "SELECT d.room_type_code FROM br5.dim_room_type AS d INNER JOIN ("
        "  SELECT reserved_room_type_key AS room_type_key, count() / {m:UInt32} AS n FROM br5.fact_booking "
        f"  WHERE {valid} GROUP BY room_type_key HAVING n >= {{min:UInt32}}) AS f USING (room_type_key) "
        "ORDER BY d.room_type_code",
        parameters=params,
    ).result_rows
    return hotels, dict(countries), [r[0] for r in rooms]


def latest_result():
    """The latest succeeded analysis run with its months and cards, or None."""
    run = runs.latest_analysis(client())
    if run is None:
        return None
    months, cards = runs.result_rows(client(), str(run["run_id"]))
    return {"run": run, "months": months, "cards": cards,
            "summary": json.loads(run["result_summary"] or "{}")}


def latest_attempt():
    return runs.latest_analysis(client(), succeeded_only=False)


def stored_import_id():
    import_id, _ = runs.stored_import(client())
    return import_id
