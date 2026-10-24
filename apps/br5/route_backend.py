"""What the app's BR6 (Route Finder) pages need from ClickHouse and Spark.

The BR6 counterpart of backend.py, which stays BR5's. Jobs are started through
jobs/br6_route/route_launch.py, the same code the CLI uses (`make br6-import`).
Run state lives in br6.pipeline_runs; the busy check also reads BR5's, because
the Spark cluster is shared (docs/plan/br6/plan-data-import.md, P6).
"""

import csv
import glob
import json
import os
import threading
import uuid
from pathlib import Path

import streamlit as st

import route_launch
import route_reference as reference
import route_rules as rules
import route_runs as runs
from clickhouse_io import get_client

DATA_DIR = os.environ.get("BR6_DATA_DIR", "/opt/data/br6")
SUFFIX = "_checkins.csv"
WRITE_STEPS = {"write_raw": "raw_checkin", "write_fact": "fact_checkin"}

# Opened by the user's browser, so host addresses (docker-compose.yml ports).
GRAFANA_URL = os.environ.get("BR6_GRAFANA_URL", "http://localhost:3000")
SPARK_HISTORY_URL = os.environ.get("BR6_SPARK_HISTORY_URL", "http://localhost:18080")
SPARK_LIVE_URL = os.environ.get("BR6_SPARK_LIVE_URL", "http://localhost:4040")
CATALOG = route_launch.CATALOG
# Not offered as a start, stop or end (spec 3.1).
PRIVATE_CATEGORIES = ("Home (private)",)
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
    return run_history(limit, "import")


def run_history(limit=200, run_type=None):
    where = f"WHERE run_type = '{run_type}'" if run_type else ""
    return list(client().query(
        "SELECT run_id, run_type, status, step, started_at, duration_ms, multiplier, writer, criteria, rows_read, "
        "rows_written, rows_used, result_count, step_durations, spark_app_id, error, settings "
        f"FROM {runs.DB}.pipeline_runs FINAL {where} ORDER BY started_at DESC LIMIT {int(limit)}"
    ).named_results())


def spark_page_url(app_id):
    return f"{SPARK_HISTORY_URL}/history/{app_id}/jobs/" if app_id else None


# --- Settings (Advanced Settings, Route Finder panel) ------------------------

def save_current(values):
    runs.save_settings(client(), runs.CURRENT, values)


@st.cache_data(ttl=30, show_spinner=False)
def cluster():
    return runs.run_store.cluster_info()


def validation(values):
    return CATALOG.validate(values, cluster())


def preset_names():
    saved = runs.preset_names(client())
    return list(CATALOG.presets) + [n for n in saved if n not in CATALOG.presets]


def preset_values(name):
    values, _ = route_launch.resolve_values(client(), name)
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


# --- Route Finder lists -------------------------------------------------------

def stored_import_id():
    return runs.stored_import(client())[0]


def grouping_hash():
    return reference.file_hash()


@st.cache_data(show_spinner=False)
def grouping(import_id, sha):
    """Reloads the category file into ClickHouse when it changed (plan F9);
    cached per import and file version."""
    return runs.refresh_grouping(client())


def _near_centre(prefix, city, max_centre_km):
    """SQL condition: the place (columns prefix.latitude/longitude) has a
    location within max_centre_km of the city centre on the flat map."""
    m = rules.FlatMap(city["centre_lat"], city["centre_lon"])
    k = repr(rules.KM_PER_DEGREE)
    x = f"(assumeNotNull({prefix}.longitude) - {m.lon0!r}) * {m.cos0!r} * {k}"
    y = f"(assumeNotNull({prefix}.latitude) - {m.lat0!r}) * {k}"
    return f"({prefix}.has_location = 1 AND sqrt({x} * {x} + {y} * {y}) <= {float(max_centre_km)!r})"


@st.cache_data(show_spinner=False)
def city_options(import_id, min_visitors, max_centre_km, sha):
    """Imported cities with their number of places to suggest: located, near
    the centre, an offered type, with the minimum of different visitors."""
    c = client()
    cities = list(c.query(f"SELECT * FROM {runs.DB}.dim_city ORDER BY city_name").named_results())
    for city in cities:
        city["places"] = int(c.command(f"""
            SELECT count() FROM {runs.DB}.dim_place AS p
            INNER JOIN {runs.DB}.place_stats AS s ON s.place_key = p.place_key
            INNER JOIN {runs.DB}.category_place_type AS g ON g.category_key = p.main_category_key
            INNER JOIN {runs.DB}.dim_place_type AS t ON t.place_type_key = g.place_type_key
            WHERE p.city_key = {int(city['city_key'])} AND t.is_offered = 1 AND s.visitors >= {int(min_visitors)}
              AND {_near_centre('p', city, max_centre_km)}"""))
    return cities


@st.cache_data(show_spinner=False, max_entries=200)
def search_places(city, text, max_centre_km, import_id, limit=50):
    """The most visited places of the city whose name contains text (any type
    except private homes), for the start, stop and end choices."""
    rows = client().query(f"""
        SELECT p.venue_id AS venue_id, ifNull(p.name, '') AS name, c.category_name AS category_name,
               ifNull(p.address, '') AS address, s.visitors AS visitors
        FROM {runs.DB}.dim_place AS p
        INNER JOIN {runs.DB}.dim_category AS c ON c.category_key = p.main_category_key
        LEFT JOIN {runs.DB}.place_stats AS s ON s.place_key = p.place_key
        WHERE p.city_key = {{city:UInt8}} AND {_near_centre('p', city, max_centre_km)}
          AND c.category_name NOT IN {{private:Array(String)}}
          AND positionCaseInsensitiveUTF8(ifNull(p.name, ''), {{text:String}}) > 0
        ORDER BY s.visitors DESC, p.venue_id
        LIMIT {int(limit)}
    """, parameters={"city": int(city["city_key"]), "private": list(PRIVATE_CATEGORIES), "text": text.strip()})
    return list(rows.named_results())


@st.cache_data(show_spinner="Loading the visitors…", max_entries=50)
def visitor_options(city, min_checkins, max_centre_km, import_id, sha):
    """Visitors with at least min_checkins used check-ins (offered place
    types, spec 3.5) in the city: user id, check-ins, favourite types."""
    rows = client().query(f"""
        SELECT v.user_id AS user_id, sum(n) AS checkins, groupArray((place_type_name, n)) AS per_type
        FROM (
            SELECT f.visitor_key AS visitor_key, t.place_type_name AS place_type_name, count() AS n
            FROM {runs.DB}.fact_checkin AS f
            INNER JOIN {runs.DB}.dim_place AS p ON p.place_key = f.place_key
            INNER JOIN {runs.DB}.category_place_type AS g ON g.category_key = p.main_category_key
            INNER JOIN {runs.DB}.dim_place_type AS t ON t.place_type_key = g.place_type_key
            WHERE f.city_key = {{city:UInt8}} AND f.copy_no = 0 AND t.is_offered = 1
              AND {_near_centre('p', city, max_centre_km)}
            GROUP BY visitor_key, place_type_name
        ) AS x
        INNER JOIN {runs.DB}.dim_visitor AS v ON v.visitor_key = x.visitor_key
        GROUP BY v.user_id
        HAVING checkins >= {{min:UInt32}}
        ORDER BY checkins DESC, v.user_id
    """, parameters={"city": int(city["city_key"]), "min": int(min_checkins)})
    return [{"user_id": r["user_id"], "checkins": int(r["checkins"]),
             "favourites": rules.favourite_types(dict(r["per_type"]))} for r in rows.named_results()]


def latest_attempt():
    return runs.run_store.latest_run(client(), runs.DB, "run")


def route_result(run_id=None):
    """The latest succeeded Route Finder run with its points and places, or
    None. With run_id: that run, if it is a succeeded Route Finder run."""
    if run_id:
        try:
            run = runs.get_run(client(), run_id)
        except Exception:  # not a UUID
            return None
        if not run or run["run_type"] != "run" or run["status"] != "succeeded":
            return None
    else:
        run = runs.latest_route_run(client())
    if run is None:
        return None
    run_id = str(run["run_id"])
    return {"run": run, "points": runs.run_points(client(), run_id), "places": runs.run_places(client(), run_id),
            "summary": json.loads(run["result_summary"] or "{}")}


# --- Jobs -------------------------------------------------------------------

def launch_job(kind, job_args):
    """Starts an import or a Route Finder run in the background with the
    current settings. Returns an error message or None."""
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
