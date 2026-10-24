"""BR6 names on top of run_store (jobs/common): database, tables, the data
status and the busy check across BR5 and BR6.

Shared by the import job, the status command and the app, so the CLI and the
app see the same state.
"""

import route_reference as reference
import run_store
from run_store import CURRENT, Stopped, failure_message, row_count, stop_on_sigterm, table_exists  # noqa: F401

DB = "br6"
STAGING_DB = "br6_staging"
PREFIX = "[br6]"
# Replaced together by an import; dimensions first so the fact never points
# at a missing key. pipeline_runs and settings are never part of this list.
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
# Other BRs on the same Spark cluster: a BR6 job waits until theirs are done
# (plan-data-import.md, P6). Read-only.
OTHER_DBS = {"br5": "BR5"}


def Run(client, run_type, **fields):
    return run_store.Run(client, DB, run_type, prefix=PREFIX, **fields)


def busy_run(client):
    """The BR5 or BR6 run currently holding the shared cluster, with "br"
    added, or None."""
    for db, br in [(DB, "BR6"), *OTHER_DBS.items()]:
        run = run_store.busy_run(client, db)
        if run:
            return {**run, "br": br}
    return None


def busy_message(busy):
    kind = "import" if busy["run_type"] == "import" else "run"
    return f"Busy: a {busy['br']} {kind} is in progress (step: {busy['step']})."


def get_run(client, run_id):
    return run_store.get_run(client, DB, run_id)


def load_settings(client, name):
    return run_store.load_settings(client, DB, name)


def save_settings(client, name, values, deleted=False):
    run_store.save_settings(client, DB, name, values, deleted)


def preset_names(client):
    return run_store.preset_names(client, DB)


def stored_import(client):
    """import_id and multiplier of the live fact table, or (None, 1) when empty."""
    row = client.query(
        f"SELECT any(import_id) AS import_id, max(copy_no) + 1 AS copies, count() AS rows FROM {DB}.fact_checkin"
    ).first_item
    if not row["rows"]:
        return None, 1
    return str(row["import_id"]), int(row["copies"])


def city_table(client):
    """Per imported city: name, country, check-ins (original rows), without
    location, places, visitors."""
    return list(client.query(f"""
        SELECT c.city_code, c.city_name, c.country_name,
               r.checkins, r.no_location, p.places, v.visitors
        FROM {DB}.dim_city AS c
        LEFT JOIN (SELECT city, count() AS checkins, countIf(latitude IS NULL) AS no_location
                   FROM {DB}.raw_checkin GROUP BY city) AS r ON r.city = c.city_code
        LEFT JOIN (SELECT city_key, count() AS places FROM {DB}.dim_place GROUP BY city_key) AS p
               ON p.city_key = c.city_key
        LEFT JOIN (SELECT city_key, count() AS visitors FROM {DB}.dim_visitor GROUP BY city_key) AS v
               ON v.city_key = c.city_key
        ORDER BY c.city_name
    """).named_results())


def data_status(client):
    """What the Data screen and `make br6-status` show."""
    if not table_exists(client, DB, "fact_checkin"):
        return {"ready": False, "reason": "BR6 tables are missing. Run `make br6-up`."}
    counts = {t: row_count(client, DB, t) for t in DATA_TABLES}
    status = {"ready": True, "loaded": bool(counts["fact_checkin"]), "counts": counts, "import": None,
              "cities": []}
    if status["loaded"]:
        import_id, multiplier = stored_import(client)
        status["multiplier"] = multiplier
        status["import"] = run_store.latest_run(client, DB, "import", status="succeeded", import_id=import_id)
        status["cities"] = city_table(client)
    status["last_import_run"] = run_store.latest_run(client, DB, "import")
    status["busy"] = busy_run(client)
    status["place_stats_ready"] = place_stats_ready(client)
    return status


def place_stats_ready(client):
    """True when br6.place_stats holds one row per place of the stored data."""
    stats = row_count(client, DB, "place_stats")
    return bool(stats) and stats == row_count(client, DB, "dim_place")


# --- Route Finder (docs/plan/br6/plan-pipeline.md) ---------------------------

def refresh_grouping(client, path=reference.GROUPING_FILE):
    """Reloads the category -> place type file into category_place_type when
    the table differs from it (plan F9). The new table is built aside and
    swapped in, so a reader sees either the old or the new grouping.
    Returns {"sha1", "reloaded", "ungrouped", "per_type"}."""
    grouping = reference.load_grouping(path)
    categories = list(client.query(
        f"SELECT category_key, category_id, category_name FROM {DB}.dim_category ORDER BY category_key"
    ).named_results())
    _, mapping, missing = reference.place_type_rows(grouping, categories)
    wanted = sorted((m["category_key"], m["place_type_key"]) for m in mapping)
    stored = sorted(tuple(r) for r in client.query(
        f"SELECT category_key, place_type_key FROM {DB}.category_place_type").result_rows)
    reloaded = bool(categories) and wanted != stored
    if reloaded:
        aside = f"{DB}.category_place_type_reload"
        client.command(f"DROP TABLE IF EXISTS {aside}")
        client.command(f"CREATE TABLE {aside} AS {DB}.category_place_type")
        client.insert("category_place_type_reload", [list(row) for row in wanted],
                      column_names=["category_key", "place_type_key"], database=DB)
        client.command(f"EXCHANGE TABLES {DB}.category_place_type AND {aside}")
        client.command(f"DROP TABLE {aside}")
    names = {key: name for key, name in enumerate(reference.PLACE_TYPES, start=1)}
    per_type = {}
    for _, type_key in wanted:
        per_type[names[type_key]] = per_type.get(names[type_key], 0) + 1
    return {"sha1": reference.file_hash(path), "reloaded": reloaded, "ungrouped": missing, "per_type": per_type}


def city_by_code(client, city_code):
    result = client.query(f"SELECT * FROM {DB}.dim_city WHERE city_code = {{c:String}}", parameters={"c": city_code})
    return next(iter(result.named_results()), None)


def places_by_venue(client, city_key, venue_ids):
    """{venue_id: place row with its category and place type}."""
    if not venue_ids:
        return {}
    rows = client.query(f"""
        SELECT p.place_key AS place_key, p.venue_id AS venue_id, ifNull(p.name, '') AS name,
               p.latitude AS latitude, p.longitude AS longitude, p.has_location AS has_location,
               c.category_name AS category_name
        FROM {DB}.dim_place AS p
        INNER JOIN {DB}.dim_category AS c ON c.category_key = p.main_category_key
        WHERE p.city_key = {{city:UInt8}} AND p.venue_id IN {{venues:Array(UInt32)}}
    """, parameters={"city": city_key, "venues": [int(v) for v in venue_ids]}).named_results()
    return {r["venue_id"]: r for r in rows}


def visitor_by_user(client, city_key, user_id):
    """visitor_key of a user in a city, or None."""
    rows = client.query(
        f"SELECT visitor_key FROM {DB}.dim_visitor WHERE city_key = {{city:UInt8}} AND user_id = {{user:UInt32}}",
        parameters={"city": city_key, "user": int(user_id)},
    ).result_rows
    return rows[0][0] if rows else None


def latest_route_run(client):
    return run_store.latest_run(client, DB, "run", status="succeeded")


def run_points(client, run_id):
    return list(client.query(f"SELECT * FROM {DB}.run_point WHERE run_id = {{r:UUID}} ORDER BY seq",
                             parameters={"r": run_id}).named_results())


def run_places(client, run_id):
    """Every place of a run, suggestions first in rank order."""
    return list(client.query(
        f"SELECT * FROM {DB}.run_place WHERE run_id = {{r:UUID}} "
        "ORDER BY rank = 0, rank, score DESC, visitors DESC, checkins DESC, place_key",
        parameters={"r": run_id},
    ).named_results())


HISTORY_COLUMNS = ("run_id, run_type, status, step, started_at, finished_at, duration_s, outside_steps_s, "
                   "settings_from, engine, variant, city, visitor_kind, visitor, types, route, choice, multiplier, "
                   "rows_read, rows_used, rows_written, rows_excluded, step_durations, settings, criteria, resources, "
                   "spark_app_id, import_id, result_count, route_km, found, after_filters, suggested, below_top_n, "
                   "few_visitors, type_not_chosen, not_offered, error")


def run_history(client, limit=500, run_type=None):
    """Imports and runs, newest first (view br6.run_history, plan-monitoring.md H2)."""
    where = "WHERE run_type = {t:String}" if run_type else ""
    return list(client.query(
        f"SELECT {HISTORY_COLUMNS} FROM {DB}.run_history {where} ORDER BY started_at DESC LIMIT {int(limit)}",
        parameters={"t": run_type} if run_type else None,
    ).named_results())
