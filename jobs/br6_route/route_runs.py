"""BR6 names on top of run_store (jobs/common): database, tables, the data
status and the busy check across BR5 and BR6.

Shared by the import job, the status command and the app, so the CLI and the
app see the same state.
"""

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
    return status
