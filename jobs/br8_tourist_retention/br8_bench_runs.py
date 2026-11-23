"""BR8 benchmark names on top of run_store (jobs/common): database, tables,
data status, the busy check across the batch BRs sharing the Spark cluster,
and the result fingerprint. Built like BR6's route_runs.py."""

import run_store
from run_store import CURRENT, Stopped, failure_message, row_count, stop_on_sigterm, table_exists  # noqa: F401

DB = "br8"
STAGING_DB = "br8_staging"
PREFIX = "[br8]"
# Replaced together by an import. pipeline_runs, settings and run_* are kept.
DATA_TABLES = ["region", "place", "raw_checkin"]
# Other batch BRs on the same cluster (read-only): a run waits for theirs.
OTHER_DBS = {"br5": "BR5", "br6": "BR6", "br7": "BR7"}
# What a run's input (plan 3.1) means: a destination region, or ALL.
INPUTS = {"I1": "ALL", "I2": "Q1490", "I3": "Q1865"}


def Run(client, run_type, **fields):
    return run_store.Run(client, DB, run_type, prefix=PREFIX, **fields)


def busy_run(client):
    for db, br in [(DB, "BR8"), *OTHER_DBS.items()]:
        run = run_store.busy_run(client, db)
        if run:
            return {**run, "br": br}
    return None


def busy_message(busy):
    kind = "import" if busy["run_type"] == "import" else "run"
    return f"Busy: a {busy['br']} {kind} is in progress (step: {busy['step']})."


def load_settings(client, name):
    return run_store.load_settings(client, DB, name)


def stored_import(client):
    """Id of the stored data's import, or None."""
    if not row_count(client, DB, "raw_checkin"):
        return None
    run = run_store.latest_run(client, DB, "import", status="succeeded")
    return run["import_id"] if run else None


def region_where(region):
    """SQL filter of the raw check-ins for one input."""
    if region == "ALL":
        return "1"
    if not region.replace("_", "").isalnum():
        raise ValueError(f"Invalid region id {region!r}")
    return f"region_id = '{region}'"


# Same answer = same rows. Order-independent hash per result table, computed in
# ClickHouse for every engine (plan D7). Floats are stored, never computed, by a run.
FINGERPRINT_SQL = """
SELECT
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(
      event_id, user_id, place_id, visit_date, visit_time, raw_checkins))))
   FROM br8.run_visit WHERE run_id = {run_id:UUID}) AS visits,
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(
      place_id, place_name, category, region_id, city_name, country_name,
      ifNull(latitude, -999), ifNull(longitude, -999), visitors, returning_visitors, visits))))
   FROM br8.run_place WHERE run_id = {run_id:UUID}) AS places
"""


def fingerprint(client, run_id):
    row = next(client.query(FINGERPRINT_SQL, parameters={"run_id": run_id}).named_results())
    return f"visits {row['visits']} · places {row['places']}"


def result_summary(client, run_id):
    """Counts of the stored result: visits, places, visitor/place pairs and the
    returning ones (prepare_br8.py's summary.json figures)."""
    row = next(client.query("""
        SELECT (SELECT count() FROM br8.run_visit WHERE run_id = {run_id:UUID}) AS visits,
               count() AS places, sum(visitors) AS pairs, sum(returning_visitors) AS returning_pairs,
               (SELECT toString(min(visit_date)) FROM br8.run_visit WHERE run_id = {run_id:UUID}) AS first_day,
               (SELECT toString(max(visit_date)) FROM br8.run_visit WHERE run_id = {run_id:UUID}) AS last_day
        FROM br8.run_place WHERE run_id = {run_id:UUID}
    """, parameters={"run_id": run_id}).named_results())
    return {k: (int(v) if isinstance(v, int) else v) for k, v in row.items()}
