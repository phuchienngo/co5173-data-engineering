"""BR7 benchmark names on top of run_store (jobs/common): database, tables,
inputs, the busy check across the batch BRs sharing the Spark cluster, and the
result fingerprint. Built like br8_bench_runs.py."""

import re

import run_store
from run_store import CURRENT, Stopped, failure_message, row_count, stop_on_sigterm, table_exists  # noqa: F401

DB = "br7"
STAGING_DB = "br7_staging"
PREFIX = "[br7]"
# Replaced together by an import. pipeline_runs, settings and run_* are kept.
DATA_TABLES = ["zone", "raw_trip"]
# Other batch BRs on the same cluster (read-only): a run waits for theirs.
OTHER_DBS = {"br5": "BR5", "br6": "BR6", "br8": "BR8"}

# The pinned TLC months of scripts/prepare_br7.py: 12 training months and the replay month.
ALL_MONTHS = ["2025-09", "2025-10", "2025-11", "2025-12"] + [f"2026-{m:02d}" for m in range(1, 10)]
REPLAY_MONTH = "2026-09"
# What a run's input (plan 3.1) means: its first training month. Training runs to
# the month before the replay month, as prepare.py's TRAIN_MONTHS.
INPUTS = {"I1": "2025-09", "I2": "2026-03", "I3": "2026-07"}
MONTH = re.compile(r"^\d{4}-\d{2}$")


def train_months(first_month):
    """Training months from first_month to the month before the replay month."""
    if not MONTH.match(first_month or "") or first_month not in ALL_MONTHS[:-1]:
        raise ValueError(f"Invalid first training month {first_month!r}; choose one of {ALL_MONTHS[:-1]}")
    return ALL_MONTHS[ALL_MONTHS.index(first_month):-1]


def month_where(first_month):
    """SQL filter of the raw trips for one input: its training months and the replay month."""
    months = train_months(first_month) + [REPLAY_MONTH]
    return "source_month IN (" + ", ".join(f"'{m}'" for m in months) + ")"


def Run(client, run_type, **fields):
    return run_store.Run(client, DB, run_type, prefix=PREFIX, **fields)


def busy_run(client):
    for db, br in [(DB, "BR7"), *OTHER_DBS.items()]:
        if not table_exists(client, db, "pipeline_runs"):
            continue
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
    if not row_count(client, DB, "raw_trip"):
        return None
    run = run_store.latest_run(client, DB, "import", status="succeeded")
    return run["import_id"] if run else None


# Same answer = same rows. Order-independent hash per result table, computed in
# ClickHouse for every engine (plan D7). The replay's passenger_count is left out:
# prepare.py's dropDuplicates keeps any one row of a duplicate group, and in the
# TLC files 7,608 such groups disagree on passenger_count, so that column can
# change between two runs of prepare.py itself (docs/result/br7/result-benchmark.md).
FINGERPRINT_SQL = """
SELECT
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(source_month, quality, rows))))
   FROM br7.run_quality WHERE run_id = {run_id:UUID}) AS quality,
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(source_month, ok_trips, trips))))
   FROM br7.run_month WHERE run_id = {run_id:UUID}) AS months,
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(
      zone_id, hour_of_week, mean_arrivals, p90_arrivals, training_hours))))
   FROM br7.run_baseline WHERE run_id = {run_id:UUID}) AS baseline,
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(
      event_id, dropoff_zone_id, pickup_zone_id, dropoff_time))))
   FROM br7.run_replay WHERE run_id = {run_id:UUID}) AS replay,
  (SELECT concat(toString(count()), ':', toString(groupBitXor(cityHash64(
      zone_id, zone_name, borough, landmark, replay_events))))
   FROM br7.run_zone WHERE run_id = {run_id:UUID}) AS zones
"""
FINGERPRINT_PARTS = ("quality", "months", "baseline", "replay", "zones")


def fingerprint(client, run_id):
    row = next(client.query(FINGERPRINT_SQL, parameters={"run_id": run_id}).named_results())
    return " · ".join(f"{part} {row[part]}" for part in FINGERPRINT_PARTS)


def result_summary(client, run_id):
    """Counts of the stored result, the figures of prepare.py's summary.json.
    Aliases differ from column names: ClickHouse would read sum(trips) AS trips
    back into the next expression."""
    row = next(client.query("""
        SELECT (SELECT sum(rows) FROM br7.run_quality WHERE run_id = {run_id:UUID}) AS n_read,
               (SELECT sumIf(rows, quality = 'ok') FROM br7.run_quality WHERE run_id = {run_id:UUID}) AS n_ok,
               sum(ok_trips) AS n_tourist, sum(trips) AS n_trips,
               (SELECT count() FROM br7.run_baseline WHERE run_id = {run_id:UUID}) AS n_cells,
               (SELECT count() FROM br7.run_replay WHERE run_id = {run_id:UUID}) AS n_replay,
               (SELECT countIf(replay_events > 0) FROM br7.run_zone WHERE run_id = {run_id:UUID}) AS n_zones
        FROM br7.run_month WHERE run_id = {run_id:UUID}
    """, parameters={"run_id": run_id}).named_results())
    n = {k: int(v) for k, v in row.items()}
    return {"rows_read": n["n_read"], "rows_ok": n["n_ok"], "tourist_ok_trips": n["n_tourist"],
            "trips": n["n_trips"], "duplicates": n["n_tourist"] - n["n_trips"], "baseline_cells": n["n_cells"],
            "replay_events": n["n_replay"], "zones": n["n_zones"]}


def write_small(client, run_id, quality, months, zone_rows):
    """The three small tables (a few dozen rows), the same for every engine and writer."""
    client.insert(f"{DB}.run_quality", [(run_id, m, q, n) for (m, q), n in sorted(quality.items())],
                  column_names=["run_id", "source_month", "quality", "rows"])
    client.insert(f"{DB}.run_month", [(run_id, m, a, b) for m, (a, b) in sorted(months.items())],
                  column_names=["run_id", "source_month", "ok_trips", "trips"])
    client.insert(f"{DB}.run_zone", [(run_id, *row) for row in zone_rows],
                  column_names=["run_id", "zone_id", "zone_name", "borough", "landmark", "replay_events"])
