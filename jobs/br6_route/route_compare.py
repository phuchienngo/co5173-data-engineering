"""Checks that two BR6 Route Finder runs gave the same answer: the same places
with the same status, rank, detour, position and scores, and the same cleaning
counts. Used to show that the optimizations and the two engines only change the
speed (spec C9, C10; plan-pipeline.md section 12). The app's Run History uses
it too, with settings_diff (plan-monitoring.md H5).

    python3 route_compare.py RUN_ID_A RUN_ID_B
"""

import sys

import route_runs as runs
from clickhouse_io import get_client

COMPARED = ["place_key", "status", "rank", "leg", "visitors", "checkins", "visitor_visits", "detour_km",
            "position_km", "type_share", "popularity", "fit", "closeness", "score"]
DECIMALS = 6


def comparable(row):
    return tuple(round(row[c], DECIMALS) if isinstance(row[c], float) else row[c] for c in COMPARED)


def settings_diff(a, b):
    """[(key, value in a, value in b)] for the snapshot keys that differ
    (None = missing). Settings first, then Spark's effective values, the
    cluster, the category grouping and where the settings came from."""
    order = {"effective": 1, "cluster": 2, "grouping": 3, "launch": 4}
    keys = sorted(set(a) | set(b), key=lambda k: (order.get(k.split(".", 1)[0], 0), k))
    return [(k, a.get(k), b.get(k)) for k in keys if a.get(k) != b.get(k)]


def differences(client, run_a, run_b):
    problems = []
    a, b = runs.get_run(client, run_a), runs.get_run(client, run_b)
    for name, run in ((run_a, a), (run_b, b)):
        if run is None or run["run_type"] != "run" or run["status"] != "succeeded":
            problems.append(f"{name} is not a succeeded Route Finder run")
    if problems:
        return problems
    if dict(a["criteria"]) != dict(b["criteria"]):
        problems.append(f"different choices: {dict(a['criteria'])} vs {dict(b['criteria'])}")
    for field in ("rows_read", "rows_used", "rows_excluded", "result_count"):
        if a[field] != b[field]:
            problems.append(f"{field}: {a[field]} vs {b[field]}")
    places_a = [comparable(r) for r in runs.run_places(client, run_a)]
    places_b = [comparable(r) for r in runs.run_places(client, run_b)]
    if places_a != places_b:
        set_a, set_b = set(places_a), set(places_b)
        only_a, only_b = sorted(set_a - set_b)[:5], sorted(set_b - set_a)[:5]
        problems.append(f"places differ ({len(places_a)} vs {len(places_b)}); only in A: {only_a}; only in B: {only_b}")
    return problems


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    problems = differences(get_client(autogenerate_session_id=False), sys.argv[1], sys.argv[2])
    if problems:
        print("DIFFERENT:\n  " + "\n  ".join(problems))
        sys.exit(1)
    print("SAME: identical places, statuses, ranks, scores and counts")


if __name__ == "__main__":
    main()
