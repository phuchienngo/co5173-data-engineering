"""Prints the BR6 data status (what the app's Data screen shows).

Used by `make br6-status` (scripts/br6.py runs it inside br5-app).
"""

import json

import route_runs as runs
from clickhouse_io import get_client


def describe_run(run):
    seconds = (run["duration_ms"] or 0) / 1000
    when = run["started_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
    return (f"{run['status']} at {when}, {seconds:.1f}s, step {run['step']}, ×{run['multiplier']}, "
            f"writer {run['writer']}, cities {run['criteria'].get('cities', '?')}")


def main():
    status = runs.data_status(get_client())
    if not status["ready"]:
        print(status["reason"])
        return
    if status["loaded"]:
        imported = status["import"]
        print(f"Data loaded: {status['counts']['fact_checkin']:,} check-ins "
              f"({status['counts']['raw_checkin']:,} in the files)")
        if status["multiplier"] > 1:
            print(f"  multiplier: ×{status['multiplier']} (synthetic)")
        if imported:
            print(f"  imported:   {describe_run(imported)}")
            print(f"  source:     {imported['source_file']} ({imported['source_bytes'] / 1e6:.1f} MB)")
            ungrouped = json.loads(imported["result_summary"] or "{}").get("ungrouped_categories", [])
            if ungrouped:
                print(f"  categories not grouped (Not offered): {len(ungrouped)}")
        for c in status["cities"]:
            print(f"  {c['city_name']:15} {c['country_name']:15} {c['checkins']:>9,} check-ins "
                  f"({c['no_location']:,} without location), {c['places']:,} places, {c['visitors']:,} visitors")
    else:
        print("No data yet: import it with `make br6-import` or from the app's Data screen.")
    print("  tables:     " + ", ".join(f"{t}={c:,}" for t, c in status["counts"].items() if c is not None))
    last = status["last_import_run"]
    if last and (not status["import"] or last["run_id"] != status["import"]["run_id"]):
        print(f"Last import run: {describe_run(last)}")
        if last["error"]:
            print(f"  error: {last['error']}")
    if status["busy"]:
        print(runs.busy_message(status["busy"]))


if __name__ == "__main__":
    main()
