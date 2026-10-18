"""Prints the BR5 data status (what the app's Data screen shows).

Used by `make br5-status` (scripts/br5.py runs it inside spark-local).
"""

import runs
from clickhouse_io import get_client


def describe_run(run):
    seconds = (run["duration_ms"] or 0) / 1000
    when = run["started_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"{run['status']} at {when}, {seconds:.1f}s, step {run['step']}, ×{run['multiplier']}, writer {run['writer']}"


def main():
    status = runs.data_status(get_client())
    if not status["ready"]:
        print(status["reason"])
        return
    if status["loaded"]:
        imported = status["import"]
        print(f"Data loaded: {status['counts']['fact_booking']:,} bookings")
        if imported:
            label = " (synthetic)" if imported["multiplier"] > 1 else ""
            print(f"  multiplier: ×{imported['multiplier']}{label}")
            print(f"  imported:   {describe_run(imported)}")
            print(f"  source:     {imported['source_file']} ({imported['source_bytes'] / 1e6:.1f} MB)")
    else:
        print("No data yet: import it with `make br5-import` or from the app's Data screen.")
    print("  tables:     " + ", ".join(f"{t}={c:,}" for t, c in status["counts"].items() if c is not None))
    last = status["last_import_run"]
    if last and (not status["import"] or last["run_id"] != status["import"]["run_id"]):
        print(f"Last import run: {describe_run(last)}")
        if last["error"]:
            print(f"  error: {last['error']}")
    if status["busy"]:
        print(f"Busy: a {status['busy']['run_type']} run is in progress (step: {status['busy']['step']}).")


if __name__ == "__main__":
    main()
