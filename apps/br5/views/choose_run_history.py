"""Run History menu item: choose Promotion Finder (BR5) or Route Finder (BR6)."""

import backend
import choose
import route_backend


def describe(run):
    if run is None:
        return "No runs yet."
    kind = "import" if run["run_type"] == "import" else "run"
    return f"Latest: {kind} {run['status']} at {run['started_at']:%Y-%m-%d %H:%M} UTC."


def br5_status():
    latest = backend.run_history(limit=1)
    return describe(latest[0] if latest else None)


def br6_status():
    latest = route_backend.run_history(limit=1)
    return describe(latest[0] if latest else None)


choose.page(
    "Run History",
    "Each BR has its own runs; two runs are compared within one BR. Choose which to view.",
    {choose.BR5: "views/run_history.py", choose.BR6: "views/br6_run_history.py"},
    {choose.BR5: br5_status, choose.BR6: br6_status},
)
