"""Data menu item: choose Promotion Finder (BR5) or Route Finder (BR6)."""

import backend
import choose
import route_backend


def br5_status():
    status = backend.data_status()
    if not status["ready"] or not status["loaded"]:
        return "No data yet."
    return f"{status['counts']['fact_booking']:,} bookings stored."


def br6_status():
    status = route_backend.data_status()
    if not status["ready"] or not status["loaded"]:
        return "No data yet."
    return f"{status['counts']['fact_checkin']:,} check-ins stored, {len(status['cities'])} cities."


choose.page(
    "Data",
    "Each BR has its own data. Choose which to view or import.",
    {choose.BR5: "views/data.py", choose.BR6: "views/br6_data.py"},
    {choose.BR5: br5_status, choose.BR6: br6_status},
)
