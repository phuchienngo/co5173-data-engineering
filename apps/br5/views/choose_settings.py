"""Advanced Settings menu item: choose Promotion Finder (BR5) or Route Finder (BR6)."""

import backend
import choose
import route_backend
import run_settings


def br5_status():
    changed = run_settings.changed(backend.current_values())
    return f"{len(changed)} settings changed from the defaults." if changed else "All settings at their defaults."


def br6_status():
    changed = route_backend.route_launch.CATALOG.changed(route_backend.current_values())
    return f"{len(changed)} settings changed from the defaults." if changed else "All settings at their defaults."


choose.page(
    "Advanced Settings",
    "Each BR has its own settings and presets. Choose which to view or change.",
    {choose.BR5: "views/settings.py", choose.BR6: "views/br6_settings.py"},
    {choose.BR5: br5_status, choose.BR6: br6_status},
)
