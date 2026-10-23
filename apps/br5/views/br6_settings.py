"""Route Finder Advanced Settings. Placeholder until the config plan
(spec-pipeline-config.md): shows the values an import uses now."""

import streamlit as st

import route_backend as backend

catalog = backend.route_launch.CATALOG

st.title("Advanced Settings")
st.caption("Route Finder · the settings screen (edit, presets, checks) comes with the config plan. "
           "Until then every import uses these values.")

values = backend.current_values()
changed = set(catalog.changed(values))
for group in catalog.groups:
    settings = [s for s in catalog.settings if s.group == group]
    if not settings:
        continue
    st.subheader(group)
    st.table({
        "Setting": [s.label for s in settings],
        "Value": [(values[s.key] or "unset") + (" (changed)" if s.key in changed else "") for s in settings],
        "Default": [s.default or "unset" for s in settings],
        "What it does": [s.help for s in settings],
    })
