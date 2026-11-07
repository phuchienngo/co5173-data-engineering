"""Route Finder Advanced Settings: per-run processing settings and analysis
rules (spec-pipeline-config.md C1-C7, C12). Built like BR5's settings.py on
BR6's own catalog, current values and presets (br6.settings). Edits are saved
straight away and used by the next run or import, from the app and the CLI."""

import pandas as pd
import streamlit as st

import route_backend as backend
import route_reference as reference
import route_runs as runs

GUIDE = "docs/guide/br6/config-guide.md"
catalog = backend.CATALOG

st.title("Advanced Settings")
st.caption("Route Finder · for the project team. Each change applies to the next run or import; no restart is "
           "needed.")

values = backend.current_values()
errors, warnings = backend.validation(values)
field_errors = st.session_state.setdefault("br6_setting_errors", {})


def widget_key(key):
    # Session state is shared with BR5's settings page, so BR6's keys differ.
    return f"br6set:{key}"


def widget_value(setting, value):
    return value == "true" if setting.type == "bool" else value


def sync_widgets(new_values):
    """After a load or reset, show the new values in the inputs."""
    for s in catalog.settings:
        st.session_state[widget_key(s.key)] = widget_value(s, new_values[s.key])
    field_errors.clear()


def on_edit(key):
    s = catalog.by_key[key]
    raw = st.session_state[widget_key(key)]
    text = ("true" if raw else "false") if s.type == "bool" else str(raw).strip()
    try:
        catalog.parse(key, text)
    except ValueError as exc:
        field_errors[key] = f"Not saved: {exc}."
        return
    field_errors.pop(key, None)
    backend.save_current({**backend.current_values(), key: text})


def reset(keys):
    current = backend.current_values()
    current.update({k: catalog.by_key[k].default for k in keys})
    backend.save_current(current)
    for k in keys:
        st.session_state[widget_key(k)] = widget_value(catalog.by_key[k], current[k])
        field_errors.pop(k, None)


# --- Presets (C5) and reset all (C3) ---------------------------------------
with st.container(border=True):
    names = backend.preset_names()
    left, mid, right = st.columns([2, 2, 1], vertical_alignment="bottom")
    chosen = left.selectbox("Preset", names, key="br6_preset", help="default and baseline are built in and read-only.")
    if mid.button("Load preset", use_container_width=True, key="br6_load_preset"):
        loaded = backend.preset_values(chosen)
        backend.save_current(loaded)
        sync_widgets(loaded)
        st.toast(f"Loaded preset {chosen}.")
        st.rerun()
    if chosen not in catalog.presets and right.button("Delete", use_container_width=True, key="br6_delete_preset"):
        backend.delete_preset(chosen)
        st.toast(f"Deleted preset {chosen}.")
        st.rerun()

    left, mid, right = st.columns([2, 2, 1], vertical_alignment="bottom")
    name = left.text_input("Save the current settings as", placeholder="for example tuned", key="br6_preset_name")
    if mid.button("Save preset", use_container_width=True, disabled=not name.strip(), key="br6_save_preset"):
        name = name.strip()
        if name in catalog.presets or name == runs.CURRENT:
            st.error(f"'{name}' is reserved; choose another name.")
        else:
            backend.save_preset(name, values)
            st.toast(f"Saved preset {name}.")
            st.rerun()
    if right.button("Reset all", use_container_width=True, help="Set every setting back to its default.",
                    key="br6_reset_all"):
        reset(list(catalog.by_key))
        st.rerun()

changed = catalog.changed(values)
if errors:
    st.error("Run and Import are blocked until these are fixed:\n\n" + "\n".join(
        f"- **{catalog.by_key[k].label}** {msg}" for k, msg in errors.items()))
st.caption(f"{len(changed)} setting(s) differ from the defaults." if changed else "All settings are at their defaults.")


def category_table():
    """The category -> place type grouping (read-only; edited in the file)."""
    grouping = reference.load_grouping()
    per_type = pd.Series(list(grouping.values())).value_counts().reindex(reference.PLACE_TYPES, fill_value=0)
    st.markdown("**Category → place type table** · read-only")
    st.caption(f"Edit `data/br6/category_place_type.csv` in the repository; the next run reloads it. "
               f"{len(grouping)} categories, file version `{reference.file_hash()[:12]}`.")
    st.dataframe(pd.DataFrame({"Place type": per_type.index, "Categories": per_type.values}), hide_index=True)


# --- One section per group (C1) --------------------------------------------
for group in catalog.groups:
    settings = [s for s in catalog.settings if s.group == group]
    if not settings:
        continue
    title = group + (" (for the team: what the suggestions say)" if group == "Analysis rules" else "")
    with st.expander(title, expanded=group in ("Resources", "Finding places near the route", "Place statistics")):
        for s in settings:
            key = s.key
            if widget_key(key) not in st.session_state:
                st.session_state[widget_key(key)] = widget_value(s, values[key])
            label_col, input_col, default_col = st.columns([3, 2, 1.2], vertical_alignment="center")
            is_changed = key in changed
            label_col.markdown(f"**{s.label}**" + ("  · *changed*" if is_changed else ""))
            label_col.caption(s.help + (f"  \n`{key}`" if s.kind in ("spark", "submit", "clickhouse") else ""))
            if s.type == "bool":
                input_col.toggle("value", key=widget_key(key), on_change=on_edit, args=(key,),
                                 label_visibility="collapsed")
            elif s.type == "choice":
                input_col.selectbox("value", s.choices, key=widget_key(key), on_change=on_edit, args=(key,),
                                    label_visibility="collapsed")
            else:
                placeholder = "unset (Spark decides)" if s.optional else None
                input_col.text_input("value", key=widget_key(key), on_change=on_edit, args=(key,),
                                     label_visibility="collapsed", placeholder=placeholder)
            default_text = s.default if s.default != "" else "unset"
            default_col.caption(f"Default: {default_text}")
            if is_changed:
                default_col.button("Reset", key=f"br6reset:{key}", on_click=reset, args=([key],))
            for message in (field_errors.get(key), errors.get(key)):
                if message:
                    input_col.error(message)
            if key in warnings:
                input_col.warning(warnings[key])
        if group == "Analysis rules":
            category_table()

# --- Needs restart (C7, read-only) -----------------------------------------
st.subheader("Needs restart (read-only)")
st.caption(f"These come from the running stack, shared with BR5. To change them, follow the restart guide: "
           f"`{GUIDE}`.")
limits = backend.server_limits()
spark = limits["cluster"]
ch = limits.get("clickhouse", {})
rows = {
    "Spark workers alive": spark["workers"] if spark else "unreachable",
    "Spark worker memory (SPARK_WORKER_MEMORY)": f"{spark['worker_memory_mb']} MB" if spark else "unreachable",
    "Spark worker cores (SPARK_WORKER_CORES)": spark["worker_cores"] if spark else "unreachable",
    "ClickHouse container memory": (f"{float(ch['CGroupMemoryTotal']) / 2**30:.2f} GB"
                                    if ch.get("CGroupMemoryTotal") else "n/a"),
    "ClickHouse CPU limit": f"{float(ch['CGroupMaxCPU']):g}" if ch.get("CGroupMaxCPU") else "n/a",
    "ClickHouse max_server_memory_usage_to_ram_ratio": ch.get("max_server_memory_usage_to_ram_ratio", "n/a"),
    "ClickHouse background_pool_size": ch.get("background_pool_size", "n/a"),
}
st.table({"Setting": list(rows), "Current value": [str(v) for v in rows.values()]})
if "clickhouse_error" in limits:
    st.caption(f"ClickHouse values unavailable: {limits['clickhouse_error']}")
