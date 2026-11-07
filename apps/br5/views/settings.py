"""Advanced Settings: per-run processing and analysis settings
(spec-pipeline-config.md C1-C7, C12). Edits are saved straight away and used
by the next run or import, from the app and from the CLI."""

import streamlit as st

import backend
import run_settings as rs

GUIDE = "docs/guide/br5/config-guide.md"

st.title("Advanced Settings")
st.caption("For the project team. Each change applies to the next run or import; no restart is needed.")

values = backend.current_values()
errors, warnings = backend.validation(values)
field_errors = st.session_state.setdefault("setting_errors", {})


def widget_key(key):
    return f"set:{key}"


def widget_value(setting, value):
    return value == "true" if setting.type == "bool" else value


def sync_widgets(new_values):
    """After a load or reset, show the new values in the inputs."""
    for s in rs.SETTINGS:
        st.session_state[widget_key(s.key)] = widget_value(s, new_values[s.key])
    field_errors.clear()


def on_edit(key):
    s = rs.BY_KEY[key]
    raw = st.session_state[widget_key(key)]
    text = ("true" if raw else "false") if s.type == "bool" else str(raw).strip()
    try:
        rs.parse(key, text)
    except ValueError as exc:
        field_errors[key] = f"Not saved: {exc}."
        return
    field_errors.pop(key, None)
    backend.save_current({**backend.current_values(), key: text})


def reset(keys):
    current = backend.current_values()
    current.update({k: rs.BY_KEY[k].default for k in keys})
    backend.save_current(current)
    for k in keys:
        st.session_state[widget_key(k)] = widget_value(rs.BY_KEY[k], current[k])
        field_errors.pop(k, None)


# --- Presets (C5) and reset all (C3) ---------------------------------------
with st.container(border=True):
    names = backend.preset_names()
    left, mid, right = st.columns([2, 2, 1], vertical_alignment="bottom")
    chosen = left.selectbox("Preset", names, help="default and baseline are built in and read-only.")
    if mid.button("Load preset", use_container_width=True):
        loaded = backend.preset_values(chosen)
        backend.save_current(loaded)
        sync_widgets(loaded)
        st.toast(f"Loaded preset {chosen}.")
        st.rerun()
    if chosen not in rs.BUILTIN_PRESETS and right.button("Delete", use_container_width=True):
        backend.delete_preset(chosen)
        st.toast(f"Deleted preset {chosen}.")
        st.rerun()

    left, mid, right = st.columns([2, 2, 1], vertical_alignment="bottom")
    name = left.text_input("Save the current settings as", placeholder="for example tuned")
    if mid.button("Save preset", use_container_width=True, disabled=not name.strip()):
        name = name.strip()
        if name in rs.BUILTIN_PRESETS or name == rs.CURRENT:
            st.error(f"'{name}' is reserved; choose another name.")
        else:
            backend.save_preset(name, values)
            st.toast(f"Saved preset {name}.")
            st.rerun()
    if right.button("Reset all", use_container_width=True, help="Set every setting back to its default."):
        reset(list(rs.BY_KEY))
        st.rerun()

changed = rs.changed(values)
if errors:
    st.error("Run and Import are blocked until these are fixed:\n\n" + "\n".join(
        f"- **{rs.BY_KEY[k].label}** {msg}" for k, msg in errors.items()))
st.caption(f"{len(changed)} setting(s) differ from the defaults." if changed else "All settings are at their defaults.")

# --- One section per group (C1) --------------------------------------------
for group in rs.GROUPS:
    settings = [s for s in rs.SETTINGS if s.group == group]
    title = group + (" (for the team: what the recommendation says)" if group == "Analysis rules" else "")
    with st.expander(title, expanded=group in ("Resources", "Reading and writing")):
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
                default_col.button("Reset", key=f"reset:{key}", on_click=reset, args=([key],))
            for message in (field_errors.get(key), errors.get(key)):
                if message:
                    input_col.error(message)
            if key in warnings:
                input_col.warning(warnings[key])

# --- Needs restart (C7, read-only) -----------------------------------------
st.subheader("Needs restart (read-only)")
st.caption(f"These come from the running stack. To change them, follow the restart guide: `{GUIDE}`.")
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
