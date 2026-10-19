"""Run History (spec-monitoring.md section 1): every import and analysis run,
and two runs side by side (docs/plan/br5/plan-monitoring.md section 6)."""

import altair as alt
import pandas as pd
import streamlit as st

import backend
import run_settings as rs

MB = 1024 * 1024
# (label, resources key or keys, divisor, decimals): exact figures stored with each run.
RUN_FIGURES = [
    ("Spark CPU time (s)", "spark.cpu_ms", 1000, 1),
    ("Spark task time (s)", "spark.run_ms", 1000, 1),
    ("Spark GC time (s)", "spark.gc_ms", 1000, 2),
    ("Spark tasks", "spark.tasks", 1, 0),
    ("Shuffle written (MB)", "spark.shuffle_write_bytes", MB, 1),
    ("Spilled (MB)", ("spark.spill_memory_bytes", "spark.spill_disk_bytes"), MB, 1),
    ("Executor peak heap (MB)", "spark.executor_peak_heap_bytes", MB, 0),
    ("Driver peak heap (MB)", "spark.driver_peak_heap_bytes", MB, 0),
    ("ClickHouse queries", "clickhouse.queries", 1, 0),
    ("ClickHouse rows read", "clickhouse.read_rows", 1, 0),
    ("ClickHouse rows written", "clickhouse.written_rows", 1, 0),
    ("ClickHouse CPU time (s)", "clickhouse.cpu_ms", 1000, 2),
    ("ClickHouse peak query memory (MB)", "clickhouse.peak_memory_bytes", MB, 0),
]

st.title("Run History")
st.caption("Every import and analysis run, started from the app or the CLI. Times are UTC. "
           "Grafana shows trends over many runs; here you compare two runs.")

history = backend.run_history()
if not history:
    st.info("No runs yet. Import data on the Data page, then run the Promotion Finder.")
    st.stop()
by_id = {str(r["run_id"]): r for r in history}


def seconds(value):
    return "–" if value is None else f"{value:,.1f} s"


def label(r):
    parts = [f"{r['started_at']:%d %b %H:%M:%S}", r["run_type"], r["variant"]]
    if r["run_type"] == "analysis":
        parts.append(r["choice"])
    parts += [seconds(r["duration_s"]), r["status"]]
    return " · ".join(parts)


# --- Table ------------------------------------------------------------------

c1, c2, c3 = st.columns(3)
types = c1.multiselect("Type", ["analysis", "import"], default=["analysis", "import"])
statuses = c2.multiselect("Status", ["succeeded", "failed", "running"], default=["succeeded", "failed", "running"])
variants = sorted({r["variant"] for r in history})
chosen_variants = c3.multiselect("Settings", variants, placeholder="All")
shown = [r for r in history if r["run_type"] in types and r["status"] in statuses
         and (not chosen_variants or r["variant"] in chosen_variants)]

st.dataframe(pd.DataFrame([{
    "Started (UTC)": r["started_at"].replace(tzinfo=None),
    "Type": r["run_type"],
    "Status": r["status"],
    "Choice": r["choice"] if r["run_type"] == "analysis" else "",
    "Settings": r["settings_from"],
    "Engine": r["engine"],
    "×": r["multiplier"],
    "Seconds": None if r["duration_s"] is None else round(r["duration_s"], 1),
    "Read": r["rows_read"],
    "Used": r["rows_used"] if r["run_type"] == "analysis" else None,
    "Written": r["rows_written"] if r["run_type"] == "import" else None,
    "Cards": r["result_count"] if r["run_type"] == "analysis" else None,
    "Run": str(r["run_id"])[:8],
    "Spark page": backend.spark_page_url(r["spark_app_id"]),
    "Error": r["error"][:200],
} for r in shown]), hide_index=True, height=320, column_config={
    "Started (UTC)": st.column_config.DatetimeColumn(format="DD MMM HH:mm:ss"),
    "Spark page": st.column_config.LinkColumn(display_text="open"),
    "Read": st.column_config.NumberColumn(format="localized"),
    "Used": st.column_config.NumberColumn(format="localized"),
    "Written": st.column_config.NumberColumn(format="localized"),
})
st.caption(f"{len(shown)} of {len(history)} runs. The Spark page (history server) has each Spark run's "
           "jobs and stages; their names are the steps.")

# --- Compare two runs -------------------------------------------------------

st.divider()
st.subheader("Compare two runs")
ids = [str(r["run_id"]) for r in shown] or list(by_id)
asked = st.query_params.get("run")
asked = next((i for i in ids if i.startswith(asked)), None) if asked else None
default_b = asked or ids[0]
same_type = [i for i in ids if i != default_b and by_id[i]["run_type"] == by_id[default_b]["run_type"]]
default_a = same_type[0] if same_type else next((i for i in ids if i != default_b), default_b)

left, right = st.columns(2)
run_a = by_id[left.selectbox("Run A", ids, index=ids.index(default_a), format_func=lambda i: label(by_id[i]))]
run_b = by_id[right.selectbox("Run B", ids, index=ids.index(default_b), format_func=lambda i: label(by_id[i]))]
id_a, id_b = str(run_a["run_id"]), str(run_b["run_id"])

if id_a == id_b:
    st.info("Pick two different runs.")
    st.stop()
if run_a["run_type"] != run_b["run_type"]:
    st.warning("An import and an analysis run do different work: only their settings are comparable.")
notes = []
if run_a["import_id"] != run_b["import_id"]:
    notes.append("they read different imports" + (
        f" (×{run_a['multiplier']} and ×{run_b['multiplier']})" if run_a["multiplier"] != run_b["multiplier"] else ""))
if run_a["run_type"] == run_b["run_type"] == "analysis" and run_a["choice"] != run_b["choice"]:
    notes.append("the choices differ")
if "succeeded" not in {run_a["status"]} & {run_b["status"]}:
    notes.append("one of them didn't succeed")
if notes and run_a["run_type"] == run_b["run_type"]:
    st.warning("Not a fair speed comparison: " + "; ".join(notes) + ".")


def ratio(a, b):
    if not a or b is None:
        return ""
    return f"B is {b / a:.2f}× A"


summary = [
    ("Started (UTC)", f"{run_a['started_at']:%d %b %Y %H:%M:%S}", f"{run_b['started_at']:%d %b %Y %H:%M:%S}", ""),
    ("Status", run_a["status"], run_b["status"], ""),
    ("Choice", run_a["choice"], run_b["choice"], ""),
    ("Settings from", run_a["settings_from"], run_b["settings_from"], ""),
    ("Engine", run_a["engine"], run_b["engine"], ""),
    ("Data", f"import {str(run_a['import_id'])[:8]} ×{run_a['multiplier']}",
     f"import {str(run_b['import_id'])[:8]} ×{run_b['multiplier']}", ""),
    ("Run time", seconds(run_a["duration_s"]), seconds(run_b["duration_s"]), ratio(run_a["duration_s"], run_b["duration_s"])),
    ("Outside the steps (Spark start-up)", seconds(run_a["outside_steps_s"]), seconds(run_b["outside_steps_s"]), ""),
    ("Rows read", f"{run_a['rows_read']:,}", f"{run_b['rows_read']:,}", ""),
]
if run_a["run_type"] == "analysis":
    summary += [("Rows used", f"{run_a['rows_used']:,}", f"{run_b['rows_used']:,}", ""),
                ("Cards", str(run_a["result_count"]), str(run_b["result_count"]), "")]
else:
    summary.append(("Rows written", f"{run_a['rows_written']:,}", f"{run_b['rows_written']:,}", ""))
st.dataframe(pd.DataFrame(summary, columns=["", "A", "B", "B vs A"]), hide_index=True)

links = st.columns(4)
for col, run, name in ((links[0], run_a, "A"), (links[1], run_b, "B")):
    url = backend.spark_page_url(run["spark_app_id"])
    if url:
        col.link_button(f"Spark page of {name}", url, width="stretch")
    col.link_button(f"Resource use during {name}", backend.dashboard_url("Resource use", run), width="stretch")

# Step durations
steps = []
for name, run in (("A", run_a), ("B", run_b)):
    for step, ms in run["step_durations"].items():
        steps.append({"run": name, "step": step, "seconds": ms / 1000})
    if run["outside_steps_s"] is not None:
        steps.append({"run": name, "step": "start-up and other", "seconds": run["outside_steps_s"]})
if steps:
    order = list(dict.fromkeys(s["step"] for s in steps))
    st.markdown("**Seconds per step**")
    st.altair_chart(alt.Chart(pd.DataFrame(steps)).mark_bar().encode(
        y=alt.Y("step:N", sort=order, title=None),
        yOffset=alt.YOffset("run:N"),
        x=alt.X("seconds:Q", title="seconds"),
        color=alt.Color("run:N", title="Run", scale=alt.Scale(range=["#4c78a8", "#f58518"])),
        tooltip=["run", "step", alt.Tooltip("seconds:Q", format=".1f")],
    ).properties(height=40 * len(order)), use_container_width=True)

# Settings that differ
st.markdown("**Settings that differ**")
diff = backend.settings_diff(run_a["settings"], run_b["settings"])


def setting_label(key):
    if key == "launch.settings_from":
        return "Settings from"
    for prefix, text in (("effective.", "Spark reports: "), ("cluster.", "Cluster: ")):
        if key.startswith(prefix):
            base = key[len(prefix):]
            return text + (rs.BY_KEY[base].label if base in rs.BY_KEY else base)
    return rs.BY_KEY[key].label if key in rs.BY_KEY else key


if diff:
    st.dataframe(pd.DataFrame([{"Setting": setting_label(k), "Key": k, "A": "–" if a is None else a,
                                "B": "–" if b is None else b} for k, a, b in diff]), hide_index=True)
else:
    st.caption("Both runs used the same settings, on the same cluster.")

# Resource figures
st.markdown("**Resources**")


def figure(resources, keys, divisor, decimals):
    keys = keys if isinstance(keys, tuple) else (keys,)
    if not any(k in resources for k in keys):
        return "–"
    return f"{sum(resources.get(k, 0) for k in keys) / divisor:,.{decimals}f}"


rows = [(name, figure(run_a["resources"], keys, d, n), figure(run_b["resources"], keys, d, n))
        for name, keys, d, n in RUN_FIGURES]
rows = [r for r in rows if r[1] != "–" or r[2] != "–"]
containers_a = backend.container_figures(id_a, run_a["started_at"], run_a["finished_at"])
containers_b = backend.container_figures(id_b, run_b["started_at"], run_b["finished_at"])
for container in backend.CONTAINERS:
    for key, name, divisor, decimals in (("cpu_s", "CPU (s)", 1, 1), ("peak_memory_bytes", "peak memory (MB)", MB, 0)):
        a = containers_a.get(container, {}).get(key)
        b = containers_b.get(container, {}).get(key)
        if a is not None or b is not None:
            rows.append((f"Container {container}: {name}",
                         "–" if a is None else f"{a / divisor:,.{decimals}f}",
                         "–" if b is None else f"{b / divisor:,.{decimals}f}"))
if rows:
    st.dataframe(pd.DataFrame(rows, columns=["", "A", "B"]), hide_index=True)
    st.caption("Spark and ClickHouse figures are exact and stored with each run. Spark CPU time counts the JVM "
               "task threads only, not the Python workers of the parallel reader. Container figures come from "
               "Prometheus (samples every 5 s), so a run of a few seconds may have none.")
else:
    st.caption("No resource figures for these runs (they ran before monitoring was added).")

# Same result?
st.markdown("**Same result?**")
if run_a["run_type"] != "analysis" or run_b["run_type"] != "analysis":
    st.caption("Only analysis runs have a result to compare.")
elif run_a["status"] != "succeeded" or run_b["status"] != "succeeded":
    st.caption("Only succeeded runs have a result to compare.")
elif run_a["choice"] != run_b["choice"]:
    st.caption("The choices differ, so the results can't be the same.")
else:
    problems, cards, months = backend.same_result(id_a, id_b)
    if problems:
        st.error(f"Different: {len(problems)} differences. First ones:\n\n" + "\n".join(f"- {p}" for p in problems[:10]))
    else:
        st.success(f"Yes: the {cards} cards and {months} month rows match (figures rounded to 2 decimals).")
