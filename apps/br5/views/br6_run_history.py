"""Route Finder Run History. Placeholder until the monitoring plan
(spec-monitoring.md): lists the BR6 imports and Route Finder runs with their
step times."""

import pandas as pd
import streamlit as st

import route_backend as backend

st.title("Run History")
st.caption("Route Finder · every import and run, started from the app or the CLI. Times are UTC. "
           "Run comparison comes with the monitoring plan.")

history = backend.run_history()
if not history:
    st.info("No imports or runs yet. Import data on the Route Finder Data page.")
    st.page_link("views/br6_data.py", label="Go to Data")
    st.stop()

asked = st.query_params.get("run")
rows = []
for r in history:
    criteria = r["criteria"]
    is_run = r["run_type"] == "run"
    rows.append({
        "Started": r["started_at"].strftime("%Y-%m-%d %H:%M:%S"),
        "Type": r["run_type"],
        "Status": r["status"],
        "Duration (s)": round((r["duration_ms"] or 0) / 1000, 1),
        "Choices": (f"{criteria.get('city', '')} {criteria.get('start', '')} → {criteria.get('end', '')}"
                    + (f", visitor {criteria['visitor']}" if criteria.get("visitor") else "")) if is_run
        else criteria.get("cities", ""),
        "Engine / writer": r["settings"].get("pipeline.engine", "") if is_run else r["writer"],
        "Preset": r["settings"].get("launch.settings_from", ""),
        "Multiplier": r["multiplier"],
        "Check-ins read": r["rows_read"],
        "Suggested / rows written": r["result_count"] if is_run else r["rows_written"],
        "Run id": str(r["run_id"]),
    })
st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

ids = [str(r["run_id"]) for r in history]
labels = {str(r["run_id"]): f"{r['started_at']:%Y-%m-%d %H:%M} · {r['run_type']} · {r['status']}" for r in history}
chosen = st.selectbox("Import or run", ids, index=ids.index(asked) if asked in ids else 0, format_func=labels.get)
run = next(r for r in history if str(r["run_id"]) == chosen)
if run["error"]:
    st.error(run["error"])
steps = run["step_durations"]
if steps:
    st.subheader("Time per step")
    st.bar_chart(pd.DataFrame({"Step": list(steps), "Seconds": [v / 1000 for v in steps.values()]}),
                 x="Step", y="Seconds", horizontal=True, sort=False)
buttons = st.columns(3)
if run["spark_app_id"]:
    buttons[0].link_button("Spark page", backend.spark_page_url(run["spark_app_id"]), width="stretch")
if run["run_type"] == "run" and run["status"] == "succeeded":
    buttons[1].page_link("views/route_finder.py", label="Open its result", query_params={"run": chosen})
