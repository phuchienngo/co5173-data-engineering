"""Route Finder Run History. Placeholder until the monitoring plan
(spec-monitoring.md): lists the BR6 imports with their step times."""

import pandas as pd
import streamlit as st

import route_backend as backend

st.title("Run History")
st.caption("Route Finder · every import, started from the app or the CLI. Times are UTC. "
           "Run comparison comes with the monitoring plan.")

history = backend.import_history()
if not history:
    st.info("No imports yet. Import data on the Route Finder Data page.")
    st.page_link("views/br6_data.py", label="Go to Data")
    st.stop()

asked = st.query_params.get("run")
rows = []
for r in history:
    rows.append({
        "Started": r["started_at"].strftime("%Y-%m-%d %H:%M:%S"),
        "Status": r["status"],
        "Duration (s)": round((r["duration_ms"] or 0) / 1000, 1),
        "Cities": r["criteria"].get("cities", ""),
        "Multiplier": r["multiplier"],
        "Writer": r["writer"],
        "Rows read": r["rows_read"],
        "Rows written": r["rows_written"],
        "Run id": str(r["run_id"]),
    })
st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

ids = [str(r["run_id"]) for r in history]
labels = {str(r["run_id"]): f"{r['started_at']:%Y-%m-%d %H:%M} · {r['status']}" for r in history}
chosen = st.selectbox("Import", ids, index=ids.index(asked) if asked in ids else 0, format_func=labels.get)
run = next(r for r in history if str(r["run_id"]) == chosen)
if run["error"]:
    st.error(run["error"])
steps = run["step_durations"]
if steps:
    st.subheader("Time per step")
    st.bar_chart(pd.DataFrame({"Step": list(steps), "Seconds": [v / 1000 for v in steps.values()]}),
                 x="Step", y="Seconds", horizontal=True, sort=False)
if run["spark_app_id"]:
    st.link_button("Spark page of this import", f"{backend.SPARK_HISTORY_URL}/history/{run['spark_app_id']}/jobs/")
