"""Home page showing pipeline progress and the latest available data."""

from __future__ import annotations

import streamlit as st

import ch

st.set_page_config(page_title="Data Pipeline · Tourism Pressure", page_icon="✈️", layout="wide")
st.title("✈️ Tourism pressure in six US destinations")
st.caption("Data Pipeline · BTS On-Time Performance + Inside Airbnb · "
           "Kafka → Spark (Batch + Structured Streaming) → ClickHouse → Streamlit / Grafana")

ok, msg = ch.health()
if not ok:
    st.error(f"Cannot connect to ClickHouse at `{ch.HOST}:{ch.PORT}`\n\n```\n{msg[:400]}\n```")
    st.stop()

st.markdown("""
| Page | Business question |
|---|---|
| **BR9 · Destination alerts** | Which destination has unusually many arriving flights while Airbnb accommodation is close to full? |
| **BR10 · Returning guests** | What share of guests return to the same destination? Which cohorts retain guests? Where do guests go next? |
""")

st.subheader("Loaded data")
counts = ch.q("""
    SELECT 'fact_airport_flow_15min · batch' AS table_name, count() AS row_count, toString(max(window_ts)) AS latest_date
      FROM tourism.fact_airport_flow_15min WHERE source = 'batch'
    UNION ALL
    SELECT 'fact_airport_flow_15min · stream', count(), toString(max(window_ts))
      FROM tourism.fact_airport_flow_15min WHERE source = 'stream'
    UNION ALL
    SELECT 'raw_flight_events (Kafka Engine)', count(), toString(max(event_ts)) FROM tourism.raw_flight_events
    UNION ALL
    SELECT 'baseline_airport_slot', count(), toString(max(history_to)) FROM tourism.baseline_airport_slot
    UNION ALL
    SELECT 'fact_occupancy_night', count(), toString(max(snapshot_date)) FROM tourism.fact_occupancy_night
    UNION ALL
    SELECT 'fact_stay', count(), toString(max(review_date)) FROM tourism.fact_stay
    UNION ALL
    SELECT 'dim_guest', count(), toString(max(as_of_date)) FROM tourism.dim_guest
""")
counts.columns = ["Table", "Rows", "Latest date"]
st.dataframe(counts, hide_index=True, use_container_width=True)

st.subheader("Latest run of each job")
status = ch.q("""
    SELECT job_name, br, last_status, last_run_ts, last_rows, round(last_duration_sec, 1) AS sec
    FROM tourism.v_pipeline_status
""")
if status.empty:
    st.info("No jobs have run yet. Follow docs/02-runbook.md.")
else:
    status.columns = ["Job", "BR", "Status", "At", "Rows", "Seconds"]
    st.dataframe(status, hide_index=True, use_container_width=True)

st.caption("Sources: US Bureau of Transportation Statistics (public-domain data) · "
           "Inside Airbnb (insideairbnb.com, CC BY 4.0) · OpenFlights.org (ODbL)")
