"""BR10 returning-guest dashboard based on Inside Airbnb reviews."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

import ch

st.set_page_config(page_title="BR10 · Returning guests", page_icon="🔁", layout="wide")
st.title("🔁 BR10 · Returning guests")

if not ch.require(
    ["dim_guest", "mart_br10_city_overview", "mart_br10_cohort"],
    "Run these jobs first:\n```\n./scripts/run_job.sh br10_01\n./scripts/run_job.sh br10_02\n./scripts/run_job.sh br10_03\n```",
):
    st.stop()

st.info("Rates cover **guests who wrote reviews** in six destinations. "
        "Guests without reviews are absent, and reviews from removed listings may be missing "
        "(so cohorts start in 2022).", icon="ℹ️")

ov = ch.q("SELECT * FROM tourism.mart_br10_city_overview ORDER BY guests DESC")
all_row = ov[ov["city_id"] == "ALL"].iloc[0]
k = st.columns(4)
k[0].metric("Guests (six destinations)", f"{all_row['guests']:,}")
k[1].metric("At least two trips (any destination)", f"{all_row['repeat_rate_pct']:.1f} %")
k[2].metric("Visited at least two destinations", f"{all_row['multi_city_pct']:.1f} %")
k[3].metric("Median days until second trip", f"{all_row['median_days_to_return']:.0f}")

st.subheader("By destination")
city = ov[ov["city_id"] != "ALL"].copy()
c1, c2 = st.columns([3, 2])
with c1:
    fig = px.bar(city.sort_values("repeat_rate_pct"), x="repeat_rate_pct", y="city_name", orientation="h",
                 text="repeat_rate_pct", labels={"repeat_rate_pct": "% guests returning to the same destination", "city_name": ""})
    fig.update_traces(texttemplate="%{text:.1f}%")
    fig.update_layout(height=320, margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)
with c2:
    show = city[["city_name", "guests", "repeat_guests", "repeat_rate_pct", "multi_city_pct", "median_days_to_return"]]
    show.columns = ["Destination", "Guests", "Returning", "% returning", "% visiting other destinations", "Median days"]
    st.dataframe(show, hide_index=True, use_container_width=True)

st.subheader("Cohort matrix (first-trip month × months since)")
metric = st.radio("Metric", ["Cumulative return (%)", "Active in that month (%)"], horizontal=True)
col = "cumulative_return_pct" if metric.startswith("Cumulative") else "retention_pct"
coh = ch.q(f"""
    SELECT formatDateTime(cohort_month, '%Y-%m') AS cohort, month_offset, cohort_size, {col} AS v
    FROM tourism.mart_br10_cohort WHERE month_offset BETWEEN 1 AND 24 ORDER BY cohort_month, month_offset
""")
if coh.empty:
    st.info("No sufficiently large cohorts.")
else:
    pv = coh.pivot(index="cohort", columns="month_offset", values="v")
    fig = px.imshow(pv, aspect="auto", color_continuous_scale="Blues", text_auto=".1f",
                    labels=dict(x="Months after first trip", y="Cohort", color="%"))
    fig.update_layout(height=520, margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

a, b = st.columns(2)
with a:
    st.subheader("Where do guests go next?")
    flow = ch.q("SELECT from_city, to_city, guests FROM tourism.mart_br10_city_flow")
    if not flow.empty:
        pv = flow.pivot(index="from_city", columns="to_city", values="guests").fillna(0)
        fig = px.imshow(pv, text_auto=True, color_continuous_scale="Oranges",
                        labels=dict(x="Next destination", y="Previous destination", color="Guests"))
        fig.update_layout(height=420, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Diagonal cells represent returns to the same destination.")
with b:
    st.subheader("RFM segments")
    seg = ch.q("SELECT * FROM tourism.mart_br10_rfm_segment ORDER BY guests DESC")
    fig = px.treemap(seg, path=["rfm_segment"], values="guests",
                     hover_data=["avg_recency_days", "avg_trips", "avg_cities"])
    fig.update_layout(height=420, margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("R = days since last trip · F = trip count · M = distinct destinations (no spending data).")

st.subheader("Annual share of returning guests")
yr = ch.q("SELECT year, city_id, guests, returning_pct FROM tourism.mart_br10_yearly WHERE year >= 2015 ORDER BY year")
fig = px.line(yr, x="year", y="returning_pct", color="city_id", markers=True,
              labels={"returning_pct": "% guests with a stay in a prior year", "year": ""})
fig.update_layout(height=340, margin=dict(t=10, b=10))
st.plotly_chart(fig, use_container_width=True)

st.subheader("Export guest list for CRM")
tier = st.selectbox("Segment", seg["rfm_segment"].tolist())
lst = ch.q("""
    SELECT reviewer_id, n_trips, n_cities, arrayStringConcat(cities, ',') AS cities,
           last_trip_date, recency_days, guest_tier
    FROM tourism.dim_guest FINAL WHERE rfm_segment = {s:String}
    ORDER BY n_trips DESC, last_trip_date DESC LIMIT 5000
""", {"s": tier})
st.dataframe(lst.head(200), hide_index=True, use_container_width=True)
st.download_button("⬇️ Download CSV (up to 5,000 guests)", lst.to_csv(index=False).encode("utf-8"),
                   file_name=f"guests_{tier}.csv", mime="text/csv")
st.caption("Data: Inside Airbnb (CC BY 4.0). reviewer_id is an Airbnb account identifier; guest names are not stored.")
