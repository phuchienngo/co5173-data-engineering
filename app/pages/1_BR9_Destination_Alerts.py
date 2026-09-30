"""BR9 destination alerts: status cards, trends, neighbourhoods, and history."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

import ch

st.set_page_config(page_title="BR9 · Destination alerts", page_icon="🚦", layout="wide")
st.title("🚦 BR9 · Destination pressure alerts")

if not ch.require(
    ["dim_destination", "fact_airport_flow_15min", "fact_occupancy_night"],
    "Run these jobs first:\n```\n./scripts/run_job.sh br9_01\n./scripts/run_job.sh br9_02\n"
    "./scripts/run_job.sh br9_03\n./scripts/run_job.sh br9_04\n```",
):
    st.stop()

COLOR = {"NORMAL": "#2ca02c", "WATCH": "#e0b400", "WARNING": "#ff7f0e",
         "CRITICAL": "#d62728", "EMERGENCY": "#111111"}

with st.sidebar:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()
    src = ch.q("""SELECT source, count() AS n, max(window_ts) AS newest
                  FROM tourism.fact_airport_flow_15min GROUP BY source""")
    for _, r in src.iterrows():
        st.caption(f"{'Streaming' if r['source'] == 'stream' else 'Batch'}: "
                   f"{r['n']:,} windows · latest {r['newest']} UTC")
    st.markdown("""
**How to read alert levels**

- *Flight arrival indicator*: arrivals / historical p95 for the same weekday and time slot
  (100 % WATCH · 120 % WARNING · 150 % CRITICAL · 200 % EMERGENCY; five-flight floor).
- *Accommodation indicator*: percentage of Airbnb guest capacity unavailable that night
  (70 % WATCH · 85 % WARNING · 95 % CRITICAL).
- **Combined** = the higher level, **plus one level when both are at least WARNING**.
""")

# ══════════════════════════════════════════════════════════════════════
#  1. Status cards
# ══════════════════════════════════════════════════════════════════════
live = ch.q("""
    SELECT city_id, city_name, window_local, arrivals, round(flow_capacity) AS cap, city_load_pct,
           flow_severity, occupancy_pct, occ_severity, occ_in_horizon, combined_level, hottest_airport, source
    FROM tourism.v_destination_live ORDER BY combined_severity DESC, city_load_pct DESC
""")
st.subheader("Latest status")
cols = st.columns(max(len(live), 1))
for c, (_, r) in zip(cols, live.iterrows()):
    with c:
        color = COLOR.get(r["combined_level"], "#999")
        occ = "—" if pd.isna(r["occupancy_pct"]) else f"{r['occupancy_pct']:.0f}%"
        st.markdown(
            f"<div style='border-left:6px solid {color};padding:6px 10px'>"
            f"<b>{r['city_name']}</b><br><span style='color:{color};font-weight:700'>{r['combined_level']}</span><br>"
            f"<small>{pd.Timestamp(r['window_local']):%d/%m %H:%M} local time</small><br>"
            f"✈️ {r['arrivals']} / {r['cap']:.0f} flights ({r['city_load_pct']:.0f}%)<br>"
            f"🏠 unavailable {occ}{'' if r['occ_in_horizon'] != 0 else ' ⚠️'}</div>",
            unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════
#  2. Destination trend
# ══════════════════════════════════════════════════════════════════════
st.subheader("Change over time")
cities = ch.q("SELECT city_id, city_name FROM tourism.dim_destination FINAL ORDER BY city_name")
left, mid, right = st.columns([2, 1, 1])
city_name = left.selectbox("Destination", cities["city_name"])
city_id = cities.loc[cities["city_name"] == city_name, "city_id"].iloc[0]

bounds = ch.q("""SELECT toDate(min(window_local)) AS a, toDate(max(window_local)) AS b
                 FROM tourism.v_city_flow_15min WHERE city_id = {c:String}""", {"c": city_id})
last_day = pd.Timestamp(bounds["b"].iloc[0]).date()
day_to = mid.date_input("End date", last_day)
days = right.slider("Number of days", 1, 14, 3)

ts = ch.q("""
    SELECT window_local, arrivals, flow_capacity, city_load_pct, occupancy_pct, combined_level, source
    FROM tourism.v_destination_15min
    WHERE city_id = {c:String}
      AND window_local >= toDateTime({d:Date}) - INTERVAL {n:UInt8} DAY + INTERVAL 1 DAY
      AND window_local <  toDateTime({d:Date}) + INTERVAL 1 DAY
    ORDER BY window_local
""", {"c": city_id, "d": day_to, "n": days})

if ts.empty:
    st.info("No windows in this date range.")
else:
    fig = go.Figure()
    fig.add_bar(x=ts["window_local"], y=ts["arrivals"], name="Arrivals",
                marker_color=[COLOR[l] for l in ts["combined_level"]])
    fig.add_scatter(x=ts["window_local"], y=ts["flow_capacity"], name="Historical high level (p95)",
                    line=dict(color="#555", dash="dash"))
    fig.add_scatter(x=ts["window_local"], y=ts["occupancy_pct"], name="% unavailable",
                    yaxis="y2", line=dict(color="#1f77b4"))
    fig.update_layout(height=380, margin=dict(t=20, b=10), legend=dict(orientation="h"),
                      yaxis=dict(title="flights / 15 minutes"),
                      yaxis2=dict(title="% unavailable", overlaying="y", side="right", range=[0, 100]))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Bar color shows the combined alert level. Times use the airport's local time.")

occ = ch.q("""
    SELECT night_date, occupancy_pct, snapshot_date, in_horizon
    FROM tourism.v_occupancy_city_night
    WHERE city_id = {c:String} AND night_date BETWEEN {a:Date} AND {a:Date} + 89
    ORDER BY night_date
""", {"c": city_id, "a": day_to})
if not occ.empty:
    st.markdown("**Unavailable capacity over the next 90 nights** (accommodation indicator)")
    f2 = go.Figure(go.Bar(x=occ["night_date"], y=occ["occupancy_pct"],
                          marker_color=["#1f77b4" if h else "#bbb" for h in occ["in_horizon"]]))
    for y, name in [(70, "WATCH"), (85, "WARNING"), (95, "CRITICAL")]:
        f2.add_hline(y=y, line_dash="dot", line_color=COLOR[name], annotation_text=name)
    f2.update_layout(height=260, margin=dict(t=10, b=10), yaxis=dict(range=[0, 100], title="% unavailable"))
    st.plotly_chart(f2, use_container_width=True)
    st.caption("Gray bars are outside the 90-night snapshot horizon and have lower confidence.")

# ══════════════════════════════════════════════════════════════════════
#  3. Neighbourhood map and alert history
# ══════════════════════════════════════════════════════════════════════
m1, m2 = st.columns([3, 2])
with m1:
    st.markdown("**Map: unavailable share over the next 30 nights by neighbourhood** (latest snapshot)")
    nb = ch.q("""
        SELECT neighbourhood, latitude, longitude, guest_capacity, occupancy_pct_next30
        FROM tourism.v_neighbourhood_next30 WHERE city_id = {c:String} AND guest_capacity >= 20
    """, {"c": city_id})
    if nb.empty:
        st.info("No neighbourhood data available.")
    else:
        def rgb(p):
            return [214, 39, 40] if p >= 85 else [255, 127, 14] if p >= 70 else [44, 160, 44]
        nb["color"] = nb["occupancy_pct_next30"].apply(rgb)
        nb["radius"] = (nb["guest_capacity"] ** 0.5) * 25
        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(latitude=nb["latitude"].median(),
                                             longitude=nb["longitude"].median(), zoom=9),
            layers=[pdk.Layer("ScatterplotLayer", nb, get_position=["longitude", "latitude"],
                              get_fill_color="color", get_radius="radius", opacity=0.6, pickable=True)],
            tooltip={"text": "{neighbourhood}\nunavailable {occupancy_pct_next30}% · capacity {guest_capacity} guests"},
        ))
with m2:
    st.markdown("**Alert history** (transitions to CRITICAL or above)")
    alerts = ch.q("""
        SELECT window_local, combined_level, hottest_airport, arrivals, round(flow_capacity) AS cap,
               city_load_pct, occupancy_pct, source
        FROM tourism.v_destination_alerts WHERE city_id = {c:String}
        ORDER BY window_ts DESC LIMIT 50
    """, {"c": city_id})
    alerts.columns = ["Local time", "Level", "Airport", "Flights", "p95", "% load", "% unavailable", "Source"]
    st.dataframe(alerts, hide_index=True, use_container_width=True, height=380)

st.caption("Data: U.S. BTS · Inside Airbnb (CC BY 4.0) · OpenFlights")
