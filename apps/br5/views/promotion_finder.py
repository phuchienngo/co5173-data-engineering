"""Promotion Finder (spec.md section 3): choose hotel, guest country and room
type, run the analysis, read the low season and the recommended promotions."""

import calendar
import csv
import io
import json

import altair as alt
import pandas as pd
import streamlit as st

import backend
import launch
import run_settings as rs

ALL = "All"
MONTHS = list(calendar.month_abbr)
STEP_LABELS = {
    "starting": "Starting",
    "read": "Reading the bookings",
    "clean": "Applying the cleaning rules",
    "aggregate": "Totalling demand per month",
    "rank": "Finding low seasons and ranking",
    "profile": "Comparing low season with peak",
    "recommend": "Choosing the promotions",
    "write": "Saving the result",
}

st.title("Promotion Finder")

status = backend.data_status()
if not status["ready"]:
    st.error(status["reason"])
    st.stop()
if not status["loaded"]:
    st.info("No data yet. Import it on the Data page first.")
    st.page_link("views/data.py", label="Go to Data")
    st.stop()

values = backend.current_values()
rules = rs.options(values)["rules"]
hotels, countries, rooms = backend.choices(backend.stored_import_id(), int(rules["min_bookings"]),
                                           rules["duplicates"] == "remove")


def country_label(code):
    return ALL if code == ALL else f"{countries.get(code, code)} ({code})"


st.caption("Choose who you want to reach. **All** for country or room type ranks them for you.")
c1, c2, c3 = st.columns(3)
c1.selectbox("Hotel", [ALL, *hotels], key="hotel")
c2.selectbox("Guest country", [ALL, *countries], key="country", format_func=country_label,
             help=f"Countries with at least {int(rules['min_bookings'])} valid bookings.")
c3.selectbox("Room type", [ALL, *rooms], key="room_type",
             help=f"Room types booked, with at least {int(rules['min_bookings'])} valid bookings.")


@st.fragment(run_every=2)
def run_panel():
    status = backend.data_status()
    job = backend.launched_job()
    ours = job if job and job["running"] else None
    busy = status["busy"]
    if busy or ours:
        run = busy or (ours and ours["run"])
        if run and run["run_type"] == "import":
            st.info("Busy: data is being imported. Try again when it has finished (see the Data page).")
        else:
            st.info(f"Running: {STEP_LABELS.get(run['step'], run['step']) if run else 'Starting Spark… (15–30 s)'}")
            if run and run["run_type"] == "analysis" and run.get("spark_app_id"):
                st.link_button("Live Spark page", backend.SPARK_LIVE_URL,
                               help="Spark's own page of this run: jobs and stages per step, while it runs.")
        if ours:
            if st.button("Stop", help="Stop the job started from this app."):
                backend.stop_job()
                st.rerun(scope="fragment")
        return

    if job and job["kind"] == "analysis" and job["exit_code"] not in (0, None) and job["run"] is None:
        st.error(f"The run could not start (exit code {job['exit_code']}).")
        st.code(job["log_tail"])
    errors, _ = backend.validation(values)
    if errors:
        st.warning("Some settings are invalid; fix them on the Advanced Settings page to run.")
    if st.button("Find promotions", type="primary", disabled=bool(errors)):
        error = backend.launch_job("analysis", launch.run_args(
            st.session_state.hotel, st.session_state.country, st.session_state.room_type))
        if error:
            st.error(error)
        else:
            st.rerun(scope="fragment")

    # A run finished since the page was drawn: redraw the result.
    latest = backend.latest_attempt()
    latest_id = str(latest["run_id"]) if latest else None
    if st.session_state.setdefault("seen_run", latest_id) != latest_id and latest["status"] != "running":
        st.session_state.seen_run = latest_id
        st.rerun()


run_panel()

attempt = backend.latest_attempt()
result = backend.latest_result()
if attempt and result and attempt["run_id"] != result["run"]["run_id"]:
    if attempt["status"] == "failed":
        st.error(f"The last run failed, so the result below is from an earlier run.\n\n{attempt['error']}")
    elif attempt["status"] == "running" and not (status["busy"] and status["busy"]["run_id"] == attempt["run_id"]):
        st.warning("The last run stopped without finishing; the result below is from an earlier run.")
if result is None:
    st.stop()

run, months, cards, summary = result["run"], result["months"], result["cards"], result["summary"]
criteria = run["criteria"]
settings = run["settings"]
threshold = float(settings.get("rules.low_season_threshold", "60")) / 100

st.divider()
st.subheader("Result")
header = " · ".join([
    criteria.get("hotel", ALL) if criteria.get("hotel") != ALL else "All hotels",
    country_label(criteria.get("country", ALL)) if criteria.get("country") != ALL else "All countries",
    f"Room {criteria['room_type']}" if criteria.get("room_type") != ALL else "All room types",
])
st.markdown(f"**For: {header}**  \n"
            f"Run on {run['started_at']:%d %b %Y %H:%M} UTC in {(run['duration_ms'] or 0) / 1000:.1f} s")
if str(run["import_id"]) != str(backend.stored_import_id()):
    st.warning("This result was computed on data that has since been re-imported. Run again to update it.")
if summary.get("multiplier", 1) > 1:
    st.caption(f"Data imported ×{summary['multiplier']} (synthetic); figures are per original booking.")


def month_frame(rank):
    rows = [m for m in months if m["target_rank"] == rank]
    return pd.DataFrame({
        "Month": [MONTHS[m["month"]] for m in rows],
        "month_no": [m["month"] for m in rows],
        "Demand": [round(m["demand"], 1) for m in rows],
        "Share of peak": [m["share_of_peak"] for m in rows],
        "Season": ["Peak" if m["is_peak"] else "Low season" if m["is_low"] else "Other" for m in rows],
        "Average price": [round(m["avg_adr"], 2) if m["avg_adr"] is not None else None for m in rows],
        "Bookings": [round(m["bookings"], 1) for m in rows],
    })


def demand_chart(frame, height=260):
    peak = frame["Demand"].max() if len(frame) else 0
    bars = alt.Chart(frame).mark_bar().encode(
        x=alt.X("Month:N", sort=MONTHS[1:], title=None),
        y=alt.Y("Demand:Q", title="Room-nights per month"),
        color=alt.Color("Season:N", scale=alt.Scale(domain=["Low season", "Other", "Peak"],
                                                    range=["#e4572e", "#a0aec0", "#2e86ab"]),
                        legend=alt.Legend(orient="top", title=None)),
        tooltip=["Month", "Demand", alt.Tooltip("Share of peak:Q", format=".0%"), "Average price", "Bookings"],
    )
    line = alt.Chart(pd.DataFrame({"y": [peak * threshold]})).mark_rule(strokeDash=[4, 4]).encode(y="y:Q")
    return (bars + line).properties(height=height)


# R1: low season of the whole choice
selection = month_frame(0)
st.markdown("#### When is the low season?")
if summary.get("season_texts"):
    st.markdown("\n".join(f"- **{t}**" for t in summary["season_texts"]))
if len(selection) and selection["Demand"].max() > 0:
    st.altair_chart(demand_chart(selection), use_container_width=True)
    st.caption(f"Demand = room-nights of bookings that were not canceled, average per year. "
               f"Dashed line: {threshold:.0%} of the peak month; months below it are low season.")

# R2: the cards
st.markdown("#### Recommended promotions")
if summary.get("message"):
    st.warning(summary["message"])
for card in cards:
    with st.container(border=True):
        country = f"{card['country_name']} ({card['country_code']})"
        st.markdown(f"##### #{card['target_rank']} · {card['hotel']} · Room {card['room_type']} · {country}")
        when, why = st.columns(2)
        when.markdown(f"**When:** {card['when_text']}")
        why.markdown(
            f"**Why:** low-season demand is {card['low_demand']:.0f} room-nights per month vs "
            f"{card['peak_demand']:.0f} in {MONTHS[card['peak_month']]} (peak): "
            f"**{card['potential']:.0f} room-nights per month** could be filled."
        )
        st.markdown("**What:**")
        for offer, evidence in zip(card["offers"], card["evidence"]):
            st.markdown(f"- {offer}  \n  <small>{evidence}</small>", unsafe_allow_html=True)
        if card["channels"]:
            st.markdown(f"**Where:** {', '.join(card['channels'])} (share of low-season bookings)")
        if len(cards) > 1 or ALL in (criteria.get("country"), criteria.get("room_type")):
            # With one exact target, its chart is the one above.
            st.altair_chart(demand_chart(month_frame(card["target_rank"]), height=160), use_container_width=True)

# R3: supporting figures
with st.expander("Supporting figures"):
    st.markdown("**Month by month** (whole choice)")
    st.dataframe(selection.drop(columns=["month_no"]), hide_index=True,
                 column_config={"Share of peak": st.column_config.NumberColumn(format="percent")})
    if cards:
        st.markdown("**Low season vs peak month**, per card")
        st.dataframe(pd.DataFrame([{
            "#": c["target_rank"], "Room": c["room_type"], "Country": c["country_code"],
            "Bookings (low / peak)": f"{c['low_bookings']:.0f} / {c['peak_bookings']:.0f}",
            "Lead time, median days": f"{c['low_median_lead'] or 0:.0f} / {c['peak_median_lead'] or 0:.0f}",
            "Nights": f"{c['low_avg_stay'] or 0:.1f} / {c['peak_avg_stay'] or 0:.1f}",
            "With children": f"{c['low_family_share'] or 0:.0%} / {c['peak_family_share'] or 0:.0%}",
            "Average price": f"{c['low_avg_adr'] or 0:.0f} / {c['peak_avg_adr'] or 0:.0f}",
        } for c in cards]), hide_index=True)

# R4: run summary
with st.expander("Run summary (for the team)"):
    excluded = run["rows_excluded"]
    left, right = st.columns(2)
    left.markdown("**Bookings**")
    left.table({
        "": ["Read", "Used", *[f"Excluded: {r.replace('_', ' ')}" for r in
                               ("canceled", "zero_nights", "zero_guests", "duplicate")],
             "Kept, country unknown", "Kept, price 0 or below (not in average price)"],
        "Rows": [f"{run['rows_read']:,}", f"{run['rows_used']:,}",
                 *[f"{excluded.get(r, 0):,}" for r in ("canceled", "zero_nights", "zero_guests", "duplicate")],
                 f"{excluded.get('kept_unknown_country', 0):,}", f"{excluded.get('kept_no_price', 0):,}"],
    })
    right.markdown("**Steps**")
    steps = run["step_durations"]
    right.table({"Step": list(steps), "Seconds": [f"{ms / 1000:.1f}" for ms in steps.values()]})
    right.caption(f"Total {(run['duration_ms'] or 0) / 1000:.1f} s, Spark start-up included.")
    buttons = st.columns(4)
    if run["spark_app_id"]:
        buttons[0].link_button("Spark page of this run", backend.spark_page_url(run["spark_app_id"]), width="stretch")
    buttons[1].link_button("Resource use during the run", backend.dashboard_url("Resource use", run), width="stretch")
    buttons[2].page_link("views/run_history.py", label="Compare with another run", query_params={"run": str(run["run_id"])})
    st.markdown(f"**Settings:** engine **{settings.get('pipeline.engine')}**, from "
                f"{settings.get('launch.settings_from', 'current settings')}. "
                f"{summary.get('targets_found', 0)} targets found, {summary.get('targets_qualified', 0)} qualified.")
    differ = [k for k in rs.changed({k: v for k, v in settings.items() if k in rs.BY_KEY})]
    if differ:
        st.table({"Setting": [rs.BY_KEY[k].label for k in differ], "Used": [settings[k] for k in differ],
                  "Default": [rs.BY_KEY[k].default or "unset" for k in differ]})
    else:
        st.caption("All settings were at their defaults.")
    with st.expander("Full settings snapshot"):
        st.code(json.dumps(dict(sorted(settings.items())), indent=1))

# R5: CSV export
if cards:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["rank", "hotel", "room_type", "country", "when", "potential_room_nights_per_month",
                     "offers", "evidence", "channels"])
    for c in cards:
        writer.writerow([c["target_rank"], c["hotel"], c["room_type"], c["country_code"], c["when_text"],
                         round(c["potential"], 1), " | ".join(c["offers"]), " | ".join(c["evidence"]),
                         ", ".join(c["channels"])])
    st.download_button("Download recommendations (CSV)", buffer.getvalue(), "br5_promotions.csv", "text/csv")
