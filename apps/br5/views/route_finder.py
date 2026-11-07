"""Route Finder (docs/spec/br6/spec.md, section 3): choose a city, the start,
end and stops and the kinds of places, optionally a visitor; run; read the
suggested stops along the way. Plan: docs/plan/br6/plan-pipeline.md, section 8."""

import csv
import io
import json

import pandas as pd
import streamlit as st

import route_backend as backend
import route_launch
import route_reference as reference
import route_rules as rules

GENERAL = -1  # user ids start at 0
MAX_STOPS = 3
SEARCH_LIMIT = 50
STEP_LABELS = {
    "starting": "Starting",
    "read": "Reading the places and their visitors",
    "clean": "Applying the cleaning rules",
    "match": "Finding places near the route",
    "visitor": "Reading the visitor's habits",
    "score": "Scoring the places",
    "write": "Saving the result",
}
EXCLUDED = {"no_location": "no location", "far_from_city": "far from the city", "not_offered": "Not offered type"}

st.title("Route Finder")
st.caption("Good stops along your way: choose a city, where you start and end, and the kinds of places you like.")

status = backend.data_status()
if not status["ready"]:
    st.error(status["reason"])
    st.stop()
if not status["loaded"]:
    st.info("No data yet. Import it on the Route Finder Data page first.")
    st.page_link("views/br6_data.py", label="Go to Data")
    st.stop()

values = backend.current_values()
rule_values = backend.CATALOG.options(values)["rules"]
import_id = backend.stored_import_id()
sha = backend.grouping_hash()
backend.grouping(import_id, sha)
cities = {c["city_code"]: c for c in backend.city_options(
    import_id, rule_values["min_place_visitors"], rule_values["max_centre_km"], sha)}

# ?run=<id>: an earlier run's result (links from Run History). The city starts as that run's.
asked = st.query_params.get("run")
asked_result = backend.route_result(asked) if asked else None
if asked_result and st.session_state.get("br6_choices_from") != asked:
    st.session_state.br6_choices_from = asked
    city_code = asked_result["run"]["criteria"].get("city")
    if city_code in cities:
        st.session_state.br6_city = city_code
elif "br6_city" not in st.session_state:
    # First visit: start from the latest run's city, so its result shows below.
    latest_result = backend.route_result()
    if latest_result and latest_result["run"]["criteria"].get("city") in cities:
        st.session_state.br6_city = latest_result["run"]["criteria"]["city"]


def city_label(code):
    c = cities[code]
    return f"{c['city_name']} ({c['country_name']}) · {c['places']:,} places to suggest"


def clear_points():
    for key in list(st.session_state):
        if key.startswith(("br6_point_", "br6_search_")):
            del st.session_state[key]
    st.session_state.br6_stops = 0
    st.session_state.pop("br6_visitor", None)


st.selectbox("City", list(cities), key="br6_city", format_func=city_label, index=None,
             placeholder="Choose a city", on_change=clear_points,
             help="Places to suggest: with a location, of a place type that is offered, and with at least "
                  f"{rule_values['min_place_visitors']:g} different visitors.")
city = cities.get(st.session_state.get("br6_city"))
if city is None:
    st.stop()


def place_label(p):
    parts = [p["name"] or f"Place {p['venue_id']}", p["category_name"]]
    if p["address"]:
        parts.append(p["address"])
    return " · ".join(parts)


def point_picker(label, slot, stop_no=None):
    """A search box and a drop-down of matching places. Returns the chosen
    venue id (kept in st.session_state[f"br6_point_{slot}"]). A stop
    (stop_no 1..3) also gets a Remove button."""
    key = f"br6_point_{slot}"
    labels = st.session_state.setdefault("br6_point_labels", {})
    columns = st.columns([1, 1.6, 0.5] if stop_no else [1, 2.1], vertical_alignment="bottom")
    text = columns[0].text_input(f"{label}: search", key=f"br6_search_{slot}", placeholder="part of the name")
    matches = backend.search_places(city, text, rule_values["max_centre_km"], import_id, SEARCH_LIMIT)
    options = [m["venue_id"] for m in matches]
    labels.update({m["venue_id"]: place_label(m) for m in matches})
    chosen = st.session_state.get(key)
    if chosen is not None and chosen not in options:
        options.insert(0, chosen)
    found = (f"{SEARCH_LIMIT} most visited matches" if len(matches) == SEARCH_LIMIT
             else f"{len(matches)} matches" if matches else "no match")
    columns[1].selectbox(label, options, key=key, index=None, format_func=lambda v: labels.get(v, str(v)),
                         placeholder=found)
    if stop_no:
        columns[2].button("Remove", key=f"br6_remove_{slot}", on_click=remove_stop, args=(stop_no,),
                          help="Remove this stop; the later stops move up.")
    return st.session_state.get(key)


def add_stop():
    st.session_state.br6_stops = st.session_state.get("br6_stops", 0) + 1


def remove_stop(stop_no):
    """Removes stop stop_no (1-based) and moves the later stops up."""
    count = st.session_state.get("br6_stops", 0)
    for i in range(stop_no, count):
        for prefix in ("br6_point_stop", "br6_search_stop"):
            st.session_state[f"{prefix}{i}"] = st.session_state.get(f"{prefix}{i + 1}", None if "point" in prefix
                                                                    else "")
    for prefix in ("br6_point_stop", "br6_search_stop"):
        st.session_state.pop(f"{prefix}{count}", None)
    st.session_state.br6_stops = count - 1


st.caption("Type part of a name to search; the lists show the most visited matching places.")
start = point_picker("Start", "start")
stops = [point_picker(f"Stop {i}", f"stop{i}", stop_no=i) for i in range(1, st.session_state.get("br6_stops", 0) + 1)]
if st.session_state.get("br6_stops", 0) < MAX_STOPS:
    st.button("Add a stop", on_click=add_stop, help=f"Up to {MAX_STOPS} stops, visited in this order.")
end = point_picker("End", "end")

types = st.multiselect("Place types", reference.OFFERED_TYPES, default=reference.OFFERED_TYPES, key="br6_types")
visitors = backend.visitor_options(city, rule_values["min_visitor_checkins"], rule_values["max_centre_km"],
                                   import_id, sha)
visitor_info = {v["user_id"]: v for v in visitors}


def visitor_label(user_id):
    if user_id == GENERAL:
        return "General visitor (popularity and closeness only)"
    v = visitor_info[user_id]
    return f"Visitor {user_id} · {v['checkins']:,} check-ins · mostly {', '.join(v['favourites'])}"


if st.session_state.get("br6_visitor") not in visitor_info:
    st.session_state.br6_visitor = GENERAL
st.selectbox("Visitor", [GENERAL, *visitor_info], key="br6_visitor", format_func=visitor_label,
             help=f"Anonymous visitors of this city with at least {rule_values['min_visitor_checkins']:g} check-ins "
                  "at places of the offered types; their own check-ins make the ranking personal.")

route = [start, *stops, end]
problems = []
if None in route:
    problems.append("Choose a start, an end and every stop you added.")
elif start == end:
    problems.append("The end must differ from the start.")
elif any(a == b for a, b in zip(route, route[1:])):
    problems.append("Two points in a row are the same place.")
if not types:
    problems.append("Choose at least one place type.")


@st.fragment(run_every=2)
def run_panel():
    status = backend.data_status()
    job = backend.launched_job()
    ours = job if job and job["running"] else None
    busy = status["busy"]
    if busy or ours:
        run = busy or (ours and ours["run"])
        if run and (run.get("br") == "BR5" or run["run_type"] == "import"):
            st.info(backend.busy_message(run) + " Find stops is possible when it has finished.")
        else:
            st.info(f"Running: {STEP_LABELS.get(run['step'], run['step']) if run else 'Starting Spark… (15–30 s)'}")
            if run and run.get("spark_app_id"):
                st.link_button("Live Spark page", backend.SPARK_LIVE_URL,
                               help="Spark's own page of this run: jobs and stages per step, while it runs.")
        if ours and st.button("Stop", help="Stop the job started from this app."):
            backend.stop_job()
            st.rerun(scope="fragment")
        return

    if job and job["kind"] == "run" and job["exit_code"] not in (0, None) and job["run"] is None:
        st.error(f"The run could not start (exit code {job['exit_code']}).")
        st.code(job["log_tail"])
    errors, _ = backend.validation(values)
    if errors:
        st.warning("Some settings are invalid; fix them on the Advanced Settings page to run.")
    for problem in problems:
        st.caption(problem)
    if st.button("Find stops", type="primary", disabled=bool(errors or problems)):
        error = backend.launch_job("run", route_launch.run_args(
            city["city_code"], start, end, stops, types if set(types) != set(reference.OFFERED_TYPES) else (),
            None if st.session_state.br6_visitor == GENERAL else st.session_state.br6_visitor))
        if error:
            st.error(error)
        else:
            st.rerun(scope="fragment")

    # A run finished since the page was drawn: redraw the result.
    latest = backend.latest_attempt()
    latest_id = str(latest["run_id"]) if latest else None
    if st.session_state.setdefault("br6_seen_run", latest_id) != latest_id and latest["status"] != "running":
        st.session_state.br6_seen_run = latest_id
        st.rerun()


run_panel()

attempt = backend.latest_attempt()
result = asked_result if asked else backend.route_result()
if asked:
    if result is None:
        st.error(f"No succeeded Route Finder run with id {asked}.")
        st.stop()
    st.info("Showing an earlier run's result. Open the Route Finder without a run in the link for the latest.")
elif attempt and result and attempt["run_id"] != result["run"]["run_id"]:
    if attempt["status"] == "failed":
        st.error(f"The last run failed, so the result below is from an earlier run.\n\n{attempt['error']}")
    elif attempt["status"] == "running" and not (status["busy"] and status["busy"]["run_id"] == attempt["run_id"]):
        st.warning("The last run stopped without finishing; the result below is from an earlier run.")
elif attempt and not result and attempt["status"] == "failed":
    st.error(f"The last run failed.\n\n{attempt['error']}")
if result is None:
    st.stop()

run, points, places, summary = result["run"], result["points"], result["places"], result["summary"]
criteria, settings = run["criteria"], run["settings"]
labels = rules.point_labels(points)
suggested = [p for p in places if p["rank"]]
personal = bool(criteria.get("visitor"))
run_city = next((c for c in cities.values() if c["city_code"] == criteria.get("city")), None)

st.divider()
st.subheader("Result")
route_text = " → ".join(p["name"] or f"Place {p['venue_id']}" for p in points)
who = f"Visitor {criteria['visitor']}" if personal else "General visitor"
st.markdown(f"**{run_city['city_name'] if run_city else criteria.get('city')}: {route_text}**  \n"
            f"{who} · {criteria.get('types', '').replace(',', ', ')}  \n"
            f"Run on {run['started_at']:%d %b %Y %H:%M} UTC in {(run['duration_ms'] or 0) / 1000:.1f} s")
if str(run["import_id"]) != str(import_id):
    st.warning("This result was computed on data that has since been re-imported. Run again to update it.")

# R1: route summary
m = st.columns(4)
m[0].metric("Route length", f"{summary.get('route_km', 0):.1f} km")
m[1].metric("Places near the route", f"{summary.get('found', 0):,}")
m[2].metric("After the filters", f"{summary.get('after_filters', 0):,}")
m[3].metric("Suggested", f"{len(suggested)}")
max_detour = float(settings.get("rules.max_detour_km", "1"))
st.caption(f"Distances are straight lines (the app knows no roads). Near the route = at most {max_detour:g} km "
           "from it. Filters: the chosen place types, and at least "
           f"{float(settings.get('rules.min_place_visitors', '3')):g} different visitors.")
if summary.get("multiplier", 1) > 1:
    st.caption(f"Data imported ×{summary['multiplier']} (synthetic); figures are per original check-in.")

# R2: suggestion cards
st.markdown("#### Suggested stops")
if summary.get("message"):
    st.warning(summary["message"])
weights = ({"popularity": settings.get("rules.weight_popularity"), "fit": settings.get("rules.weight_fit"),
            "closeness": settings.get("rules.weight_closeness")} if personal else
           {"popularity": settings.get("rules.general_weight_popularity"),
            "closeness": settings.get("rules.general_weight_closeness")})
for p in suggested:
    with st.container(border=True):
        st.markdown(f"##### #{p['rank']} · {p['name'] or 'Place ' + str(p['venue_id'])}")
        st.caption(" · ".join(x for x in (p["category_name"], p["place_type"], p["address"]) if x))
        left, right = st.columns(2)
        left.markdown(f"**Detour:** {p['detour_km']:.2f} km off the route  \n"
                      f"**Position:** {rules.position_text(p['position_km'], p['leg'], labels)}")
        lines = [f"**Popular because:** {p['visitors']:,} different visitors, {p['checkins']:,} check-ins here"]
        if personal:
            visits = (f"you went here {p['visitor_visits']} time{'s' if p['visitor_visits'] != 1 else ''}"
                      if p["visitor_visits"] else "you haven't been here")
            lines.append(f"**Fits you because:** {p['type_share']:.0%} of your check-ins are {p['place_type']}; "
                         f"{visits}")
        right.markdown("  \n".join(lines))
        parts = [f"popularity {p['popularity']:.2f} × {weights['popularity']}%"]
        if personal:
            parts.append(f"fit {p['fit']:.2f} × {weights['fit']}%")
        parts.append(f"closeness {p['closeness']:.2f} × {weights['closeness']}%")
        st.markdown(f"**Score {p['score']:.3f}** = " + " + ".join(parts))

# R3: supporting figures
with st.expander("Supporting figures: every place near the route"):
    per_type = summary.get("suggestions_per_type", {})
    if per_type:
        st.markdown("**Suggestions per place type**")
        st.bar_chart(pd.DataFrame({"Place type": list(per_type), "Suggestions": list(per_type.values())}),
                     x="Place type", y="Suggestions", horizontal=True)
    counts = summary.get("status_counts", {})
    st.caption(" · ".join(f"{name}: {n:,}" for name, n in counts.items()))
    st.dataframe(pd.DataFrame([{
        "Rank": p["rank"] or None, "Place": p["name"], "Category": p["category_name"], "Type": p["place_type"],
        "Status": p["status"], "Detour (km)": round(p["detour_km"], 3), "From start (km)": round(p["position_km"], 2),
        "Visitors": p["visitors"], "Check-ins": p["checkins"], "Popularity": p["popularity"], "Fit": p["fit"],
        "Closeness": p["closeness"], "Score": p["score"],
    } for p in places]), hide_index=True, width="stretch")

# R4: run summary
with st.expander("Run summary (for the team)"):
    excluded = run["rows_excluded"]
    left, right = st.columns(2)
    left.markdown("**Check-ins of the city**")
    left.table({
        "": ["Read", "Used", *[f"Excluded: {label}" for label in EXCLUDED.values()]],
        "Check-ins": [f"{run['rows_read']:,}", f"{run['rows_used']:,}",
                      *[f"{excluded.get(r, 0):,}" for r in EXCLUDED]],
    })
    if personal and summary.get("visitor"):
        v = summary["visitor"]
        left.caption(f"Visitor {criteria['visitor']}: {v['used_checkins']:,} used check-ins, " + ", ".join(
            f"{t} {n:,}" for t, n in sorted(v["type_checkins"].items(), key=lambda kv: -kv[1])))
    right.markdown("**Steps**")
    steps = run["step_durations"]
    right.table({"Step": list(steps), "Seconds": [f"{ms / 1000:.1f}" for ms in steps.values()]})
    right.caption(f"Total {(run['duration_ms'] or 0) / 1000:.1f} s, Spark start-up included.")
    buttons = st.columns(3)
    if run["spark_app_id"]:
        buttons[0].link_button("Spark page of this run", backend.spark_page_url(run["spark_app_id"]), width="stretch")
    buttons[1].link_button("Resource use during the run", backend.dashboard_url("Resource use", run), width="stretch")
    buttons[2].page_link("views/br6_run_history.py", label="Compare with another run",
                         query_params={"run": str(run["run_id"])})
    st.markdown(f"**Settings:** engine **{settings.get('pipeline.engine')}**, matching "
                f"**{settings.get('pipeline.matching')}**, distance **{settings.get('pipeline.distance')}**, place "
                f"statistics **{settings.get('pipeline.place_stats')}**; from "
                f"{settings.get('launch.settings_from', 'current settings')}.")
    catalog = backend.CATALOG
    differ = catalog.changed({k: v for k, v in settings.items() if k in catalog.by_key})
    if differ:
        st.table({"Setting": [catalog.by_key[k].label for k in differ], "Used": [settings[k] for k in differ],
                  "Default": [catalog.by_key[k].default or "unset" for k in differ]})
    else:
        st.caption("All settings were at their defaults.")
    with st.expander("Full settings snapshot"):
        st.code(json.dumps(dict(sorted(settings.items())), indent=1))

# R5: CSV export
if suggested:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["rank", "name", "category", "place_type", "address", "detour_km", "km_from_start", "between",
                     "visitors", "checkins", "visitor_visits", "type_share", "popularity", "fit", "closeness",
                     "score"])
    for p in suggested:
        writer.writerow([p["rank"], p["name"], p["category_name"], p["place_type"], p["address"],
                         round(p["detour_km"], 3), round(p["position_km"], 2),
                         f"{labels[p['leg']]} - {labels[p['leg'] + 1]}", p["visitors"], p["checkins"],
                         p["visitor_visits"], round(p["type_share"], 4), p["popularity"], p["fit"], p["closeness"],
                         p["score"]])
    st.download_button("Download suggestions (CSV)", buffer.getvalue(), "br6_route_stops.csv", "text/csv")
