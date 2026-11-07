"""BR6 Data screen: what is stored, and import / re-import (spec-data-import.md,
docs/plan/br6/plan-data-import.md section 8)."""

import json

import streamlit as st

import route_backend as backend
import route_launch

MULTIPLIERS = [1, 2, 5]
STEP_LABELS = {
    "starting": "Starting",
    "read": "Reading the files",
    "check": "Checking values",
    "derive": "Preparing rows",
    "write_raw": "Writing the raw table",
    "write_dims": "Writing the dimension tables",
    "write_fact": "Writing the check-in fact table",
    "verify": "Verifying the new data",
    "place_stats": "Counting check-ins and visitors per place",
    "swap": "Switching to the new data",
}

st.title("Data")
st.caption("Route Finder · visitor check-ins (Massive-STEPS)")


def when(run):
    return run["started_at"].strftime("%Y-%m-%d %H:%M UTC")


def seconds(run):
    return f"{(run['duration_ms'] or 0) / 1000:.1f} s"


def multiplier_label(n):
    return "×1 (original data)" if n == 1 else f"×{n} (synthetic: the check-ins repeated {n} times)"


def city_label(code):
    name, country = backend.city_names().get(code, (code, ""))
    return f"{name} ({country})" if country else name


def show_running(run, job):
    if run is None:
        st.info("Starting Spark… (this takes 15–30 s)")
        return
    st.info(f"Importing: {STEP_LABELS.get(run['step'], run['step'])}")
    progress = backend.write_progress(run)
    if progress is not None:
        fraction, written, expected = progress
        st.progress(fraction, text=f"{written:,} of {expected:,} rows written")
    if job and job["log_tail"]:
        with st.expander("Spark log"):
            st.code(job["log_tail"])
    if job and st.button("Stop import", help="The stored data stays as it is."):
        backend.stop_job()
        st.rerun(scope="fragment")


def show_stored(status):
    imported = status["import"]
    if not status["loaded"]:
        st.info("No data yet. Import now to use the Route Finder.")
        return
    counts = status["counts"]
    top, bottom = st.columns(3), st.columns(3)
    top[0].metric("Check-ins", f"{counts['fact_checkin']:,}")
    top[1].metric("Cities", len(status["cities"]))
    top[2].metric("Multiplier", f"×{status['multiplier']}")
    bottom[0].metric("Places", f"{counts['dim_place']:,}")
    if imported:
        bottom[1].metric("Imported (UTC)", imported["started_at"].strftime("%d %b %H:%M"))
        bottom[2].metric("Import took", seconds(imported))
        st.caption(f"Source: {imported['source_file']}, {imported['source_bytes'] / 1e6:.1f} MB")
    if status["place_stats_ready"]:
        st.caption(f"Place statistics (check-ins and visitors per place, built at import): "
                   f"{counts['place_stats']:,} places.")
    else:
        st.warning("Place statistics are missing (the data was imported before they existed): re-import, or set "
                   "*Place statistics* to *run* in Advanced Settings.")
    if status["multiplier"] > 1:
        st.warning(f"Synthetic data: {multiplier_label(status['multiplier'])}. Each copy counts as other "
                   "visitors at the same places, so rankings stay the same.")
    ungrouped = json.loads(imported["result_summary"] or "{}").get("ungrouped_categories", []) if imported else []
    if ungrouped:
        st.warning(f"{len(ungrouped)} categories are not in `data/br6/category_place_type.csv`, so they are "
                   f"*Not offered*: {', '.join(ungrouped[:15])}{' …' if len(ungrouped) > 15 else ''}")

    st.dataframe(
        [{"City": c["city_name"], "Country": c["country_name"], "Check-ins": c["checkins"],
          "Without location": c["no_location"], "Places": c["places"], "Visitors": c["visitors"]}
         for c in status["cities"]],
        hide_index=True, width="stretch",
        column_config={name: st.column_config.NumberColumn(format="localized")
                       for name in ("Check-ins", "Without location", "Places", "Visitors")},
    )
    st.caption("Check-ins as in the files (before the multiplier). Check-ins without location are kept; "
               "each run excludes and counts them.")
    with st.expander("Tables (star schema)"):
        st.table({"Table": list(counts), "Rows": [f"{c:,}" if c is not None else "missing"
                                                   for c in counts.values()]})


def show_last_failure(status, busy):
    last = status["last_import_run"]
    if not last or busy:
        return
    if status["import"] and last["run_id"] == status["import"]["run_id"]:
        return
    if last["status"] == "failed":
        st.error(f"The last import failed ({when(last)}), so the data above is unchanged.\n\n{last['error']}")
    elif last["status"] == "running":
        st.warning(f"The last import ({when(last)}) stopped without finishing; the data above is unchanged.")


def import_controls(status, busy):
    loaded = status["loaded"]
    st.subheader("Re-import" if loaded else "Import")
    available = backend.available_cities()
    cities = st.multiselect("Cities", available, default=available, format_func=city_label, key="br6_cities",
                            disabled=bool(busy), help="The import replaces all stored BR6 data with these cities.")
    multiplier = st.selectbox("Data size", MULTIPLIERS, format_func=multiplier_label, key="br6_multiplier",
                              disabled=bool(busy))
    values = backend.current_values()
    st.caption(f"Uses the current Route Finder settings: writer **{values['pipeline.writer']}**, "
               f"{values['pipeline.write_partitions']} write partitions, executor memory "
               f"{values['spark.executor.memory']}.")

    if busy:
        st.button("Import", disabled=True, help=busy)
        return
    if not cities:
        st.button("Import", disabled=True, help="Choose at least one city.")
        return
    chosen = [] if set(cities) == set(available) else cities
    if not loaded:
        if st.button("Import", type="primary"):
            start(chosen, multiplier)
        return
    if not st.session_state.get("br6_confirm_replace"):
        if st.button("Re-import"):
            st.session_state.br6_confirm_replace = True
            st.rerun(scope="fragment")
        return
    st.warning(f"This replaces the {status['counts']['fact_checkin']:,} stored check-ins. "
               "The current data stays until the new import succeeds.")
    yes, no = st.columns(2)
    if yes.button("Yes, replace", type="primary"):
        st.session_state.br6_confirm_replace = False
        start(chosen, multiplier)
    if no.button("Cancel"):
        st.session_state.br6_confirm_replace = False
        st.rerun(scope="fragment")


def start(cities, multiplier):
    error = backend.launch_job("import", route_launch.import_args(cities, multiplier))
    if error:
        st.error(error)
    else:
        st.rerun(scope="fragment")


@st.fragment(run_every=2)
def panel():
    status = backend.data_status()
    if not status["ready"]:
        st.error(status["reason"])
        return
    job = backend.launched_job("import")
    busy = status["busy"]
    own_import = busy and busy["br"] == "BR6" and busy["run_type"] == "import"
    running = own_import or (job and job["running"])

    if busy and not own_import:
        st.info(backend.busy_message(busy) + " Import is possible when it has finished.")
    elif running:
        run = (job["run"] if job and job["running"] else None) or busy
        if busy and run and run["run_id"] != busy["run_id"]:
            run = busy
        show_running(run, job if job and job["running"] else None)
    elif job and job["exit_code"] not in (0, None) and job["run"] is None:
        # Spark failed before the job could record anything.
        st.error(f"The import could not start (exit code {job['exit_code']}). Log: {job['log']}")
        st.code(job["log_tail"])

    show_stored(status)
    show_last_failure(status, running)
    st.divider()
    import_controls(status, backend.busy_message(busy) if busy else ("Busy: an import is starting." if running
                                                                     else None))


panel()
