"""Data screen: what is stored, and import / re-import (spec-data-import.md)."""

import streamlit as st

import backend
import launch

MULTIPLIERS = [1, 10, 50]
STEP_LABELS = {
    "starting": "Starting",
    "read": "Reading the file",
    "check": "Checking values",
    "derive": "Preparing rows",
    "write_raw": "Writing the raw table",
    "write_dims": "Writing the dimension tables",
    "write_fact": "Writing the booking fact table",
    "verify": "Verifying the new data",
    "swap": "Switching to the new data",
}

st.title("Data")


def when(run):
    return run["started_at"].strftime("%Y-%m-%d %H:%M UTC")


def seconds(run):
    return f"{(run['duration_ms'] or 0) / 1000:.1f} s"


def multiplier_label(n):
    return "×1 (original data)" if n == 1 else f"×{n} (synthetic: the data repeated {n} times)"


def show_running(run, job):
    if run is None:
        st.info("Starting Spark… (this takes 15–30 s)")
        return
    st.info(f"Importing: {STEP_LABELS.get(run['step'], run['step'])}")
    progress = backend.write_progress(run)
    if progress is not None:
        fraction, written = progress
        st.progress(fraction, text=f"{written:,} of {run['rows_expected']:,} rows written")
    if job and job["log_tail"]:
        with st.expander("Spark log"):
            st.code(job["log_tail"])
    if job and st.button("Stop import", help="The stored data stays as it is."):
        backend.stop_job()
        st.rerun(scope="fragment")


def show_stored(status):
    imported = status["import"]
    if not status["loaded"]:
        st.info("No data yet. Import now to use the Promotion Finder.")
        return
    top, bottom = st.columns(2), st.columns(2)
    top[0].metric("Bookings", f"{status['counts']['fact_booking']:,}")
    if imported:
        top[1].metric("Multiplier", f"×{imported['multiplier']}")
        bottom[0].metric("Imported (UTC)", imported["started_at"].strftime("%d %b %H:%M"))
        bottom[1].metric("Import took", seconds(imported))
        st.caption(f"Source: {imported['source_file']} ({imported['source_bytes'] / 1e6:.1f} MB)")
        if imported["multiplier"] > 1:
            st.warning(f"Synthetic data: {multiplier_label(imported['multiplier'])}.")
    with st.expander("Tables"):
        st.table({"Table": list(status["counts"]), "Rows": [f"{c:,}" if c is not None else "missing"
                                                         for c in status["counts"].values()]})


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
    multiplier = st.selectbox("Data size", MULTIPLIERS, format_func=multiplier_label, key="multiplier",
                              disabled=busy)
    values = backend.current_values()
    st.caption(f"Uses the current Advanced Settings: writer **{values['pipeline.writer']}**, "
               f"{values['pipeline.write_partitions']} write partitions, executor memory "
               f"{values['spark.executor.memory']}.")

    if busy:
        st.button("Import", disabled=True, help="Busy: another BR5 run is in progress.")
        return
    if not loaded:
        if st.button("Import", type="primary"):
            start(multiplier)
        return
    if not st.session_state.get("confirm_replace"):
        if st.button("Re-import"):
            st.session_state.confirm_replace = True
            st.rerun(scope="fragment")
        return
    st.warning(f"This replaces the {status['counts']['fact_booking']:,} stored bookings. "
               "The current data stays until the new import succeeds.")
    yes, no = st.columns(2)
    if yes.button("Yes, replace", type="primary"):
        st.session_state.confirm_replace = False
        start(multiplier)
    if no.button("Cancel"):
        st.session_state.confirm_replace = False
        st.rerun(scope="fragment")


def start(multiplier):
    error = backend.launch_job("import", launch.import_args(multiplier))
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
    running = status["busy"] or (job and job["running"])

    if status["busy"] and status["busy"]["run_type"] != "import":
        st.info("Busy: a promotion run is in progress. Import is possible when it has finished.")
    elif running:
        run = (job["run"] if job and job["running"] else None) or status["busy"]
        if status["busy"] and run and run["run_id"] != status["busy"]["run_id"]:
            run = status["busy"]
        show_running(run, job if job and job["running"] else None)
    elif job and job["exit_code"] not in (0, None) and job["run"] is None:
        # Spark failed before the job could record anything.
        st.error(f"The import could not start (exit code {job['exit_code']}). Log: {job['log']}")
        st.code(job["log_tail"])

    show_stored(status)
    show_last_failure(status, running)
    st.divider()
    import_controls(status, bool(running))


panel()
