"""Route Finder (docs/spec/br6/spec.md, section 3). Placeholder until the
pipeline plan: shows whether data is there and links to it."""

import streamlit as st

import route_backend as backend

st.title("Route Finder")
st.caption("Good stops along your way: choose a city, where you start and end, and the kinds of places you like.")

status = backend.data_status()
busy = status.get("busy")
if busy:
    st.info(backend.busy_message(busy))
if not status["ready"]:
    st.error(status["reason"])
elif not status["loaded"]:
    st.info("No data yet. Import it on the Route Finder Data page first.")
    st.page_link("views/br6_data.py", label="Go to Data")
else:
    st.info(f"{status['counts']['fact_checkin']:,} check-ins in {len(status['cities'])} cities are ready. "
            "The finder itself comes with the next stage (the Route Finder pipeline).")
    st.page_link("views/br6_data.py", label="See the data")
