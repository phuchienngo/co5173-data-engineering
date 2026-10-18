import streamlit as st

import backend

st.title("Promotion Finder")

status = backend.data_status()
if not status["ready"]:
    st.error(status["reason"])
elif not status["loaded"]:
    st.info("No data yet. Import it on the Data page first.")
    st.page_link("views/data.py", label="Go to Data")
else:
    st.info("The Promotion Finder will be added in the next step.")
