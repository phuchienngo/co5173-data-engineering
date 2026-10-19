"""BR5 Low-Season Promotion Finder (spec: docs/spec/br5/spec.md)."""

import streamlit as st

import backend

st.set_page_config(page_title="BR5 Promotion Finder", layout="wide")

pages = [
    st.Page("views/promotion_finder.py", title="Promotion Finder", default=True),
    st.Page("views/settings.py", title="Advanced Settings"),
    st.Page("views/data.py", title="Data"),
    st.Page("views/run_history.py", title="Run History"),
]
page = st.navigation(pages)
backend.monitoring_links()
page.run()
