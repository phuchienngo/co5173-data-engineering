"""The shared app: BR5 Low-Season Promotion Finder (spec: docs/spec/br5/spec.md)
and BR6 Route Finder (docs/spec/br6/spec.md, section 2).

BR5's pages keep their files and addresses as hidden pages, so every existing
link still opens BR5 directly; the Advanced Settings, Data and Run History menu
items open a selection page (docs/plan/br6/plan-shared-app.md).
"""

import streamlit as st

import backend
import choose
import route_backend

st.set_page_config(page_title="BR5 Promotion Finder", layout="wide")

# (page, group): the group picks the sidebar Monitoring block.
PAGES = [
    (st.Page("views/promotion_finder.py", title="Promotion Finder", default=True), choose.BR5),
    (st.Page("views/route_finder.py", title="Route Finder", url_path="route_finder"), choose.BR6),
    (st.Page("views/choose_settings.py", title="Advanced Settings", url_path="choose_settings"), choose.BOTH),
    (st.Page("views/choose_data.py", title="Data", url_path="choose_data"), choose.BOTH),
    (st.Page("views/choose_run_history.py", title="Run History", url_path="choose_run_history"), choose.BOTH),
    # BR5 content, at its existing addresses (/settings, /data, /run_history).
    (st.Page("views/settings.py", title="Advanced Settings", visibility="hidden"), choose.BR5),
    (st.Page("views/data.py", title="Data", visibility="hidden"), choose.BR5),
    (st.Page("views/run_history.py", title="Run History", visibility="hidden"), choose.BR5),
    # BR6 content.
    (st.Page("views/br6_settings.py", title="Advanced Settings", url_path="route_settings",
             visibility="hidden"), choose.BR6),
    (st.Page("views/br6_data.py", title="Data", url_path="route_data", visibility="hidden"), choose.BR6),
    (st.Page("views/br6_run_history.py", title="Run History", url_path="route_run_history",
             visibility="hidden"), choose.BR6),
]
page = st.navigation([p for p, _ in PAGES])
group = next(g for p, g in PAGES if p.url_path == page.url_path)
if group == choose.BR5:
    backend.monitoring_links()
elif group == choose.BR6:
    route_backend.monitoring_links()
else:
    choose.monitoring_links()
page.run()
