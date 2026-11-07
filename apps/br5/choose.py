"""Selection pages of the shared app: the Advanced Settings, Data and Run
History menu items open a page with one panel per BR
(docs/plan/br6/plan-shared-app.md, section 2)."""

import streamlit as st

import backend
import route_backend

BR5, BR6, BOTH = "br5", "br6", "both"
NAMES = {BR5: "Promotion Finder", BR6: "Route Finder"}
ABOUT = {
    BR5: "Hotel bookings: when the low season is and which promotions fit it.",
    BR6: "Visitor check-ins in 15 cities: which places are worth a stop along a route.",
}


def monitoring_links():
    """Sidebar on a selection page: both BRs' links, grouped."""
    st.sidebar.markdown("**Monitoring**")
    st.sidebar.caption(NAMES[BR5])
    for name in backend.DASHBOARDS:
        st.sidebar.link_button(f"Grafana: {name}", backend.dashboard_url(name), width="stretch")
    st.sidebar.link_button("Spark history", backend.SPARK_HISTORY_URL, width="stretch", key="br5-spark-history")
    st.sidebar.caption(NAMES[BR6])
    route_backend.monitoring_buttons()


def page(title, intro, pages, status):
    """pages: {BR5: page file, BR6: page file}; status: {BR5: fn, BR6: fn},
    each returning one status line (errors are shown, never raised)."""
    st.title(title)
    st.caption(intro)
    for column, br in zip(st.columns(2), (BR5, BR6)):
        with column.container(border=True):
            st.subheader(NAMES[br])
            st.write(ABOUT[br])
            try:
                st.caption(status[br]())
            except Exception as exc:  # the panel still opens the page
                st.caption(f"Status unavailable: {exc}")
            st.page_link(pages[br], label=f"Open {NAMES[br]} {title.lower()}", icon=":material/arrow_forward:")
