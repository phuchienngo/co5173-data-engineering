# BR6 Plan: Shared App (BR5 + BR6)

Implements section 2 of [spec.md](../../spec/br6/spec.md) (G1–G7). BR6 reuses the BR5 Streamlit app in container `br5-app`. This plan sets up the menu, the selection pages and the sidebar once, during the data import stage. Later BR6 plans only fill in their own pages.

## 1. Decisions

| # | Decision | Why |
|---|---|---|
| A1 | **BR5's pages keep their files and their addresses** (`/`, `/settings`, `/data`, `/run_history`) and are registered as **hidden pages** (`st.Page(..., visibility="hidden")`, Streamlit 1.66). The menu items *Advanced Settings*, *Data* and *Run History* point to **new selection pages**. | Every existing link (Grafana's `/run_history?run=…`, the benchmark script's `/?run=…` and `/run_history?a=…&run=…`, BR5's own `page_link`s) still opens BR5 content directly (G4). The BR5 page files stay unchanged (G2). |
| A2 | **BR6 pages get their own addresses:** `/route_finder`, `/route_settings`, `/route_data`, `/route_run_history`. Only `/route_finder` is in the menu. | Direct links for BR6, in the same pattern as BR5's (G4). |
| A3 | **BR6's app code lives in the same folder, `apps/br5/`,** as `route_backend.py`, `choose.py` and `views/route_finder.py`, `views/br6_*.py` and `views/choose_*.py`. (The BR6 views are not called `route_settings.py` etc., because `jobs/br6_route/route_settings.py` is on the same Python path.) | One Streamlit app has one main folder. The folder name is historical; renaming it would change BR5's compose service and scripts. |
| A4 | **The sidebar Monitoring block depends on the page's group:** BR5, BR6 or both (selection pages). | G6. BR5 pages render exactly today's block through the unchanged `backend.monitoring_links()`. |

## 2. Navigation

| Menu item (order) | Page file | Address | Group |
|---|---|---|---|
| Promotion Finder | `views/promotion_finder.py` (unchanged, default) | `/` | BR5 |
| **Route Finder** | `views/route_finder.py` (new) | `/route_finder` | BR6 |
| Advanced Settings | `views/choose_settings.py` (new) | `/choose_settings` | both |
| Data | `views/choose_data.py` (new) | `/choose_data` | both |
| Run History | `views/choose_run_history.py` (new) | `/choose_run_history` | both |
| *(hidden)* | `views/settings.py`, `views/data.py`, `views/run_history.py` (unchanged) | `/settings`, `/data`, `/run_history` | BR5 |
| *(hidden)* | `views/br6_settings.py`, `views/br6_data.py`, `views/br6_run_history.py` (new) | `/route_settings`, `/route_data`, `/route_run_history` | BR6 |

`app.py` is the only BR5 file that changes: the page list, and the call that draws the monitoring block for the page's group.

**Selection pages** (`choose_*.py`) have the menu item as title and two bordered panels side by side:
- **Promotion Finder:** one line on what it is (hotel bookings, low-season promotions) and an **Open** link to the BR5 page.
- **Route Finder:** the same for BR6 (check-ins, stops along a route).
- Each panel has one status line, for example "119,390 bookings stored" or "2,629,541 check-ins in 15 cities", or "a run is in progress".

## 3. Sidebar Monitoring block (G6)

| Page group | Shows |
|---|---|
| BR5 | Exactly today's block: Grafana *Run performance*, *Resource use*, *Data and result*, and *Spark history* |
| BR6 | BR6's three Grafana dashboards (added by the monitoring plan; until then only *Spark history*) |
| Both (selection pages) | Two small groups under the headings *Promotion Finder* and *Route Finder*, each with its links |

Grafana's BR5 and BR6 folders and the shared Spark history page (M9, M10) belong to the monitoring plan.

## 4. BR6 pages in this stage

| Page | In this stage |
|---|---|
| Route Finder | Placeholder. With no data: "No data yet" and a link to the BR6 Data page. With data: a note that the finder comes with the pipeline plan |
| BR6 Data | Complete ([plan-data-import.md](plan-data-import.md), section 8) |
| BR6 Advanced Settings | Placeholder showing the settings an import uses (defaults until the config plan builds the screen) |
| BR6 Run History | Placeholder listing BR6 imports (time, status, duration, cities, rows) until the monitoring plan builds the screen |

Each BR6 page shows the busy message when a BR5 job is running (G7). BR5 pages are not changed for this.

`route_backend.py` is the BR6 counterpart of `backend.py`: the ClickHouse client, its own launcher state (so BR5's is not touched), the data status and the job start and stop calls from `route_launch.py`.

## 5. Verification

| Check | Expected | Covers |
|---|---|---|
| Open the menu | Five items: Promotion Finder, Route Finder, Advanced Settings, Data, Run History | G1 |
| Each selection page → Promotion Finder | The same page as in the BR5 result screenshots (`docs/result/br5/`), apart from the extra menu item | G2 |
| Grafana's BR5 links to Run History, `/?run=<id>`, `/run_history?a=<id>&run=<id>` | Open BR5 content directly | G4 |
| Promotion Finder → "Go to Data" and "Compare with another run" | Open BR5's Data and Run History, as before | G4 |
| Sidebar on a BR5 page, a BR6 page and a selection page | Today's block / BR6 links / both groups | G6 |
| BR6 page during a BR5 import | Busy message | G7 |

## 6. Known differences on BR5 screens

- **The extra menu item,** accepted in the spec (G1).
- **No menu item is highlighted on BR5's Advanced Settings, Data and Run History pages.** Hidden pages are not menu items, so Streamlit has nothing to highlight; before this change, the menu item of the open page was highlighted. The page content is unchanged. If this matters for the report's screenshots, tell me before implementation. The alternative is a hand-drawn menu (`st.page_link`), which looks slightly different from Streamlit's own menu on every page.
