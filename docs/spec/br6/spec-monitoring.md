# BR6 Spec: Monitoring and Run History

Part of [spec.md](spec.md). Grafana (already in the stack) shows how the pipeline behaves; the app shows the business result.

## 1. Run history

Every import and every run is recorded in the data store (one row each) so that Grafana and the app can show them.

| ID | What is recorded | Priority |
|---|---|---|
| M1 | Run id, type (import / run), start time, total duration, success or failure | Must |
| M2 | Duration of each step (read, clean, find places near the route, score, write) | Must |
| M3 | The choices made (city, start, end, stops, place types, visitor) and a snapshot of all settings used (see config spec C6) | Must |
| M4 | Check-ins read, used, and excluded per reason (cleaning rules); places near the route and places suggested | Must |
| M5 | Data multiplier used, if any | Could |

The app shows the history as a table and lets us pick **two runs of BR6 to compare side by side** (Should). It is the *Route Finder* panel of the *Run History* menu item (main spec, section 2), built like BR5's Run History screen in the BR5 result pages.

## 2. Grafana dashboards

One folder for BR6, provisioned automatically with the stack (like the existing dashboards).

| ID | Dashboard | Shows | Priority |
|---|---|---|---|
| M6 | **Run performance** | Run duration over time; duration per step; filter by city, settings (for example place-matching method, number of partitions, AQE on/off) and data multiplier to compare | Must |
| M7 | **Resource use** | CPU and memory of Spark and ClickHouse during runs, with run start/end marked | Must |
| M8 | **Data and result** | Check-ins imported per city, check-ins excluded per reason, places near the route and suggested per run, import status | Should |

**One shared monitoring place, viewed per BR** (the same idea as the shared app, main spec section 2):

| ID | Requirement | Priority |
|---|---|---|
| M9 | Grafana stays one instance. BR5's and BR6's dashboards sit in separate folders, so the folder list is the BR picker. A small start dashboard that links to each BR's folder is optional (decided: not built, see [plan-monitoring.md](../../plan/br6/plan-monitoring.md) section 9) | Must (start dashboard: Could) |
| M10 | The Spark history page stays one shared server. BR6's Spark applications are named with a "BR6" prefix, so the list can be searched by BR; a run's own page is opened directly from Run History. BR5's names and pages are not changed | Must |

Each dashboard has a link back to the BR6 Run History in the app (a direct link, main spec G4), and the app has a link to Grafana and to the Spark web page for step-level detail (sidebar, main spec G6).

## 3. Notes for the plan

- The BR5 solution for resources can be reused: container CPU and memory over time from Prometheus, exact per-run figures (Spark CPU, GC, shuffle, spill, peak memory; ClickHouse queries, CPU, peak memory, rows read and written) stored with each run-history row, and a Spark history server for step-level detail after a run ends. The plan confirms what is reused and what is new (see [BR5 monitoring spec](../br5/spec-monitoring.md), section 4).
- The app container is shared with BR5, so its CPU and memory figures in BR6's Resource use dashboard are the same container as in BR5's.
- BR6 dashboards go in their own folder and their own provisioning directory, because a provider reads its directory recursively.
- Run-history figures are the main evidence for the report's "performance of the data engineering solution" part, so they must be stored in a way that survives restarts.
