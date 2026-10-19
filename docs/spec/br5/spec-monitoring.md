# BR5 Spec: Monitoring and Run History

Part of [spec.md](spec.md). Grafana (already in the stack) shows how the pipeline behaves; the app shows the business result.

## 1. Run history

Every import and every run is recorded in the data store (one row each) so that Grafana and the app can show them.

| ID | What is recorded | Priority |
|---|---|---|
| M1 | Run id, type (import / run), start time, total duration, success or failure | Must |
| M2 | Duration of each step (read, clean, aggregate, write) | Must |
| M3 | Criteria chosen and a snapshot of all settings used (see config spec C6) | Must |
| M4 | Rows read, rows used, rows excluded per reason (cleaning rules) | Must |
| M5 | Data multiplier used, if any | Could |

The app shows the history as a table and lets us pick **two runs to compare side by side** (Should).

## 2. Grafana dashboards

One folder for BR5, provisioned automatically with the stack (like the existing dashboards).

| ID | Dashboard | Shows | Priority |
|---|---|---|---|
| M6 | **Run performance** | Run duration over time; duration per step; filter by settings (for example partitions, AQE on/off) to compare | Must |
| M7 | **Resource use** | CPU and memory of Spark and ClickHouse during runs, with run start/end marked | Must |
| M8 | **Data and result** | Rows imported, rows excluded per reason, latest recommendation summary, import status | Should |

Each dashboard has a link back to the app, and the app has a link to Grafana and to the Spark web page for step-level detail.

## 3. Notes for the plan

- Prometheus currently collects metrics from ClickHouse and the Spark master/worker only. Since each run is its own Spark application, getting the run's own memory/CPU figures into Prometheus needs a decision in the plan.
- Run-history figures are the main evidence for the report's "performance of the data engineering solution" part, so they must be stored in a way that survives restarts.

## 4. Decided (see [plan-monitoring.md](../../plan/br5/plan-monitoring.md))

- **Resource use over time** comes from the containers (CPU and memory of the Spark worker, the app that runs the Spark driver, and ClickHouse), every 5 s, not from each run's Spark application. Prometheus keeps it for 90 days.
- **Each run also stores exact resource figures** with its history row: Spark's CPU, GC, shuffle, spill and peak memory, in total and per step; ClickHouse's queries, CPU, peak memory and rows read and written. These survive restarts.
- **Step-level detail:** a Spark history server keeps each run's Spark web page after the run ends. It needs Spark's event log, which is a setting (on by default).
- **Dashboards** sit in their own BR5 folder. Run figures come from the run history in ClickHouse; resource time series from Prometheus.
- **Run History** is a fourth screen of the app, with the side-by-side comparison of two runs.
