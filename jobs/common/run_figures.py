"""Exact resource figures of one run, stored in <db>.pipeline_runs.resources.

Built from BR5's run_metrics.py (jobs/br5_promotion), which keeps its own copy.

- Spark: from the driver's REST API, in total ("spark.cpu_ms") and per step
  ("spark.cpu_ms@read"). Steps set their Spark job group to the step name.
- ClickHouse: from system.query_log. Run queries carry log_comment = run_id;
  an import's queries are those on its staging database.

Collected after the run is marked finished and before Spark stops, so they
don't add to the run time. Best effort: a failure is logged, never raised.
"""

import json
import os
import urllib.request

from run_store import Stopped

# Stage fields summed per run and per step: (figure, field, divisor).
STAGE_FIGURES = [
    ("cpu_ms", "executorCpuTime", 1e6),  # ns; JVM task threads only (not Python workers)
    ("run_ms", "executorRunTime", 1),
    ("gc_ms", "jvmGcTime", 1),
    ("input_bytes", "inputBytes", 1),
    ("input_rows", "inputRecords", 1),
    ("shuffle_read_bytes", "shuffleReadBytes", 1),
    ("shuffle_write_bytes", "shuffleWriteBytes", 1),
    ("spill_memory_bytes", "memoryBytesSpilled", 1),
    ("spill_disk_bytes", "diskBytesSpilled", 1),
    ("tasks", "numCompleteTasks", 1),
]
NO_STEP = "setup"


def prepare_event_log(conf):
    """Spark refuses to start when the event log directory is missing."""
    directory = (conf or {}).get("spark.eventLog.dir", "")
    if conf.get("spark.eventLog.enabled") == "true" and directory.startswith("file:"):
        os.makedirs(directory[len("file:"):], exist_ok=True)


def summarize_spark(executors, stages, jobs):
    """Totals and per-step figures from the REST API's allexecutors, stages and
    jobs lists. Pure, for tests."""
    step_of_stage = {}
    for job in sorted(jobs, key=lambda j: j.get("jobId", 0)):
        for stage_id in job.get("stageIds", []):
            step_of_stage.setdefault(stage_id, job.get("jobGroup") or NO_STEP)

    figures = {"spark.jobs": float(len(jobs))}
    stage_ids = set()
    for stage in stages:
        stage_ids.add(stage.get("stageId"))
        step = step_of_stage.get(stage.get("stageId"), NO_STEP)
        for name, field, divisor in STAGE_FIGURES:
            value = (stage.get(field) or 0) / divisor
            for key in (f"spark.{name}", f"spark.{name}@{step}"):
                figures[key] = figures.get(key, 0.0) + value
    figures["spark.stages"] = float(len(stage_ids))

    workers = [e for e in executors if e.get("id") != "driver"]
    driver = [e for e in executors if e.get("id") == "driver"]
    figures["spark.executors"] = float(len(workers))
    heaps = [(e.get("peakMemoryMetrics") or {}).get("JVMHeapMemory") for e in workers]
    heaps = [h for h in heaps if h is not None]
    if heaps:
        figures["spark.executor_peak_heap_bytes"] = float(max(heaps))
    if driver and (driver[0].get("peakMemoryMetrics") or {}).get("JVMHeapMemory") is not None:
        figures["spark.driver_peak_heap_bytes"] = float(driver[0]["peakMemoryMetrics"]["JVMHeapMemory"])
    return figures


def _get(url, timeout, attempts=2):
    # The driver's page shares the app container's one CPU with the app; under
    # load it can take longer than a few seconds to answer.
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return json.loads(response.read())
        except OSError:
            if attempt == attempts - 1:
                raise


def spark_ui(spark):
    """(driver page URL, application id), read once when the session starts:
    after a Stop interrupts a call to the JVM, the next py4j call can't be
    trusted, while the REST API itself still answers."""
    sc = spark.sparkContext
    return sc.uiWebUrl, sc.applicationId


def spark_figures(ui, timeout=10):
    url, app_id = ui
    if not url:  # spark.ui.enabled=false
        return {}
    base = f"{url}/api/v1/applications/{app_id}"
    return summarize_spark(_get(f"{base}/allexecutors", timeout), _get(f"{base}/stages", timeout),
                           _get(f"{base}/jobs", timeout))


# Async inserts are logged twice: the client's Insert and the server's
# AsyncInsertFlush. Rows and queries count the client side only; CPU and
# memory include the flushes, which do the real work.
CLICKHOUSE_QUERY = """
WITH query_kind != 'AsyncInsertFlush' AS client
SELECT countIf(client) AS queries,
       sumIf(query_duration_ms, client) AS query_ms,
       sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu_ms,
       max(memory_usage) AS peak_memory_bytes,
       sumIf(read_rows, client) AS read_rows,
       sumIf(read_bytes, client) AS read_bytes,
       sumIf(written_rows, client) AS written_rows,
       sumIf(written_bytes, client) AS written_bytes
FROM system.query_log
WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
  AND event_time >= toDateTime({started:DateTime64(3, 'UTC')}) - 1
  AND {which}
"""


def clickhouse_figures(client, run, staging_db):
    client.command("SYSTEM FLUSH LOGS")
    if run.row["run_type"] == "import":
        which = f"has(databases, '{staging_db}')"
    else:
        which = "log_comment = {run_id:String}"
    result = client.query(CLICKHOUSE_QUERY.replace("{which}", which),
                          parameters={"started": run.row["started_at"], "run_id": run.run_id})
    row = result.named_results().__next__()
    if not row["queries"]:
        return {}
    return {f"clickhouse.{k}": float(v or 0) for k, v in row.items()}


def record(run, client, staging_db, ui=None):
    """Collects and stores the run's figures (ui: from spark_ui(), None
    without Spark). Never raises."""
    figures = {}
    for name, collect in (("Spark", lambda: spark_figures(ui) if ui else {}),
                          ("ClickHouse", lambda: clickhouse_figures(client, run, staging_db))):
        try:
            figures.update(collect())
        except (Exception, Stopped) as exc:
            run.log(f"{name} figures not collected: {exc}")
    if figures:
        try:
            run.update(resources=figures)
            run.log(f"figures: Spark CPU {figures.get('spark.cpu_ms', 0) / 1000:.1f}s, "
                    f"shuffle {figures.get('spark.shuffle_write_bytes', 0) / 2**20:.1f} MB; ClickHouse "
                    f"{figures.get('clickhouse.queries', 0):.0f} queries, "
                    f"{figures.get('clickhouse.read_rows', 0):,.0f} rows read")
        except (Exception, Stopped) as exc:
            run.log(f"figures not stored: {exc}")
