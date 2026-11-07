"""Run history (<db>.pipeline_runs), the busy lock and the stored settings
(<db>.settings) of a batch BR with its own database.

Built from BR5's runs.py (jobs/br5_promotion), which keeps its own copy. Used
by BR6 (docs/plan/br6/plan-data-import.md, P3); the database and the log
prefix are parameters.
"""

import json
import signal
import threading
import time
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

RUNS_TABLE = "pipeline_runs"
SETTINGS_TABLE = "settings"
CURRENT = "__current__"
HEARTBEAT_SECONDS = 10
# A running job whose heartbeat is older than this is treated as dead.
STALE_SECONDS = 30

RUN_COLUMNS = [
    "run_id", "run_type", "status", "step", "started_at", "finished_at", "duration_ms", "step_durations",
    "rows_read", "rows_expected", "rows_written", "multiplier", "writer", "source_file", "source_bytes",
    "import_id", "error", "heartbeat_at", "updated_at", "criteria", "settings", "rows_used", "rows_excluded",
    "result_count", "result_summary", "spark_app_id", "resources",
]


def now():
    return datetime.now(timezone.utc)


class Run:
    """One row of <db>.pipeline_runs. Every change inserts a new version."""

    def __init__(self, client, db, run_type, run_id=None, prefix="", **fields):
        self._client = client
        self._db = db
        self._prefix = prefix
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self.on_step = None  # called with the step name (Spark: job group, for per-step figures)
        started = now()
        self.row = {
            "run_id": run_id or str(uuid.uuid4()),
            "run_type": run_type,
            "status": "running",
            "step": "starting",
            "started_at": started,
            "finished_at": None,
            "duration_ms": None,
            "step_durations": {},
            "rows_read": 0,
            "rows_expected": 0,
            "rows_written": 0,
            "multiplier": 1,
            "writer": "",
            "source_file": "",
            "source_bytes": 0,
            "import_id": None,
            "error": "",
            "heartbeat_at": started,
            "updated_at": started,
            "criteria": {},
            "settings": {},
            "rows_used": 0,
            "rows_excluded": {},
            "result_count": 0,
            "result_summary": "",
            "spark_app_id": "",
            "resources": {},
        }
        self.row.update(fields)

    @property
    def run_id(self):
        return self.row["run_id"]

    def log(self, message):
        print(f"{self._prefix} {message}", flush=True)

    def update(self, **fields):
        with self._lock:
            self.row.update(fields)
            stamp = now()
            self.row["heartbeat_at"] = stamp
            self.row["updated_at"] = stamp
            values = [self.row[c] for c in RUN_COLUMNS]
            self._client.insert(RUNS_TABLE, [values], column_names=RUN_COLUMNS, database=self._db)

    @contextmanager
    def step(self, name):
        self.log(f"step: {name}")
        self.update(step=name)
        if self.on_step:
            self.on_step(name)
        start = time.monotonic()
        yield
        took = int((time.monotonic() - start) * 1000)
        self.update(step_durations={**self.row["step_durations"], name: took})
        self.log(f"step {name} took {took / 1000:.1f}s")

    def start(self):
        self.update()

        def beat():
            while not self._stop.wait(HEARTBEAT_SECONDS):
                try:
                    self.update()
                except Exception as exc:  # a missed heartbeat must not kill the job
                    self.log(f"heartbeat failed: {exc}")

        self._thread = threading.Thread(target=beat, daemon=True)
        self._thread.start()

    def finish(self, status, error=""):
        self._stop.set()
        if self._thread:
            self._thread.join()
        finished = now()
        duration = int((finished - self.row["started_at"]).total_seconds() * 1000)
        self.update(status=status, step="done" if status == "succeeded" else self.row["step"],
                    error=error, finished_at=finished, duration_ms=duration)


def table_exists(client, database, table):
    return int(client.command(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {t:String}",
        parameters={"db": database, "t": table},
    ))


def row_count(client, database, table):
    if not table_exists(client, database, table):
        return None
    return int(client.command(f"SELECT count() FROM {database}.{table}"))


def busy_run(client, db):
    """The run of this database currently holding the lock, or None (also
    None when the database has no run table)."""
    if not table_exists(client, db, RUNS_TABLE):
        return None
    result = client.query(
        f"SELECT * FROM {db}.{RUNS_TABLE} FINAL "
        "WHERE status = 'running' AND heartbeat_at > now64(3) - toIntervalSecond({stale:UInt32}) "
        "ORDER BY started_at DESC LIMIT 1",
        parameters={"stale": STALE_SECONDS},
    )
    return next(iter(result.named_results()), None)


def get_run(client, db, run_id):
    result = client.query(
        f"SELECT * FROM {db}.{RUNS_TABLE} FINAL WHERE run_id = {{run_id:UUID}}",
        parameters={"run_id": run_id},
    )
    return next(iter(result.named_results()), None)


def latest_run(client, db, run_type, status=None, import_id=None):
    where, params = ["run_type = {run_type:String}"], {"run_type": run_type}
    if status:
        where.append("status = {status:String}")
        params["status"] = status
    if import_id:
        where.append("import_id = {import_id:UUID}")
        params["import_id"] = str(import_id)
    result = client.query(
        f"SELECT * FROM {db}.{RUNS_TABLE} FINAL WHERE {' AND '.join(where)} ORDER BY started_at DESC LIMIT 1",
        parameters=params,
    )
    return next(iter(result.named_results()), None)


# --- Settings (<db>.settings). Values are strings. --------------------------

def load_settings(client, db, name):
    """Stored values of one row (current settings or a preset), or None."""
    result = client.query(
        f"SELECT setting_values FROM {db}.{SETTINGS_TABLE} FINAL WHERE name = {{name:String}}",
        parameters={"name": name},
    )
    rows = result.result_rows
    return dict(rows[0][0]) if rows else None


def save_settings(client, db, name, values, deleted=False):
    client.insert(
        SETTINGS_TABLE,
        [[name, {k: str(v) for k, v in values.items()}, int(deleted), now()]],
        column_names=["name", "setting_values", "is_deleted", "updated_at"],
        database=db,
    )


def preset_names(client, db):
    result = client.query(
        f"SELECT name FROM {db}.{SETTINGS_TABLE} FINAL WHERE name != {{current:String}} ORDER BY name",
        parameters={"current": CURRENT},
    )
    return [row[0] for row in result.result_rows]


def cluster_info(master_ui="http://spark-master:8080/json/"):
    """Worker memory and cores from the Spark master's JSON page, or None if
    the master can't be reached (the cluster checks are then skipped)."""
    try:
        with urllib.request.urlopen(master_ui, timeout=3) as response:
            data = json.load(response)
    except (OSError, ValueError):
        return None
    workers = [w for w in data.get("workers", []) if w.get("state") == "ALIVE"]
    if not workers:
        return None
    return {
        "workers": len(workers),
        "worker_memory_mb": max(w["memory"] for w in workers),
        "worker_cores": max(w["cores"] for w in workers),
    }


class Stopped(BaseException):
    # BaseException, like KeyboardInterrupt: py4j wraps any Exception raised
    # during a call to the JVM into a Py4JError.
    pass


STOP = {"requested": False}


def stop_on_sigterm():
    """Turn a Stop (SIGTERM from job_launch.stop) into an exception, so the job
    records the run as stopped and shuts Spark down cleanly."""

    def handler(signum, frame):
        if not STOP["requested"]:
            STOP["requested"] = True
            raise Stopped("Stopped by the user")

    signal.signal(signal.SIGTERM, handler)


def failure_message(exc):
    if STOP["requested"]:
        return "Stopped by the user"
    return f"{type(exc).__name__}: {exc}"
