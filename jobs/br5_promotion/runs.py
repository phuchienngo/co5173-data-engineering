"""BR5 run history (br5.pipeline_runs), the busy lock, the data status and
the stored settings (br5.settings).

Shared by the import job, the status command and the app, so the CLI and the
app see the same state.
"""

import json
import threading
import time
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

DB = "br5"
STAGING_DB = "br5_staging"
RUNS_TABLE = "pipeline_runs"
# Replaced together by an import; dimensions first so the fact never points
# at a missing key. pipeline_runs is never part of this list.
DATA_TABLES = [
    "dim_date",
    "dim_hotel",
    "dim_country",
    "dim_room_type",
    "dim_channel",
    "dim_booking_profile",
    "raw_booking",
    "fact_booking",
]
HEARTBEAT_SECONDS = 10
# A running job whose heartbeat is older than this is treated as dead.
STALE_SECONDS = 30

RUN_COLUMNS = [
    "run_id",
    "run_type",
    "status",
    "step",
    "started_at",
    "finished_at",
    "duration_ms",
    "step_durations",
    "rows_read",
    "rows_expected",
    "rows_written",
    "multiplier",
    "writer",
    "source_file",
    "source_bytes",
    "import_id",
    "error",
    "heartbeat_at",
    "updated_at",
    "criteria",
    "settings",
    "rows_used",
    "rows_excluded",
    "result_count",
    "result_summary",
]


def now():
    return datetime.now(timezone.utc)


class Run:
    """One row of br5.pipeline_runs. Every change inserts a new version."""

    def __init__(self, client, run_type, run_id=None, **fields):
        self._client = client
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
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
        }
        self.row.update(fields)

    @property
    def run_id(self):
        return self.row["run_id"]

    def update(self, **fields):
        with self._lock:
            self.row.update(fields)
            stamp = now()
            self.row["heartbeat_at"] = stamp
            self.row["updated_at"] = stamp
            values = [self.row[c] for c in RUN_COLUMNS]
            self._client.insert(RUNS_TABLE, [values], column_names=RUN_COLUMNS, database=DB)

    @contextmanager
    def step(self, name):
        print(f"[br5] step: {name}", flush=True)
        self.update(step=name)
        start = time.monotonic()
        yield
        took = int((time.monotonic() - start) * 1000)
        durations = {**self.row["step_durations"], name: took}
        self.update(step_durations=durations)
        print(f"[br5] step {name} took {took / 1000:.1f}s", flush=True)

    def start(self):
        self.update()

        def beat():
            while not self._stop.wait(HEARTBEAT_SECONDS):
                try:
                    self.update()
                except Exception as exc:  # a missed heartbeat must not kill the job
                    print(f"[br5] heartbeat failed: {exc}", flush=True)

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


def busy_run(client):
    """The BR5 run currently holding the lock, or None."""
    result = client.query(
        f"SELECT * FROM {DB}.{RUNS_TABLE} FINAL "
        "WHERE status = 'running' AND heartbeat_at > now64(3) - toIntervalSecond({stale:UInt32}) "
        "ORDER BY started_at DESC LIMIT 1",
        parameters={"stale": STALE_SECONDS},
    )
    rows = result.named_results()
    return next(iter(rows), None)


def get_run(client, run_id):
    result = client.query(
        f"SELECT * FROM {DB}.{RUNS_TABLE} FINAL WHERE run_id = {{run_id:UUID}}",
        parameters={"run_id": run_id},
    )
    return next(iter(result.named_results()), None)


def table_exists(client, database, table):
    return int(client.command(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {t:String}",
        parameters={"db": database, "t": table},
    ))


def row_count(client, database, table):
    if not table_exists(client, database, table):
        return None
    return int(client.command(f"SELECT count() FROM {database}.{table}"))


def data_status(client):
    """What the Data screen and `make br5-status` show."""
    if not table_exists(client, DB, "fact_booking"):
        return {"ready": False, "reason": "BR5 tables are missing. Run `make br5-up`."}
    counts = {t: row_count(client, DB, t) for t in DATA_TABLES}
    status = {"ready": True, "loaded": bool(counts["fact_booking"]), "counts": counts, "import": None}
    if status["loaded"]:
        import_id = client.command(f"SELECT any(import_id) FROM {DB}.fact_booking")
        result = client.query(
            f"SELECT * FROM {DB}.{RUNS_TABLE} FINAL "
            "WHERE run_type = 'import' AND status = 'succeeded' AND import_id = {import_id:UUID} "
            "ORDER BY started_at DESC LIMIT 1",
            parameters={"import_id": str(import_id)},
        )
        status["import"] = next(iter(result.named_results()), None)
    result = client.query(
        f"SELECT * FROM {DB}.{RUNS_TABLE} FINAL WHERE run_type = 'import' ORDER BY started_at DESC LIMIT 1"
    )
    status["last_import_run"] = next(iter(result.named_results()), None)
    status["busy"] = busy_run(client)
    return status


def stored_import(client):
    """import_id and multiplier of the live fact table, or (None, 1) when empty."""
    row = client.query(
        f"SELECT any(import_id) AS import_id, max(copy_no) + 1 AS copies, count() AS rows FROM {DB}.fact_booking"
    ).first_item
    if not row["rows"]:
        return None, 1
    return str(row["import_id"]), int(row["copies"])


def latest_analysis(client, succeeded_only=True):
    status = "AND status = 'succeeded' " if succeeded_only else ""
    result = client.query(
        f"SELECT * FROM {DB}.{RUNS_TABLE} FINAL WHERE run_type = 'analysis' {status}"
        "ORDER BY started_at DESC LIMIT 1"
    )
    return next(iter(result.named_results()), None)


def result_rows(client, run_id):
    """(run_month rows, run_card rows) of one analysis run."""
    params = {"run_id": run_id}
    months = list(client.query(
        f"SELECT * FROM {DB}.run_month WHERE run_id = {{run_id:UUID}} ORDER BY target_rank, month",
        parameters=params,
    ).named_results())
    cards = list(client.query(
        f"SELECT * FROM {DB}.run_card WHERE run_id = {{run_id:UUID}} ORDER BY target_rank",
        parameters=params,
    ).named_results())
    return months, cards


# --- Settings (br5.settings). Values are strings; see run_settings.py. ---

SETTINGS_TABLE = "settings"


def load_settings(client, name):
    """Stored values of one row (current settings or a preset), or None."""
    result = client.query(
        f"SELECT setting_values FROM {DB}.{SETTINGS_TABLE} FINAL WHERE name = {{name:String}}",
        parameters={"name": name},
    )
    rows = result.result_rows
    return dict(rows[0][0]) if rows else None


def save_settings(client, name, values, deleted=False):
    client.insert(
        SETTINGS_TABLE,
        [[name, {k: str(v) for k, v in values.items()}, int(deleted), now()]],
        column_names=["name", "setting_values", "is_deleted", "updated_at"],
        database=DB,
    )


def preset_names(client, current):
    result = client.query(
        f"SELECT name FROM {DB}.{SETTINGS_TABLE} FINAL WHERE name != {{current:String}} ORDER BY name",
        parameters={"current": current},
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
    """Turn a Stop (SIGTERM from launch.stop) into an exception, so the job
    records the run as stopped and shuts Spark down cleanly."""
    import signal

    def handler(signum, frame):
        if not STOP["requested"]:
            STOP["requested"] = True
            raise Stopped("Stopped by the user")

    signal.signal(signal.SIGTERM, handler)


def failure_message(exc):
    if STOP["requested"]:
        return "Stopped by the user"
    return f"{type(exc).__name__}: {exc}"
