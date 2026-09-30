"""Record job execution status in ClickHouse."""

from __future__ import annotations

import logging
import sys
import time
import traceback
import uuid
from contextlib import contextmanager

from common.clickhouse_io import ch_client
from common.config import settings

log = logging.getLogger(__name__)

_TABLE = "pipeline_log"


class _Run:
    """ run."""

    def __init__(self, run_id: str, job_name: str, br: str, step: str):
        self.run_id = run_id
        self.job_name = job_name
        self.br = br
        self.step = step
        self.rows = 0


def _write(run: _Run, status: str, duration: float, message: str = "") -> None:
    try:
        client = ch_client()
        try:
            client.insert(
                _TABLE,
                [[run.run_id, run.job_name, run.br, run.step,
                  status, int(run.rows), float(duration), message[:4000]]],
                column_names=["run_id", "job_name", "br", "step",
                              "status", "rows_written", "duration_sec", "message"],
                database=settings.ch_db,
            )
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001


        log.warning("Could not write pipeline_log (%s): %s", status, exc)


@contextmanager
def job_run(job_name: str, *, br: str = "COMMON", step: str = ""):
    run = _Run(str(uuid.uuid4()), job_name, br, step)
    started = time.perf_counter()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s │ %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )

    print("─" * 72)
    print(f"▶  {job_name}   [{br}]   {step}")
    print("─" * 72)
    _write(run, "STARTED", 0.0)

    try:
        yield run
    except Exception:
        elapsed = time.perf_counter() - started
        tb = traceback.format_exc()
        _write(run, "FAILED", elapsed, tb)
        print("─" * 72)
        print(f"✗  {job_name} FAILED after {elapsed:.1f}s")
        print("─" * 72)
        raise
    else:
        elapsed = time.perf_counter() - started
        _write(run, "SUCCESS", elapsed, "")
        print("─" * 72)
        print(f"✓  {job_name} completed in {elapsed:.1f}s · {run.rows:,} rows")
        print("─" * 72)
