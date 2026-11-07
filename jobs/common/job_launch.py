"""Starts and stops a batch BR's Spark jobs from the shared app container.

Built from BR5's launch.py (jobs/br5_promotion), which keeps its own copy. The
Spark driver runs in the calling container (client mode), so its host name
must be reachable from the worker (SPARK_DRIVER_HOST).
"""

import os
import signal
import subprocess
import threading
from pathlib import Path

COMMON = Path(__file__).resolve().parent


def spark_submit(script, args, driver_memory, extra_conf=None):
    """spark-submit command for script; jobs/common and the script's sibling
    .py files are shipped as --py-files."""
    script = Path(script)
    py_files = sorted(COMMON.glob("*.py")) + sorted(p for p in script.parent.glob("*.py") if p != script)
    conf = {
        "spark.jars.ivy": "/tmp/.ivy2",
        "spark.driver.host": os.environ.get("SPARK_DRIVER_HOST", "br5-app"),
        **(extra_conf or {}),
    }
    cmd = [
        "spark-submit",
        "--master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        # The driver JVM starts before the job can apply settings, so its memory goes here.
        "--driver-memory", driver_memory,
    ]
    for key, value in conf.items():
        cmd += ["--conf", f"{key}={value}"]
    return [*cmd, "--py-files", ",".join(str(p) for p in py_files), str(script), *args]


def start(cmd, log, stream=False):
    """Starts cmd in its own process group. Returns the process.
    stream=True: the caller reads proc.stdout and writes the log itself."""
    log = Path(log)
    log.parent.mkdir(parents=True, exist_ok=True)
    if stream:
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                errors="replace", start_new_session=True)
    with log.open("w") as out:
        return subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)


def driver_pid(proc, scripts):
    """The job's Python driver that spark-submit's JVM started in proc's
    process group (scripts: the job script paths to look for)."""
    scripts = {str(path) for path in scripts}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            argv = Path(f"/proc/{entry}/cmdline").read_bytes().split(b"\0")
            if os.getpgid(int(entry)) != proc.pid:
                continue
        except OSError:
            continue
        args = [a.decode(errors="replace") for a in argv if a]
        if args and "python" in Path(args[0]).name and scripts & set(args):
            return int(entry)
    return None


def stop(proc, scripts, grace_seconds=20):
    """Stops a job: SIGTERM to its Python driver first, so it records the run
    as stopped and shuts Spark down cleanly; the whole process group is killed
    if it is still running after the grace period."""
    if proc is None or proc.poll() is not None:
        return
    pid = driver_pid(proc, scripts)
    if pid is None:
        os.killpg(proc.pid, signal.SIGTERM)
        return
    os.kill(pid, signal.SIGTERM)

    def escalate():
        try:
            proc.wait(grace_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)

    threading.Thread(target=escalate, daemon=True).start()
