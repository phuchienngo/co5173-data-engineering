"""BR5 benchmark driver (docs/plan/br5/plan-benchmark.md). Run from the host
with the stack up (`make br5-up`) and the data imported at x10:

    python tests/br5/benchmark.py presets                 save the benchmark presets
    python tests/br5/benchmark.py block S2 [--clickhouse] run a block's cells (restarts the worker if needed)
    python tests/br5/benchmark.py screens S2              screenshots of a block's cells
    python tests/br5/benchmark.py restore                 worker back to 2 cores

`--cells I3-L3,I1-L2` limits a block or its screenshots to some cells.
Runs go through scripts/br5.py run, the same launcher as the app. Each run is
appended to docs/result/br5/benchmark/runs.csv; screenshots go to
docs/result/br5/benchmark/<block>-<cell>/. Screenshots need Playwright for
Python (pip install playwright) and use the installed Chrome.
"""

import argparse
import base64
import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "jobs" / "br5_promotion"))
import run_settings  # noqa: E402

OUT = REPO / "docs" / "result" / "br5" / "benchmark"
MANIFEST = OUT / "runs.csv"
FIELDS = ["block", "cell", "input", "live", "preset", "repeat", "run_id", "status", "duration_s",
          "started_at", "finished_at", "spark_app_id", "rows_read"]

CLICKHOUSE = "http://localhost:8123"
CLICKHOUSE_AUTH = "Basic " + base64.b64encode(b"default:clickhouse").decode()
SPARK_MASTER = "http://localhost:8082/json/"
APP = "http://localhost:8501"
GRAFANA = "http://localhost:3000"
SPARK_HISTORY = "http://localhost:18080"

# 3.1 inputs: (hotel, country, room type)
INPUTS = {
    "I1": ("All", "All", "All"),
    "I2": ("City Hotel", "PRT", "All"),
    "I3": ("Resort Hotel", "GBR", "A"),
}
# 3.2 start-up configurations: Spark worker cores
BLOCKS = {"S1": 1, "S2": 2, "S3": 4}
# 3.3 / 3.4 presets: (name, built-in preset to start from, overrides)
ALL_CORES = {"spark.cores.max": "4"}  # D4: a run takes whatever the worker has
LIVE = {
    "L1": ("bench-baseline", "baseline", ALL_CORES),
    "L2": ("bench-default", None, ALL_CORES),
    "L3": ("bench-read4", None, {**ALL_CORES, "pipeline.read_partitions": "4"}),
    "C1": ("bench-clickhouse", None, {"pipeline.engine": "clickhouse"}),
}
SPARK_LIVE = ["L1", "L2", "L3"]
THIRD_RUN_SPREAD = 1.15  # D7


# --- ClickHouse and Spark master --------------------------------------------

def clickhouse(sql, data=None):
    url = CLICKHOUSE + "/?" + urllib.parse.urlencode({"query": sql} if data is not None else {})
    body = (data if data is not None else sql).encode()
    request = urllib.request.Request(url, data=body, headers={"Authorization": CLICKHOUSE_AUTH})
    with urllib.request.urlopen(request, timeout=30) as response:
        return [json.loads(line) for line in response.read().decode().splitlines() if line.strip()]


def run_row(run_id):
    rows = clickhouse(
        "SELECT run_id, status, duration_s, started_at, finished_at, spark_app_id, rows_read "
        f"FROM br5.run_history WHERE run_id = '{run_id}' FORMAT JSONEachRow")
    return rows[0] if rows else None


def worker_cores():
    try:
        with urllib.request.urlopen(SPARK_MASTER, timeout=5) as response:
            workers = [w for w in json.load(response)["workers"] if w["state"] == "ALIVE"]
    except (OSError, ValueError, KeyError):
        return None
    return workers[0]["cores"] if len(workers) == 1 else None


def set_worker(cores):
    """Recreates spark-worker with `cores` cores (BR5_WORKER_CORES in
    docker-compose.yml) and waits until the master sees it."""
    if worker_cores() == cores:
        print(f"Spark worker already has {cores} cores.")
        return
    print(f"Restarting the Spark worker with {cores} cores ...")
    env = {**os.environ, "BR5_WORKER_CORES": str(cores)}
    subprocess.run(["docker", "compose", "--profile", "full", "up", "-d", "--no-deps", "spark-worker"],
                   cwd=REPO, env=env, check=True)
    for _ in range(60):
        if worker_cores() == cores:
            print(f"Spark worker has {cores} cores.")
            return
        time.sleep(2)
    sys.exit(f"The Spark master doesn't show one worker with {cores} cores after 2 minutes.")


# --- Presets and runs -------------------------------------------------------

def save_presets():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rows = []
    for name, base, overrides in LIVE.values():
        values = run_settings.resolve(preset=base)
        values.update(overrides)
        rows.append(json.dumps({"name": name, "setting_values": values, "is_deleted": 0, "updated_at": now}))
        changed = {k: v for k, v in values.items() if v != run_settings.BY_KEY[k].default}
        print(f"{name}: {changed}")
    clickhouse("INSERT INTO br5.settings (name, setting_values, is_deleted, updated_at) FORMAT JSONEachRow",
               "\n".join(rows))


def read_manifest():
    if not MANIFEST.exists():
        return []
    with MANIFEST.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_manifest(row):
    OUT.mkdir(parents=True, exist_ok=True)
    new = not MANIFEST.exists()
    with MANIFEST.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        if new:
            writer.writeheader()
        writer.writerow(row)


def run_cell(block, cell, repeat):
    input_id, live = cell.split("-")
    hotel, country, room = INPUTS[input_id]
    preset = LIVE[live][0]
    print(f"\n=== {block}-{cell} run {repeat}: {hotel} / {country} / {room}, preset {preset}", flush=True)
    cmd = [sys.executable, "scripts/br5.py", "run", "--hotel", hotel, "--country", country,
           "--room-type", room, "--preset", preset]
    proc = subprocess.Popen(cmd, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace")
    run_id = None
    for line in proc.stdout:
        print("   ", line, end="", flush=True)
        found = re.search(r"\[br5\] analysis ([0-9a-f-]{36})", line)
        run_id = run_id or (found and found.group(1))
    code = proc.wait()
    if not run_id:
        sys.exit(f"The run didn't start (exit code {code}).")
    found = run_row(run_id)
    row = {"block": block, "cell": cell, "input": input_id, "live": live, "preset": preset,
           "repeat": repeat, **{k: found[k] for k in FIELDS if k in found}}
    append_manifest(row)
    print(f"    -> {row['status']}, {float(row['duration_s'] or 0):.1f} s, run {run_id}")
    return row


def cmd_block(args):
    cells = args.cells or [f"{i}-{l}" for i in INPUTS for l in SPARK_LIVE]
    set_worker(BLOCKS[args.block])
    if not args.no_warmup:
        run_cell(args.block, cells[0], "warmup")
    measured = {cell: [] for cell in cells}
    for repeat in range(1, args.repeats + 1):  # D8: interleaved
        for cell in cells:
            measured[cell].append(run_cell(args.block, cell, repeat))
    for cell, rows in measured.items():  # D7: a third run when two disagree
        times = [float(r["duration_s"]) for r in rows if r["status"] == "succeeded"]
        if len(times) >= 2 and max(times) / min(times) > THIRD_RUN_SPREAD:
            print(f"{cell}: runs differ by more than {THIRD_RUN_SPREAD - 1:.0%}; a third run.")
            run_cell(args.block, cell, args.repeats + 1)
    if args.clickhouse:
        for input_id in INPUTS:
            for repeat in range(1, 4):
                run_cell(args.block, f"{input_id}-C1", repeat)
    print("\nBlock done.")
    for row in [r for r in read_manifest() if r["block"] == args.block]:
        print(f"  {row['cell']:7} {row['repeat']:7} {row['status']:9} {float(row['duration_s'] or 0):6.1f} s  "
              f"{row['run_id']}")


# --- Screenshots (section 6) ------------------------------------------------

def first_runs(block, cells=None):
    """{cell: manifest row} of the first succeeded measured run of each cell."""
    found = {}
    for row in read_manifest():
        if row["block"] == block and row["repeat"] != "warmup" and row["status"] == "succeeded":
            if (not cells or row["cell"] in cells) and row["cell"] not in found:
                found[row["cell"]] = row
    return found


def reference_run(block, cell, row):
    """Run A of compare.jpg: L1/L3 against L2 of the same input and block, L2
    of S1/S3 and C1 against S2's L2. Falls back to another run of the same
    cell (a measured one) while the reference cell hasn't run yet. None for
    S2's L2."""
    input_id, live = cell.split("-")
    if live in ("L1", "L3"):
        ref = (block, f"{input_id}-L2")
    elif live == "C1" or block != "S2":
        ref = ("S2", f"{input_id}-L2")
    else:
        return None, None
    found = first_runs(ref[0], [ref[1]]).get(ref[1])
    if found:
        return found["run_id"], f"{ref[0]}-{ref[1]}"
    others = [r for r in read_manifest() if r["block"] == block and r["cell"] == cell
              and r["status"] == "succeeded" and r["run_id"] != row["run_id"] and r["repeat"] != "warmup"]
    if others:
        return others[0]["run_id"], f"{block}-{cell} run {others[0]['repeat']} (reference {ref[0]}-{ref[1]} not run yet)"
    return None, None


def fit_height(page, limit=8000):
    """Streamlit and Grafana scroll inside an element, so a full-page shot
    would only show the viewport: make the viewport as tall as the content."""
    height = page.evaluate("""() => Math.max(document.documentElement.scrollHeight,
        ...[...document.querySelectorAll('*')].filter(e => e.scrollHeight > e.clientHeight + 5
            && getComputedStyle(e).overflowY.match(/auto|scroll/)).map(e => e.scrollHeight + 120))""")
    page.set_viewport_size({"width": 1600, "height": min(int(height), limit)})
    page.wait_for_timeout(1500)


def streamlit_ready(page, text, timeout=60_000):
    page.get_by_text(text, exact=False).first.wait_for(timeout=timeout)
    # The status widget ("Running...") is gone once the script has finished.
    page.wait_for_function("() => !document.querySelector('[data-testid=\"stStatusWidget\"]')", timeout=timeout)
    page.wait_for_timeout(1500)


def shoot(page, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(path), type="jpeg", quality=80, full_page=True)
    print(f"    saved {path.relative_to(REPO)} ({path.stat().st_size // 1024} KB)")


def finder_shot(page, row, folder):
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(f"{APP}/?run={row['run_id']}")
    streamlit_ready(page, "Showing an earlier run")
    page.get_by_text("Run summary (for the team)").click()
    page.wait_for_timeout(1500)
    fit_height(page)
    streamlit_ready(page, "Showing an earlier run")
    shoot(page, folder / "finder.jpg")


def compare_shot(page, row, ref_id, folder):
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(f"{APP}/run_history?a={ref_id}&run={row['run_id']}")
    streamlit_ready(page, "Same result?")
    fit_height(page)
    streamlit_ready(page, "Same result?")
    shoot(page, folder / "compare.jpg")


def grafana_login(page):
    # Grafana's login API sets the session cookie in the browser context.
    response = page.request.post(f"{GRAFANA}/login", data={"user": "admin", "password": "admin"})
    if not response.ok:
        sys.exit(f"Grafana login failed: HTTP {response.status} {response.text()[:200]}")


def resources_shot(page, row, folder):
    start = datetime.fromisoformat(row["started_at"]).replace(tzinfo=timezone.utc).timestamp()
    end = datetime.fromisoformat(row["finished_at"]).replace(tzinfo=timezone.utc).timestamp()
    # A small margin only: runs follow each other within seconds, and the
    # "in the time range" panels should cover this run alone.
    url = (f"{GRAFANA}/d/br5-resource-use?orgId=1&from={int(start * 1000) - 2_000}"
           f"&to={int(end * 1000) + 2_000}&kiosk")
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(url)
    page.wait_for_load_state("networkidle")
    fit_height(page)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)
    shoot(page, folder / "resources.jpg")


def spark_shot(page, row, folder):
    if not row["spark_app_id"]:
        return
    page.set_viewport_size({"width": 1600, "height": 1000})
    url = f"{SPARK_HISTORY}/history/{row['spark_app_id']}/jobs/"
    for _ in range(12):  # the history server picks up a log within about 5 s
        page.goto(url)
        if page.get_by_text("Completed Jobs").count():
            break
        page.wait_for_timeout(5000)
    else:
        print(f"    no Spark history page for {row['spark_app_id']}")
        return
    shoot(page, folder / "spark.jpg")


def cmd_screens(args):
    from playwright.sync_api import sync_playwright

    runs = first_runs(args.block, args.cells)
    if not runs:
        sys.exit(f"No succeeded runs of block {args.block} in {MANIFEST.relative_to(REPO)}.")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, timezone_id="UTC")
        grafana_login(page)
        for cell, row in runs.items():
            folder = OUT / f"{args.block}-{cell}"
            print(f"{args.block}-{cell}: run {row['run_id']}")
            finder_shot(page, row, folder)
            ref_id, ref_label = reference_run(args.block, cell, row)
            if ref_id:
                print(f"    compared with {ref_label}")
                compare_shot(page, row, ref_id, folder)
            if not cell.endswith("C1"):
                resources_shot(page, row, folder)
                spark_shot(page, row, folder)
        browser.close()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR5 benchmark driver (docs/plan/br5/plan-benchmark.md)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("presets")
    sub.add_parser("restore")
    for name in ("block", "screens"):
        p = sub.add_parser(name)
        p.add_argument("block", choices=list(BLOCKS))
        p.add_argument("--cells", type=lambda s: s.split(","), help="For example I3-L3,I1-L2")
        if name == "block":
            p.add_argument("--repeats", type=int, default=2)
            p.add_argument("--no-warmup", action="store_true")
            p.add_argument("--clickhouse", action="store_true", help="Also the ClickHouse engine (C1) cells")
    args = parser.parse_args()
    if args.command == "presets":
        save_presets()
    elif args.command == "restore":
        set_worker(BLOCKS["S2"])
    elif args.command == "block":
        cmd_block(args)
    else:
        cmd_screens(args)


if __name__ == "__main__":
    main()
