"""BR6 benchmark driver (docs/plan/br6/plan-benchmark.md). Run from the host
with the stack up (`make br6-up`) and the data imported at x1:

    python tests/br6/benchmark.py presets              save the benchmark presets
    python tests/br6/benchmark.py block S2             run a Spark block's cells (restarts the worker if needed)
    python tests/br6/benchmark.py block H2             ClickHouse block (restarts ClickHouse if needed)
    python tests/br6/benchmark.py screens H1           screenshots of a block's cells
    python tests/br6/benchmark.py check H1             the plan's checks on a block's stored runs
    python tests/br6/benchmark.py restore              worker back to 2 cores, ClickHouse to 1 CPU

`--cells I1-HL1,I3-HL4` limits a block, its screenshots or its checks to some
cells. Runs go through scripts/br6.py run, the same launcher as the app. Each
run is appended to docs/result/br6/benchmark/runs.csv; screenshots go to
docs/result/br6/benchmark/<block>-<cell>/. The parts that don't depend on the
BR (restarts, Playwright helpers) come from BR5's script (plan D11).
"""

import argparse
import csv
import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "jobs" / "br6_route"))
sys.path.insert(0, str(REPO / "jobs" / "common"))
import route_settings  # noqa: E402

# BR5's script, loaded under another name: this file has the same module name.
_spec = importlib.util.spec_from_file_location("br5_benchmark", REPO / "tests" / "br5" / "benchmark.py")
br5 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(br5)
clickhouse = br5.clickhouse

CATALOG = route_settings.CATALOG
OUT = REPO / "docs" / "result" / "br6" / "benchmark"
MANIFEST = OUT / "runs.csv"
FIELDS = ["block", "cell", "input", "live", "preset", "repeat", "run_id", "status", "duration_s",
          "started_at", "finished_at", "spark_app_id", "rows_read", "found", "suggested"]
APP = br5.APP
GRAFANA = br5.GRAFANA

# 3.1 inputs: city, start, end, stops, visitor (None = General visitor); all place types
INPUTS = {
    "I1": ("istanbul", 2005, 100, "3982", 88647),
    "I2": ("jakarta", 1549, 1373, "", None),
    "I3": ("beijing", 40, 409, "", None),
}
BLOCK_INPUTS = {"V": ["I1", "I2"]}  # D6
# 3.2 start-up configurations: Spark worker cores; V runs on S2 (3.4)
BLOCKS = {"S1": 1, "S2": 2, "S3": 4, "V": 2}
# 3.5 ClickHouse blocks: the clickhouse container's CPUs
CLICKHOUSE_BLOCKS = {"H1": 1, "H2": 2, "H3": 4}
# 3.3-3.5 presets: (name, built-in preset to start from, overrides)
ALL_CORES = {"spark.cores.max": "4"}  # D2: a run takes whatever the worker has
STATS_RUN = {"pipeline.place_stats": "run"}
CH = {"pipeline.engine": "clickhouse"}
NO_MEM_LIMIT = {"clickhouse.limit_threads_by_memory": "false"}
LIVE = {
    "L1": ("bench-baseline", "baseline", ALL_CORES),
    "L2": ("bench-default", None, ALL_CORES),
    "L3": ("bench-stats-run", None, {**ALL_CORES, **STATS_RUN}),
    "V1": ("bench-match-all", None, {**ALL_CORES, "pipeline.matching": "all"}),
    "V2": ("bench-python-udf", None, {**ALL_CORES, "pipeline.distance": "python_udf"}),
    "V3": ("bench-pandas-udf", None, {**ALL_CORES, "pipeline.distance": "pandas_udf"}),
    "V4": ("bench-match-all-python-udf", None,
           {**ALL_CORES, "pipeline.matching": "all", "pipeline.distance": "python_udf"}),
    "V5": ("bench-stats-run-driver", None, {**ALL_CORES, **STATS_RUN, "pipeline.reader": "driver"}),
    "HL1": ("bench-clickhouse", None, CH),
    "HL2": ("bench-clickhouse-nomemlimit", None, {**CH, **NO_MEM_LIMIT}),
    "HL3": ("bench-clickhouse-stats-run", None, {**CH, **STATS_RUN}),
    "HL4": ("bench-clickhouse-stats-run-nomemlimit", None, {**CH, **STATS_RUN, **NO_MEM_LIMIT}),
}
CLICKHOUSE_LIVE = ["HL1", "HL2", "HL3", "HL4"]
BLOCK_LIVE = {"V": ["L2", "V1", "V2", "V3", "V4", "L3", "V5"], **{b: CLICKHOUSE_LIVE for b in CLICKHOUSE_BLOCKS}}
SPARK_LIVE = ["L1", "L2", "L3"]
BLOCK_REPEATS = {"V": 3, **{b: 5 for b in CLICKHOUSE_BLOCKS}}  # D5, D6, D8
THIRD_RUN_SPREAD = 1.15  # D5


def block_cells(block, cells=None):
    found = [f"{i}-{l}" for i in BLOCK_INPUTS.get(block, INPUTS) for l in BLOCK_LIVE.get(block, SPARK_LIVE)]
    return [c for c in found if not cells or c in cells]


# --- Presets and runs -------------------------------------------------------

def save_presets():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rows = []
    for name, base, overrides in LIVE.values():
        values = CATALOG.resolve(preset=base)
        values.update(overrides)
        errors, _ = CATALOG.validate(values, {"worker_memory_mb": 2048, "worker_cores": 2})
        if errors:
            sys.exit(f"{name}: {errors}")
        rows.append(json.dumps({"name": name, "setting_values": values, "is_deleted": 0, "updated_at": now}))
        changed = {k: v for k, v in values.items() if v != CATALOG.by_key[k].default}
        print(f"{name}: {changed}")
    clickhouse("INSERT INTO br6.settings (name, setting_values, is_deleted, updated_at) FORMAT JSONEachRow",
               "\n".join(rows))


def run_row(run_id):
    rows = clickhouse(
        "SELECT run_id, status, duration_s, started_at, finished_at, spark_app_id, rows_read, found, suggested "
        f"FROM br6.run_history WHERE run_id = '{run_id}' FORMAT JSONEachRow")
    return rows[0] if rows else None


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


def block_env(block):
    """The block's container sizes for the run's environment: scripts/br6.py
    run calls `docker compose up`, which would otherwise put the worker and
    ClickHouse back to their default sizes."""
    cores = BLOCKS.get(block, BLOCKS["S2"])
    cpus = CLICKHOUSE_BLOCKS.get(block, CLICKHOUSE_BLOCKS["H1"])
    return {**os.environ, "BR5_WORKER_CORES": str(cores), "BR5_CLICKHOUSE_CPUS": str(cpus)}


def run_cell(block, cell, repeat):
    input_id, live = cell.split("-")
    city, start, end, stops, visitor = INPUTS[input_id]
    preset = LIVE[live][0]
    print(f"\n=== {block}-{cell} run {repeat}: {city} {start} -> {end}, preset {preset}", flush=True)
    cmd = [sys.executable, "scripts/br6.py", "run", "--city", city, "--start", str(start), "--end", str(end),
           "--preset", preset]
    if stops:
        cmd += ["--stops", stops]
    if visitor is not None:
        cmd += ["--visitor", str(visitor)]
    proc = subprocess.Popen(cmd, cwd=REPO, env=block_env(block), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", errors="replace")
    run_id = None
    for line in proc.stdout:
        print("   ", line, end="", flush=True)
        found = re.search(r"\[br6\] run ([0-9a-f-]{36})", line)
        run_id = run_id or (found and found.group(1))
    code = proc.wait()
    if not run_id:
        sys.exit(f"The run didn't start (exit code {code}).")
    found = run_row(run_id)
    row = {"block": block, "cell": cell, "input": input_id, "live": live, "preset": preset,
           "repeat": repeat, **{k: found[k] for k in FIELDS if k in found}}
    append_manifest(row)
    print(f"    -> {row['status']}, {float(row['duration_s'] or 0):.2f} s, run {run_id}")
    return row


def set_clickhouse(cpus):
    busy = clickhouse("SELECT run_id FROM br6.pipeline_runs FINAL WHERE status = 'running' "
                      "AND heartbeat_at > now64(3) - toIntervalSecond(30) FORMAT JSONEachRow")
    if busy and br5.clickhouse_size() != (cpus, f"auto({cpus})"):
        sys.exit(f"A BR6 run is in progress ({busy[0]['run_id']}); not restarting ClickHouse.")
    br5.set_clickhouse(cpus)  # also refuses during a BR5 run


def check_size(block):
    if block in CLICKHOUSE_BLOCKS:
        br5.check_clickhouse(CLICKHOUSE_BLOCKS[block])
    elif br5.worker_cores() != BLOCKS[block]:
        sys.exit(f"The Spark worker has {br5.worker_cores()} cores; expected {BLOCKS[block]}.")


def cmd_block(args):
    cells = block_cells(args.block, args.cells)
    repeats = args.repeats or BLOCK_REPEATS.get(args.block, 2)
    if args.block in CLICKHOUSE_BLOCKS:
        set_clickhouse(CLICKHOUSE_BLOCKS[args.block])
    else:
        br5.set_worker(BLOCKS[args.block])
    if not args.no_warmup:
        run_cell(args.block, cells[0], "warmup")
    check_size(args.block)  # the run's launcher must not have resized the containers
    measured = {cell: [] for cell in cells}
    for repeat in range(1, repeats + 1):  # D5: interleaved
        for cell in cells:
            measured[cell].append(run_cell(args.block, cell, repeat))
    for cell, rows in measured.items() if repeats < 3 else ():  # D5: a third run when two disagree
        times = [float(r["duration_s"]) for r in rows if r["status"] == "succeeded"]
        if len(times) >= 2 and max(times) / min(times) > THIRD_RUN_SPREAD:
            print(f"{cell}: runs differ by more than {THIRD_RUN_SPREAD - 1:.0%}; a third run.")
            run_cell(args.block, cell, repeats + 1)
    check_size(args.block)  # still the same size at the end
    print("\nBlock done.")
    for row in [r for r in read_manifest() if r["block"] == args.block]:
        print(f"  {row['cell']:7} {row['repeat']:7} {row['status']:9} {float(row['duration_s'] or 0):6.2f} s  "
              f"{row['run_id']}")


# --- Checks (plan section 8) ------------------------------------------------

EXPECTED = {"I1": (544471, 4574), "I2": (412100, 7178), "I3": (1470, 61)}  # check-ins read, places found


def measured_runs(block, cells=None):
    return [r for r in read_manifest() if r["block"] == block and r["repeat"] != "warmup"
            and (not cells or r["cell"] in cells)]


def reference_list(input_id):
    """Run id of the input's first succeeded S2 L2 run, else of any succeeded
    run of the input (the "same result" reference, check 4)."""
    rows = [r for r in read_manifest() if r["input"] == input_id and r["status"] == "succeeded"]
    ref = [r for r in rows if r["block"] == "S2" and r["live"] == "L2"] or rows
    return ref[0]["run_id"] if ref else None


def same_result(a, b):
    """route_compare.py on two stored runs, inside br5-app (it needs the
    ClickHouse client)."""
    code = ("import sys, json; sys.path[:0] = ['/opt/jobs/br6_route', '/opt/jobs/common'];"
            "import route_compare, route_runs; from clickhouse_io import get_client;"
            "c = get_client(); print(json.dumps(route_compare.differences(c, sys.argv[1], sys.argv[2])))")
    out = subprocess.run(["docker", "exec", "br5-app", "python3", "-c", code, a, b],
                         capture_output=True, text=True, encoding="utf-8")
    if out.returncode:
        return f"compare failed: {out.stderr.strip()[-300:]}"
    return json.loads(out.stdout.strip().splitlines()[-1])


def query_threads(run_ids):
    """{run_id: (queries, max peak threads, queries with the memory setting at 0, queries from the cache)}."""
    ids = ",".join(f"'{r}'" for r in run_ids)
    rows = clickhouse(f"""
        SELECT log_comment AS run_id, count() AS queries, max(peak_threads_usage) AS threads,
               countIf(Settings['max_threads_min_free_memory_per_thread'] = '0') AS no_limit,
               countIf(query_cache_usage NOT IN ('None', 'Unknown')) AS cached
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment IN ({ids}) AND query_kind = 'Select'
        GROUP BY log_comment FORMAT JSONEachRow""")
    return {r["run_id"]: (int(r["queries"]), int(r["threads"]), int(r["no_limit"]), int(r["cached"])) for r in rows}


def cmd_check(args):
    rows = measured_runs(args.block, args.cells)
    if not rows:
        sys.exit(f"No measured runs of block {args.block} in {MANIFEST.relative_to(REPO)}.")
    ids = ",".join(f"'{r['run_id']}'" for r in rows)
    snaps = {r["run_id"]: r for r in clickhouse(
        "SELECT toString(run_id) AS run_id, settings['launch.settings_from'] AS settings_from, "
        "settings['cluster.worker_cores'] AS worker_cores, settings['cluster.clickhouse_max_threads'] AS ch_threads "
        f"FROM br6.pipeline_runs FINAL WHERE run_id IN ({ids}) FORMAT JSONEachRow")}
    clickhouse("SYSTEM FLUSH LOGS")
    threads = query_threads([r["run_id"] for r in rows]) if args.block in CLICKHOUSE_BLOCKS else {}
    failures = 0
    for row in rows:
        problems = []
        snap = snaps.get(row["run_id"], {})
        if row["status"] != "succeeded":
            problems.append(f"status {row['status']}")
        if snap.get("settings_from") != f"preset {row['preset']}":
            problems.append(f"settings from {snap.get('settings_from')!r}")
        if args.block in CLICKHOUSE_BLOCKS:
            cpus = CLICKHOUSE_BLOCKS[args.block]
            if snap.get("ch_threads") != f"auto({cpus})":
                problems.append(f"ClickHouse max_threads {snap.get('ch_threads')!r}")
        elif snap.get("worker_cores") != str(BLOCKS[args.block]):
            problems.append(f"worker cores {snap.get('worker_cores')!r}")
        read, found = EXPECTED[row["input"]]
        if row["status"] == "succeeded" and (int(row["rows_read"]) != read or int(row["found"]) != found):
            problems.append(f"read {row['rows_read']} / found {row['found']}, expected {read} / {found}")
        ref = reference_list(row["input"])
        if row["status"] == "succeeded" and ref and ref != row["run_id"]:
            diff = same_result(ref, row["run_id"])
            if diff:
                problems.append(f"differs from {ref}: {str(diff)[:200]}")
        note = ""
        if row["run_id"] in threads:
            queries, peak, no_limit, cached = threads[row["run_id"]]
            note = f"  {queries} queries, peak threads {peak}, memory limit off in {no_limit}"
            if cached:
                problems.append(f"{cached} queries used the query cache")
            if (row["live"] in ("HL2", "HL4")) != (no_limit == queries):
                problems.append(f"memory setting at 0 in {no_limit} of {queries} queries")
        failures += bool(problems)
        print(f"{row['cell']:7} {row['repeat']:2} {row['run_id']}  {'OK' if not problems else '; '.join(problems)}"
              + note)
    print(f"\n{len(rows)} runs checked, {failures} with problems.")


# --- Screenshots (plan section 6) -------------------------------------------

def first_runs(block, cells=None):
    """{cell: manifest row} of the first succeeded measured run of each cell."""
    found = {}
    for row in measured_runs(block, cells):
        if row["status"] == "succeeded" and row["cell"] not in found:
            found[row["cell"]] = row
    return found


def reference_run(block, cell):
    """(run id, label) of run A of compare.jpg (plan section 6), or (None, None)."""
    input_id, live = cell.split("-")
    if block in CLICKHOUSE_BLOCKS:
        if live in ("HL2", "HL3"):
            ref = (block, f"{input_id}-HL1")
        elif live == "HL4":
            ref = (block, f"{input_id}-HL3")
        elif block != "H1":
            ref = ("H1", f"{input_id}-HL1")
        else:
            ref = ("S2", f"{input_id}-L2")  # Spark against ClickHouse
    elif block == "V":
        ref = {"V1": "L2", "V2": "L2", "V3": "L2", "V4": "V2", "V5": "L3"}.get(live)
        if not ref:
            return None, None
        ref = ("V", f"{input_id}-{ref}")
    elif live in ("L1", "L3"):
        ref = (block, f"{input_id}-L2")
    elif block != "S2":
        ref = ("S2", f"{input_id}-L2")
    else:
        return None, None
    found = first_runs(ref[0], [ref[1]]).get(ref[1])
    return (found["run_id"], f"{ref[0]}-{ref[1]}") if found else (None, f"{ref[0]}-{ref[1]} (not run yet)")


def finder_shot(page, row, folder):
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(f"{APP}/route_finder?run={row['run_id']}")
    br5.streamlit_ready(page, "Showing an earlier run")
    page.get_by_text("Run summary (for the team)").click()
    page.wait_for_timeout(1500)
    br5.fit_height(page)
    br5.streamlit_ready(page, "Showing an earlier run")
    br5.shoot(page, folder / "finder.jpg")


def trim(path, margin=80):
    """Cuts the empty page below the content. Run History grows the viewport
    to its long run list, so a compare shot can end in thousands of empty
    pixels. The sidebar (left 25%) is left out of the search."""
    from PIL import Image, ImageChops

    image = Image.open(path)
    width, height = image.size
    main = image.crop((width // 4, 0, width, height)).convert("L")
    background = Image.new("L", main.size, main.getpixel((main.width - 5, main.height - 5)))
    box = ImageChops.difference(main, background).point(lambda v: 255 if v > 24 else 0).getbbox()
    if box and box[3] + margin < height:
        image.crop((0, 0, width, box[3] + margin)).save(path, quality=80)
        print(f"    trimmed {path.relative_to(REPO)} from {height} to {box[3] + margin} px")


def compare_shot(page, row, ref_id, folder):
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(f"{APP}/route_run_history?a={ref_id}&run={row['run_id']}")
    br5.streamlit_ready(page, "Same result?")
    br5.fit_height(page)
    br5.streamlit_ready(page, "Same result?")
    br5.shoot(page, folder / "compare.jpg")
    trim(folder / "compare.jpg")


def resources_shot(page, row, folder):
    start = datetime.fromisoformat(row["started_at"]).replace(tzinfo=timezone.utc).timestamp()
    end = datetime.fromisoformat(row["finished_at"]).replace(tzinfo=timezone.utc).timestamp()
    url = (f"{GRAFANA}/d/br6-resource-use?orgId=1&from={int(start * 1000) - 2_000}"
           f"&to={int(end * 1000) + 2_000}&kiosk")
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(url)
    page.wait_for_load_state("networkidle")
    br5.fit_height(page)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)
    br5.shoot(page, folder / "resources.jpg")


def cmd_screens(args):
    from playwright.sync_api import sync_playwright

    runs = first_runs(args.block, args.cells)
    if not runs:
        sys.exit(f"No succeeded runs of block {args.block} in {MANIFEST.relative_to(REPO)}.")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, timezone_id="UTC")
        br5.grafana_login(page)
        for cell, row in runs.items():
            folder = OUT / f"{args.block}-{cell}"
            print(f"{args.block}-{cell}: run {row['run_id']}")
            finder_shot(page, row, folder)
            ref_id, ref_label = reference_run(args.block, cell)
            if ref_id:
                print(f"    compared with {ref_label}")
                compare_shot(page, row, ref_id, folder)
            elif ref_label:
                print(f"    no compare.jpg: reference {ref_label}")
            if args.block not in CLICKHOUSE_BLOCKS:
                resources_shot(page, row, folder)
                br5.spark_shot(page, row, folder)
        browser.close()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR6 benchmark driver (docs/plan/br6/plan-benchmark.md)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("presets")
    sub.add_parser("restore")
    sub.add_parser("trim", help="Trim the empty page below every saved compare.jpg")
    for name in ("block", "screens", "check"):
        p = sub.add_parser(name)
        p.add_argument("block", choices=[*BLOCKS, *CLICKHOUSE_BLOCKS])
        p.add_argument("--cells", type=lambda s: s.split(","), help="For example I1-HL1,I3-HL4")
        if name == "block":
            p.add_argument("--repeats", type=int, help="Default 2; 3 for block V, 5 for the H blocks")
            p.add_argument("--no-warmup", action="store_true")
    args = parser.parse_args()
    if args.command == "presets":
        save_presets()
    elif args.command == "restore":
        br5.set_worker(BLOCKS["S2"])
        set_clickhouse(CLICKHOUSE_BLOCKS["H1"])
    elif args.command == "block":
        cmd_block(args)
    elif args.command == "trim":
        for path in sorted(OUT.glob("*/compare.jpg")):
            trim(path)
    elif args.command == "check":
        cmd_check(args)
    else:
        cmd_screens(args)


if __name__ == "__main__":
    main()
