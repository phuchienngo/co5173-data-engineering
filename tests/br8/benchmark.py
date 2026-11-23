"""BR8 benchmark driver (docs/plan/br7/plan-benchmark.md). Run from the host
with the stack up (`make br8-up`) and the data imported (`make br8-import`):

    python3 tests/br8/benchmark.py presets          save the benchmark presets into br8.settings
    python3 tests/br8/benchmark.py block S2         a Spark block (restarts the worker if needed)
    python3 tests/br8/benchmark.py block H2         a ClickHouse block (restarts ClickHouse if needed)
    python3 tests/br8/benchmark.py check S2         the plan's checks on a block's stored runs
    python3 tests/br8/benchmark.py screens S2       Grafana and Spark history screenshots (Playwright)
    python3 tests/br8/benchmark.py restore          worker back to 2 cores, ClickHouse to 1 CPU

`--cells I1-L2,I3-L3` limits a block, its checks or its screenshots. Runs go
through scripts/br8.py run, tagged with their block and cell (br8.run_history
has them for Grafana). Each run is also appended to
docs/result/br8/benchmark/runs.csv. Structure taken from tests/br6/benchmark.py;
the generic parts (restarts, size checks, Playwright helpers) come from BR5's script.
"""

import argparse
import csv
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "jobs" / "br8_tourist_retention"))
sys.path.insert(0, str(REPO / "jobs" / "common"))
import br8_bench_settings  # noqa: E402

_spec = importlib.util.spec_from_file_location("br5_benchmark", REPO / "tests" / "br5" / "benchmark.py")
br5 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(br5)
clickhouse = br5.clickhouse

CATALOG = br8_bench_settings.CATALOG
OUT = REPO / "docs" / "result" / "br8" / "benchmark"
MANIFEST = OUT / "runs.csv"
FIELDS = ["block", "cell", "input", "live", "preset", "repeat", "run_id", "status", "duration_s",
          "started_at", "finished_at", "spark_app_id", "rows_read", "visits", "places", "fingerprint"]
GRAFANA = br5.GRAFANA

# 3.1 inputs: destination region (ALL = every destination)
INPUTS = {"I1": "ALL", "I2": "Q1490", "I3": "Q1865"}
BLOCK_INPUTS = {"V": ["I1", "I2"]}
# 3.2 start-up configurations: Spark worker cores; V runs on S2 (3.4)
BLOCKS = {"S1": 1, "S2": 2, "S3": 4, "V": 2}
# 3.5 ClickHouse blocks: the clickhouse container's CPUs
CLICKHOUSE_BLOCKS = {"H1": 1, "H2": 2, "H3": 4}
ALL_CORES = {"spark.cores.max": "4"}  # a run takes whatever the worker has
CH = {"pipeline.engine": "clickhouse"}
LIVE = {  # (preset name, built-in preset to start from, overrides)
    "L1": ("bench-baseline", "baseline", ALL_CORES),
    "L2": ("bench-default", None, ALL_CORES),
    "L3": ("bench-read-4", None, {**ALL_CORES, "pipeline.read_partitions": "4"}),
    "V1": ("bench-python-udf", None, {**ALL_CORES, "pipeline.udf": "python"}),
    "V2": ("bench-pandas-udf", None, {**ALL_CORES, "pipeline.udf": "pandas"}),
    "V3": ("bench-no-cache", None, {**ALL_CORES, "pipeline.cache": "false"}),
    "V4": ("bench-no-broadcast", None, {**ALL_CORES, "spark.sql.autoBroadcastJoinThreshold": "-1"}),
    "V5": ("bench-no-aqe", None, {**ALL_CORES, "spark.sql.adaptive.enabled": "false"}),
    "V6": ("bench-pandas-writer", None, {**ALL_CORES, "pipeline.writer": "pandas"}),
    "HL1": ("bench-clickhouse", None, CH),
    "HL2": ("bench-clickhouse-nomemlimit", None, {**CH, "clickhouse.limit_threads_by_memory": "false"}),
}
SPARK_LIVE = ["L1", "L2", "L3"]
CLICKHOUSE_LIVE = ["HL1", "HL2"]
BLOCK_LIVE = {"V": ["L2", "V1", "V2", "V3", "V4", "V5", "V6"], **{b: CLICKHOUSE_LIVE for b in CLICKHOUSE_BLOCKS}}
BLOCK_REPEATS = {"V": 3, **{b: 5 for b in CLICKHOUSE_BLOCKS}}  # plan D8
THIRD_RUN_SPREAD = 1.15


def block_cells(block, cells=None):
    found = [f"{i}-{l}" for i in BLOCK_INPUTS.get(block, INPUTS) for l in BLOCK_LIVE.get(block, SPARK_LIVE)]
    unknown = [c for c in (cells or []) if c not in found]
    if unknown:
        sys.exit(f"Block {block} has no cells {unknown}; its cells: {found}")
    return [c for c in found if not cells or c in cells]


# --- Presets and runs -------------------------------------------------------

def preset_values(live):
    name, base, overrides = LIVE[live]
    values = CATALOG.resolve(preset=base)
    values.update(overrides)
    return name, values


def save_presets():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rows = []
    for live in LIVE:
        name, values = preset_values(live)
        errors, _ = CATALOG.validate(values, {"worker_memory_mb": 2048, "worker_cores": 2})
        if errors:
            sys.exit(f"{name}: {errors}")
        rows.append(json.dumps({"name": name, "setting_values": values, "is_deleted": 0, "updated_at": now}))
        print(f"{name}: {dict((k, values[k]) for k in CATALOG.changed(values))}")
    clickhouse("INSERT INTO br8.settings (name, setting_values, is_deleted, updated_at) FORMAT JSONEachRow",
               "\n".join(rows))


def run_row(run_id):
    rows = clickhouse(
        "SELECT toString(run_id) AS run_id, status, duration_s, toString(started_at) AS started_at, "
        "toString(finished_at) AS finished_at, spark_app_id, rows_read, visits, places, fingerprint "
        f"FROM br8.run_history WHERE run_id = '{run_id}' FORMAT JSONEachRow")
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
    """The block's sizes for the run's environment: scripts/br8.py run calls
    `docker compose up`, which would otherwise reset them (BR6's lesson)."""
    cores = BLOCKS.get(block, BLOCKS["S2"])
    cpus = CLICKHOUSE_BLOCKS.get(block, CLICKHOUSE_BLOCKS["H1"])
    return {**os.environ, "BR5_WORKER_CORES": str(cores), "BR5_CLICKHOUSE_CPUS": str(cpus)}


def run_cell(block, cell, repeat):
    input_id, live = cell.split("-")
    preset = LIVE[live][0]
    print(f"\n=== {block}-{cell} run {repeat}: region {INPUTS[input_id]}, preset {preset}", flush=True)
    cmd = [sys.executable, "scripts/br8.py", "run", "--input", input_id, "--preset", preset,
           "--tag", f"bench_block={block}", "--tag", f"bench_cell={cell}", "--tag", f"bench_repeat={repeat}"]
    proc = subprocess.Popen(cmd, cwd=REPO, env=block_env(block), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", errors="replace")
    run_id = None
    for line in proc.stdout:
        print("   ", line, end="", flush=True)
        found = re.search(r"\[br8\] run ([0-9a-f-]{36})", line)
        run_id = run_id or (found and found.group(1))
    code = proc.wait()
    if not run_id:
        sys.exit(f"The run didn't start (exit code {code}).")
    found = run_row(run_id)
    if found is None:
        sys.exit(f"Run {run_id} is not in br8.run_history.")
    row = {"block": block, "cell": cell, "input": input_id, "live": live, "preset": preset, "repeat": repeat,
           **{k: found[k] for k in FIELDS if k in found}}
    append_manifest(row)
    print(f"    -> {row['status']}, {float(row['duration_s'] or 0):.2f} s, run {run_id}")
    return row


def other_spark_apps():
    """Names of applications running on the Spark master (a stream demo, another BR)."""
    with urllib.request.urlopen(br5.SPARK_MASTER, timeout=5) as response:
        return [a.get("name", "?") for a in json.load(response).get("activeapps", [])]


def busy_runs():
    """Running batch runs of any BR whose run table exists (BR5's helper reads br5 only)."""
    dbs = [r["database"] for r in clickhouse(
        "SELECT database FROM system.tables WHERE name = 'pipeline_runs' AND database IN ('br5','br6','br7','br8') "
        "FORMAT JSONEachRow")]
    found = []
    for db in dbs:
        found += [f"{db} {r['run_id']}" for r in clickhouse(
            f"SELECT toString(run_id) AS run_id FROM {db}.pipeline_runs FINAL WHERE status = 'running' "
            "AND heartbeat_at > now64(3) - toIntervalSecond(30) FORMAT JSONEachRow")]
    return found


def set_clickhouse(cpus):
    """BR5's set_clickhouse, with the busy check over every batch BR."""
    if br5.clickhouse_size() == (cpus, f"auto({cpus})"):
        print(f"ClickHouse already has {cpus} CPUs (max_threads auto({cpus})).")
        return
    busy = busy_runs()
    if busy:
        sys.exit(f"Runs in progress ({busy}); not restarting ClickHouse.")
    print(f"Restarting ClickHouse with {cpus} CPUs ...")
    env = {**os.environ, "BR5_CLICKHOUSE_CPUS": str(cpus)}
    subprocess.run(["docker", "compose", "--profile", "full", "up", "-d", "--no-deps", "clickhouse"],
                   cwd=REPO, env=env, check=True)
    for _ in range(60):
        time.sleep(2)
        if br5.clickhouse_size()[1] is not None:
            break
    else:
        sys.exit("ClickHouse doesn't answer after 2 minutes.")
    br5.check_clickhouse(cpus)


def check_size(block):
    if block in CLICKHOUSE_BLOCKS:
        br5.check_clickhouse(CLICKHOUSE_BLOCKS[block])
    elif br5.worker_cores() != BLOCKS[block]:
        sys.exit(f"The Spark worker has {br5.worker_cores()} cores; expected {BLOCKS[block]}.")


def cmd_block(args):
    cells = block_cells(args.block, args.cells)
    repeats = args.repeats or BLOCK_REPEATS.get(args.block, 2)
    others = other_spark_apps()
    if others:
        sys.exit(f"Other Spark applications are running ({others}); stop them (make down / Ctrl+C) first.")
    if args.block in CLICKHOUSE_BLOCKS:
        set_clickhouse(CLICKHOUSE_BLOCKS[args.block])
    else:
        br5.set_worker(BLOCKS[args.block])
    if not args.no_warmup:
        run_cell(args.block, cells[0], "warmup")
    check_size(args.block)
    measured = {cell: [] for cell in cells}
    for repeat in range(1, repeats + 1):  # interleaved rounds
        for cell in cells:
            measured[cell].append(run_cell(args.block, cell, repeat))
    for cell, rows in measured.items() if repeats < 3 else ():
        times = [float(r["duration_s"]) for r in rows if r["status"] == "succeeded"]
        if len(times) >= 2 and max(times) / min(times) > THIRD_RUN_SPREAD:
            print(f"{cell}: runs differ by more than {THIRD_RUN_SPREAD - 1:.0%}; a third run.")
            run_cell(args.block, cell, repeats + 1)
    check_size(args.block)
    print("\nBlock done.")
    for row in [r for r in read_manifest() if r["block"] == args.block]:
        print(f"  {row['cell']:7} {row['repeat']:7} {row['status']:9} {float(row['duration_s'] or 0):7.2f} s  "
              f"{row['run_id']}")


# --- Checks -----------------------------------------------------------------

# Check-ins read and result fingerprint per input (br8_bench_runs.FINGERPRINT_SQL),
# measured with the ClickHouse engine on the real TripWorld files and checked row
# by row against an independent pandas version of prepare_br8.py build()
# (I1: 107,912 visits, 10,639 places, the same figures as prepare on all data).
EXPECTED = {
    "I1": (2_764_448, "visits 107912:7797127441102435416 · places 10639:6599842764725940206"),
    "I2": (164_969, "visits 5416:15693983204303305139 · places 533:4136802300216255460"),
    "I3": (27_544, "visits 468:14393461901935006069 · places 51:3928297759179194327"),
}

def measured_runs(block, cells=None):
    return [r for r in read_manifest() if r["block"] == block and r["repeat"] != "warmup"
            and (not cells or r["cell"] in cells)]


def reference(input_id):
    """Manifest row of the input's first succeeded S2 L2 run, else of its first succeeded run."""
    rows = [r for r in read_manifest() if r["input"] == input_id and r["status"] == "succeeded"
            and r["repeat"] != "warmup"]
    ref = [r for r in rows if r["block"] == "S2" and r["live"] == "L2"] or rows
    return ref[0] if ref else None


def query_threads(run_ids):
    ids = ",".join(f"'{r}'" for r in run_ids)
    rows = clickhouse(f"""
        SELECT log_comment AS run_id, count() AS queries, max(peak_threads_usage) AS threads,
               countIf(Settings['max_threads_min_free_memory_per_thread'] = '0') AS no_limit,
               countIf(query_cache_usage NOT IN ('None', 'Unknown')) AS cached
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment IN ({ids}) AND query_kind IN ('Select', 'Insert')
        GROUP BY log_comment FORMAT JSONEachRow""")
    return {r["run_id"]: (int(r["queries"]), int(r["threads"]), int(r["no_limit"]), int(r["cached"])) for r in rows}


def check_row(row, snap, block, threads):
    """Problems of one measured run (empty list = OK)."""
    problems = []
    if row["status"] != "succeeded":
        return [f"status {row['status']}"]
    if snap.get("settings_from") != f"preset {row['preset']}":
        problems.append(f"settings from {snap.get('settings_from')!r}")
    if block in CLICKHOUSE_BLOCKS:
        cpus = CLICKHOUSE_BLOCKS[block]
        if snap.get("ch_threads") != f"auto({cpus})":
            problems.append(f"ClickHouse max_threads {snap.get('ch_threads')!r}")
        if snap.get("engine") != "clickhouse":
            problems.append(f"engine {snap.get('engine')!r}")
    else:
        if snap.get("worker_cores") != str(BLOCKS[block]):
            problems.append(f"worker cores {snap.get('worker_cores')!r}")
        if snap.get("engine") != "spark":
            problems.append(f"engine {snap.get('engine')!r}")
    read, fingerprint = EXPECTED[row["input"]]
    if int(row["rows_read"]) != read:
        problems.append(f"read {row['rows_read']} rows, expected {read}")
    if row["fingerprint"] != fingerprint:
        problems.append(f"different result: {row['fingerprint']}, expected {fingerprint}")
    ref = reference(row["input"])
    if ref and ref["run_id"] != row["run_id"] and row["fingerprint"] != ref["fingerprint"]:
        problems.append(f"differs from reference {ref['block']}-{ref['cell']} {ref['run_id']}")
    if row["run_id"] in threads:
        queries, _, no_limit, cached = threads[row["run_id"]]
        if cached:
            problems.append(f"{cached} queries used the query cache")
        if (row["live"] == "HL2") != (no_limit == queries):
            problems.append(f"memory setting at 0 in {no_limit} of {queries} queries")
    return problems


def cmd_check(args):
    rows = measured_runs(args.block, args.cells)
    if not rows:
        sys.exit(f"No measured runs of block {args.block} in {MANIFEST.relative_to(REPO)}.")
    ids = ",".join(f"'{r['run_id']}'" for r in rows)
    snaps = {r["run_id"]: r for r in clickhouse(
        "SELECT toString(run_id) AS run_id, settings_from, worker_cores, clickhouse_threads AS ch_threads, engine "
        f"FROM br8.run_history WHERE run_id IN ({ids}) FORMAT JSONEachRow")}
    clickhouse("SYSTEM FLUSH LOGS")
    threads = query_threads([r["run_id"] for r in rows]) if args.block in CLICKHOUSE_BLOCKS else {}
    failures = 0
    for row in rows:
        problems = check_row(row, snaps.get(row["run_id"], {}), args.block, threads)
        note = ""
        if row["run_id"] in threads:
            queries, peak, no_limit, _ = threads[row["run_id"]]
            note = f"  {queries} queries, peak threads {peak}, memory limit off in {no_limit}"
        failures += bool(problems)
        print(f"{row['cell']:7} {row['repeat']:2} {row['run_id']}  {'OK' if not problems else '; '.join(problems)}"
              + note)
    print(f"\n{len(rows)} runs checked, {failures} with problems.")
    if failures:
        sys.exit(1)


# --- Screenshots ------------------------------------------------------------

def first_runs(block, cells=None):
    found = {}
    for row in measured_runs(block, cells):
        if row["status"] == "succeeded" and row["cell"] not in found:
            found[row["cell"]] = row
    return found


def reference_run(block, cell):
    """(run id, label) the cell is compared with in run.jpg, or (None, None)."""
    input_id, live = cell.split("-")
    if block in CLICKHOUSE_BLOCKS:
        ref = (block, f"{input_id}-HL1") if live == "HL2" else (
            ("H1", f"{input_id}-HL1") if block != "H1" else ("S2", f"{input_id}-L2"))
    elif block == "V":
        if live == "L2":
            return None, None
        ref = ("V", f"{input_id}-L2")
    elif live in ("L1", "L3"):
        ref = (block, f"{input_id}-L2")
    elif block != "S2":
        ref = ("S2", f"{input_id}-L2")
    else:
        return None, None
    found = first_runs(ref[0], [ref[1]]).get(ref[1])
    return (found["run_id"], f"{ref[0]}-{ref[1]}") if found else (None, f"{ref[0]}-{ref[1]} (not run yet)")


def epoch_ms(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def run_shot(page, row, ref_id, folder):
    """Grafana BR8 benchmark run page: result, compare with the reference, resources."""
    url = (f"{GRAFANA}/d/br8-benchmark-run?orgId=1&var-run={row['run_id']}&var-ref={ref_id or row['run_id']}"
           f"&from={epoch_ms(row['started_at']) - 2000}&to={epoch_ms(row['finished_at']) + 2000}&kiosk")
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(url)
    page.wait_for_load_state("networkidle")
    br5.fit_height(page)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)
    br5.shoot(page, folder / "run.jpg")


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
            ref_id, ref_label = reference_run(args.block, cell)
            print(f"{args.block}-{cell}: run {row['run_id']}" + (f", compared with {ref_label}" if ref_label else ""))
            run_shot(page, row, ref_id, folder)
            if args.block not in CLICKHOUSE_BLOCKS:
                br5.spark_shot(page, row, folder)
        browser.close()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="BR8 benchmark driver (docs/plan/br7/plan-benchmark.md)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("presets")
    sub.add_parser("restore")
    for name in ("block", "screens", "check"):
        p = sub.add_parser(name)
        p.add_argument("block", choices=[*BLOCKS, *CLICKHOUSE_BLOCKS])
        p.add_argument("--cells", type=lambda s: s.split(","), help="For example I1-L2,I3-L3")
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
    elif args.command == "check":
        cmd_check(args)
    else:
        cmd_screens(args)


if __name__ == "__main__":
    main()
