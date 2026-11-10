"""BR1 benchmark: naive vs optimized tour solve, and Tour finder dashboard query latency.
  spark   - REPS runs of jobs/br1_tour_route/job.py --bench-only (no stream, tour fact tables untouched);
            timings come from tour.job_run, one run = one ran_at
  queries - every panel query of the Grafana dashboard br1-finder, N timed calls each

Usage: python3 tests/br1/benchmark.py [REPS] [--queries N] [--skip-spark]
Needs the stack up, the landmarks loaded (scripts/start.py once for BR1) and Grafana provisioned.
Side effect: each rep's sink stage truncates and reloads tour.dim_landmark and tour.dist_matrix (same data, ~18 s); run when the dashboards are idle.
Stdlib only. Writes docs/result/br1/benchmark/runs.csv and queries.csv.
"""
import argparse
import csv
import importlib.util
import json
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs" / "result" / "br1" / "benchmark"
LOGS = ROOT / "logs" / "bench"
SPARK_MASTER = "spark://spark-master:7077"
DASHBOARD = "br1-finder"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


start_script = load("start", "scripts/start.py")
dash = load("check_dashboards", "tests/br5/check_dashboards.py")


def wait_cluster_idle(timeout=120):
    # a killed driver can keep its cores on the standalone master for a while,
    # which leaves the next app without executors
    deadline = time.time() + timeout
    while time.time() < deadline:
        with urllib.request.urlopen("http://localhost:8082/json/") as r:
            if not json.load(r).get("activeapps"):
                return
        time.sleep(2)
    raise RuntimeError("Spark master still has active apps")


def ch_rows(sql):
    out = subprocess.run(
        ["docker", "exec", "-i", "clickhouse", "bash", "-c",
         'exec clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --query "$1"', "--", sql],
        capture_output=True, text=True, check=True).stdout
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def run_spark(reps):
    LOGS.mkdir(parents=True, exist_ok=True)
    began = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")
    wait_cluster_idle()
    for rep in range(1, reps + 1):
        cmd = start_script.spark_submit_cmd(1, SPARK_MASTER, job_args=["--bench-only"], packages=None)
        log_path = LOGS / f"br1-rep{rep}.log"
        t0 = time.perf_counter()
        with open(log_path, "w") as log:
            code = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT).returncode
        if code:
            print("\n".join(log_path.read_text().splitlines()[-30:]))
            sys.exit(f"rep {rep} failed (exit {code}), log: {log_path}")
        print(f"rep {rep}: {time.perf_counter() - t0:.1f}s wall", flush=True)
        wait_cluster_idle()

    rows = ch_rows("SELECT ran_at, run_mode, stage, seconds, rows, requests FROM tour.job_run "
                   f"WHERE ran_at >= '{began}' ORDER BY ran_at, run_mode, stage FORMAT JSONEachRow")
    ran_ats = sorted({r["ran_at"] for r in rows})
    OUT.mkdir(parents=True, exist_ok=True)
    fields = ["rep", "ran_at", "run_mode", "stage", "seconds", "rows", "requests"]
    with open(OUT / "runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fields)
        w.writeheader()
        for r in rows:
            w.writerow({"rep": ran_ats.index(r["ran_at"]) + 1, **{k: r[k] for k in fields[1:]}})

    secs = {}  # (run_mode, stage) -> [seconds per rep]
    for r in rows:
        secs.setdefault((r["run_mode"], r["stage"]), []).append(float(r["seconds"]))
    print(f"\n{'mode':<10} {'stage':<16} {'median':>9} {'min':>9} {'max':>9}")
    for (mode, stage), v in sorted(secs.items()):
        print(f"{mode:<10} {stage:<16} {statistics.median(v):>9.3f} {min(v):>9.3f} {max(v):>9.3f}")
    total = {(r["ran_at"], r["run_mode"]): float(r["seconds"]) for r in rows if r["stage"] == "total"}
    speedups = [total[(t, "naive")] / total[(t, "optimized")] for t in ran_ats
                if (t, "naive") in total and (t, "optimized") in total]
    if speedups:
        print("\nspeedup per rep (naive total / optimized total): " + ", ".join(f"{s:.2f}x" for s in speedups))
        print(f"median speedup: {statistics.median(speedups):.2f}x")
    if ("naive", "solve") in secs and ("optimized", "solve") in secs:
        print("median solve speedup: "
              f"{statistics.median(secs[('naive', 'solve')]) / statistics.median(secs[('optimized', 'solve')]):.2f}x")


def run_queries(n):
    found = dash.call(f"/api/dashboards/uid/{DASHBOARD}")
    if "dashboard" not in found:
        sys.exit(f"{DASHBOARD}: dashboard not provisioned ({found.get('message')})")
    board = found["dashboard"]
    time_from = board["time"]["from"]
    values = {}
    for var in board["templating"]["list"]:
        values[var["name"]] = dash.variable_values(var, values, time_from)
    results, failed = [], False
    for panel in board["panels"]:
        for target in panel.get("targets", []):
            title = dash.interpolate(panel["title"], values)
            target = dash.interpolate_target(target, values)
            ms, rows = [], 0
            for _ in range(n):
                t0 = time.perf_counter()
                found_rows, error = dash.query(target, time_from)
                ms.append((time.perf_counter() - t0) * 1000)
                if error:
                    print(f"FAIL {title}: {error}")
                    failed = True
                    break
                rows = found_rows[0]
            else:
                p95 = statistics.quantiles(ms, n=20, method="inclusive")[18] if n >= 2 else ms[0]
                results.append({"panel": title, "runs": n, "rows": rows, "p50_ms": round(statistics.median(ms), 1),
                                "p95_ms": round(p95, 1), "max_ms": round(max(ms), 1)})
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "queries.csv", "w", newline="") as f:
        w = csv.DictWriter(f, ["panel", "runs", "rows", "p50_ms", "p95_ms", "max_ms"])
        w.writeheader()
        w.writerows(results)
    print(f"\n{'panel':<44} {'runs':>4} {'rows':>7} {'p50_ms':>9} {'p95_ms':>9} {'max_ms':>9}")
    for r in results:
        print(f"{r['panel'][:44]:<44} {r['runs']:>4} {r['rows']:>7} {r['p50_ms']:>9} {r['p95_ms']:>9} {r['max_ms']:>9}")
    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("reps", nargs="?", type=int, default=3)
    parser.add_argument("--queries", type=int, default=10)
    parser.add_argument("--skip-spark", action="store_true")
    args = parser.parse_args()
    if not args.skip_spark:
        run_spark(args.reps)
    run_queries(args.queries)


if __name__ == "__main__":
    main()
