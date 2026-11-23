"""BR2 benchmark: Parquet staging + partition pruning, and the advisor dashboard's query latency.

Part 1 (Spark): rebuild the capacity baseline by re-reading the raw monthly TLC files ("raw")
vs reading the normalized lake partitioned by service/pickup_month ("lake"). Each rep runs
`jobs/br2_dynamic_pricing/job.py --bench` through spark-submit; the job writes its timings to
pricing.job_run. Rep 1 also times the one-off raw -> lake build. Raw and lake order alternate
per rep. Hourly rows and checksum must be identical for both variants.
Part 2 (always): runs every panel query of Grafana dashboard br2-advisor N times.

Usage: python3 tests/br2/benchmark.py [REPS] [--queries N] [--skip-spark]    (REPS 3, N 10)
Needs the stack up (docker compose, Grafana on :3000) and data/br2/raw/yellow downloaded.
Writes docs/result/br2/benchmark/runs.csv and queries.csv; stdlib only.
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
OUT = ROOT / "docs" / "result" / "br2" / "benchmark"
LOGS = ROOT / "logs" / "bench"
SPARK_MASTER = "spark://spark-master:7077"
DASHBOARD = "br2-advisor"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


start_script = load("start", ROOT / "scripts" / "start.py")
checks = load("check_dashboards", ROOT / "tests" / "br5" / "check_dashboards.py")


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
    return [json.loads(line) for line in out.splitlines() if line]


def run_spark(reps):
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    start_script.create_clickhouse_tables(2, truncate=False)
    began = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")
    wait_cluster_idle()
    for rep in range(1, reps + 1):
        job_args = ["--bench", "--bench-rep", str(rep), "--bench-order", "raw,lake" if rep % 2 else "lake,raw"]
        if rep == 1:
            job_args.append("--bench-lake-build")
        cmd = start_script.spark_submit_cmd(2, SPARK_MASTER, job_args=job_args, packages=None)
        log_path = LOGS / f"br2-rep{rep}.log"
        t0 = time.perf_counter()
        with open(log_path, "w") as log:
            code = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT).returncode
        if code:
            print(f"rep {rep} failed (exit {code}); last lines of {log_path}:")
            print("\n".join(log_path.read_text().splitlines()[-30:]))
            sys.exit(1)
        print(f"rep {rep}: {time.perf_counter() - t0:.1f}s wall ({log_path})", flush=True)
        wait_cluster_idle()

    rows = ch_rows(f"SELECT rep, toString(ran_at) AS ran_at, run_mode, stage, seconds, rows, checksum "
                   f"FROM pricing.job_run WHERE ran_at >= '{began}' ORDER BY ran_at, run_mode, stage FORMAT JSONEachRow")
    fields = ["rep", "ran_at", "run_mode", "stage", "seconds", "rows", "checksum"]
    with open(OUT / "runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows({k: r[k] for k in fields} for r in rows)

    for r in rows:
        r["seconds"], r["rep"], r["rows"], r["checksum"] = float(r["seconds"]), int(r["rep"]), int(r["rows"]), int(r["checksum"])
    build = [r["seconds"] for r in rows if r["stage"] == "lake_build"]
    if build:
        print(f"lake_build: {build[0]:.1f}s")
    print(f"{'run_mode':<9}{'stage':<10}{'median':>8}{'min':>8}{'max':>8}")
    for mode in ("raw", "lake"):
        for stage in ("hourly", "capacity", "total"):
            s = [r["seconds"] for r in rows if r["run_mode"] == mode and r["stage"] == stage]
            if s:
                print(f"{mode:<9}{stage:<10}{statistics.median(s):>8.2f}{min(s):>8.2f}{max(s):>8.2f}")
    total = {(r["run_mode"], r["rep"]): r["seconds"] for r in rows if r["stage"] == "total"}
    speedups = {rep: total[("raw", rep)] / total[("lake", rep)] for rep in range(1, reps + 1)
                if ("raw", rep) in total and ("lake", rep) in total}
    if speedups:
        print(f"speedup raw/lake total: median {statistics.median(speedups.values()):.2f}x; per rep "
              + ", ".join(f"{k}: {v:.2f}x" for k, v in speedups.items()))
    seen = {(r["rows"], r["checksum"]) for r in rows if r["stage"] == "hourly"}
    modes = {r["run_mode"] for r in rows if r["stage"] == "hourly"}
    ok = len(seen) == 1 and modes == {"raw", "lake"}
    print(f"hourly rows/checksum across all runs: {sorted(seen)} {'OK' if ok else 'MISMATCH'}")
    if not ok:
        sys.exit(1)


def run_queries(n):
    OUT.mkdir(parents=True, exist_ok=True)
    found = checks.call(f"/api/dashboards/uid/{DASHBOARD}")
    if "dashboard" not in found:
        sys.exit(f"{DASHBOARD}: dashboard not provisioned ({found.get('message')})")
    board = found["dashboard"]
    time_from = board["time"]["from"]
    values = {}
    for var in board["templating"]["list"]:
        values[var["name"]] = checks.variable_values(var, values, time_from)
    out, failed = [], False
    for panel in board["panels"]:
        for target in panel.get("targets", []):
            title = checks.interpolate(panel["title"], values)
            target = checks.interpolate_target(target, values)
            times, rows = [], 0
            for _ in range(n):
                t0 = time.perf_counter()
                result, error = checks.query(target, time_from)
                times.append((time.perf_counter() - t0) * 1000)
                if error:
                    print(f"FAIL {title}: {error}")
                    failed = True
                    break
                rows = result[0]
            else:
                p95 = statistics.quantiles(times, n=20, method="inclusive")[18] if n >= 2 else max(times)
                out.append({"panel": title, "runs": n, "rows": rows, "p50_ms": round(statistics.median(times), 1),
                            "p95_ms": round(p95, 1), "max_ms": round(max(times), 1)})
    with open(OUT / "queries.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["panel", "runs", "rows", "p50_ms", "p95_ms", "max_ms"])
        w.writeheader()
        w.writerows(out)
    print(f"{'panel':<50}{'rows':>7}{'p50_ms':>9}{'p95_ms':>9}{'max_ms':>9}")
    for r in out:
        print(f"{r['panel'][:49]:<50}{r['rows']:>7}{r['p50_ms']:>9}{r['p95_ms']:>9}{r['max_ms']:>9}")
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
