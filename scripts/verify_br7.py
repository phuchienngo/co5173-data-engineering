"""Verify BR7 in ClickHouse and Grafana against an independent reference.

--rules-only : isolated database, synthetic rows, checks idempotency and views.
default      : additionally compares every closed zone-hour after a full replay
               with counts and forecasts computed here from the prepared CSVs.
"""

import argparse
import base64
import csv
import glob
import json
import subprocess
import sys
import time
import urllib.request
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs" / "common"))
from br7_rules import forecast_rows  # noqa: E402

WATERMARK = timedelta(minutes=15)  # must match jobs/br7_tourist_arrival_alerts/job.py
DASHBOARD = ROOT / "grafana/dashboards/br7-tourist-arrivals.json"


def sql(query, database="default", multi=False):
    result = subprocess.run([
        "docker", "exec", "-i", "clickhouse", "clickhouse-client", "--user", "default",
        "--password", "clickhouse", "--database", database,
        "--multiquery" if multi else "--query", *([] if multi else [query]),
    ], input=query if multi else None, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def rows(query, database="default"):
    return [json.loads(line) for line in sql(query + " FORMAT JSONEachRow", database).splitlines()]


def verify_rules():
    database = "br7_verify_" + uuid.uuid4().hex
    sql(f"CREATE DATABASE {database}")
    try:
        sql((ROOT / "sql/clickhouse/ddl_br7.sql").read_text(), database, multi=True)
        sql("INSERT INTO dim_br7_zones VALUES (230, 'Times Sq/Theatre District', 'Manhattan', 'Times Square')", database)
        # 22:00 forecasts 23:00; the 22:00 row is written twice (retried batch).
        insert = ("INSERT INTO fact_br7_zone_hourly (zone_id, observed_hour, target_hour, observed_arrivals, "
                  "observed_passengers, usual_arrivals, predicted_arrivals, warning_threshold, alert_level) VALUES ")
        sql(insert + "(230, '2024-11-05 22:00:00', '2024-11-05 23:00:00', 150, 170, 100, 300, 250, 'warning')", database)
        sql(insert + "(230, '2024-11-05 22:00:00', '2024-11-05 23:00:00', 150, 170, 100, 300, 250, 'warning')", database)
        sql(insert + "(230, '2024-11-05 23:00:00', '2024-11-06 00:00:00', 280, 300, 200, 120, 150, 'normal')", database)
        assert int(sql("SELECT count() FROM v_br7_zone_hourly", database)) == 2
        check = rows("SELECT target_hour, actual_arrivals, abs_error, actual_above_threshold FROM v_br7_forecast_check", database)
        assert check == [{"target_hour": "2024-11-05 23:00:00", "actual_arrivals": 280,
                          "abs_error": 20, "actual_above_threshold": 1}], check
        local = sql("SELECT toString(observed_hour_local) FROM v_br7_zone_hourly ORDER BY observed_hour LIMIT 1", database)
        assert local == "2024-11-05 17:00:00", local
        print("PASS: retried batch is deduplicated, forecast check joins the next hour, local time is New York")
    finally:
        sql(f"DROP DATABASE {database}")


def read_prepared(name):
    files = glob.glob(str(ROOT / f"data/br7/prepared/{name}/part-*.csv"))
    if len(files) != 1:
        raise ValueError(f"Missing prepared {name}; run scripts/prepare_br7.py")
    with open(files[0], newline="") as stream:
        return list(csv.DictReader(stream))


def expected_facts():
    counts, passengers = Counter(), Counter()
    latest = None
    for record in read_prepared("replay"):
        moment = datetime.fromisoformat(record["dropoff_time"].replace("Z", "+00:00"))
        hour = moment.replace(minute=0, second=0, microsecond=0)
        key = (int(record["dropoff_zone_id"]), hour)
        counts[key] += 1
        passengers[key] += int(record["passenger_count"] or 0)
        latest = moment if latest is None or moment > latest else latest
    watermark = latest - WATERMARK
    closed = sorted(key for key in counts if key[1] + timedelta(hours=1) <= watermark)
    baseline = {(int(r["zone_id"]), int(r["hour_of_week"])): (float(r["mean_arrivals"]), float(r["p90_arrivals"]))
                for r in read_prepared("baseline")}
    facts = forecast_rows([(zone, hour, counts[(zone, hour)], passengers[(zone, hour)]) for zone, hour in closed], baseline)
    return {(f[0], f[1].strftime("%Y-%m-%d %H:%M:%S")): f for f in facts}


def verify_replay(timeout):
    expected = expected_facts()
    deadline = time.monotonic() + timeout
    while True:
        actual_count = int(sql("SELECT count() FROM fact_br7_zone_hourly FINAL"))
        if actual_count >= len(expected):
            break
        if time.monotonic() >= deadline:
            raise AssertionError(f"Expected {len(expected)} closed zone-hours, found {actual_count}; replay the full CSV")
        time.sleep(3)
    actual = {
        (r["zone_id"], r["observed_hour"]): r
        for r in rows("SELECT zone_id, toString(observed_hour) AS observed_hour, observed_arrivals, "
                      "observed_passengers, predicted_arrivals, warning_threshold, alert_level "
                      "FROM fact_br7_zone_hourly FINAL")
    }
    missing = set(expected) - set(actual)
    assert not missing, f"{len(missing)} zone-hours missing, e.g. {sorted(missing)[:3]}"
    for key, fact in expected.items():
        row = actual[key]
        assert row["observed_arrivals"] == fact[3], (key, row, fact)
        assert row["observed_passengers"] == fact[4], (key, row, fact)
        assert row["alert_level"] == fact[8], (key, row, fact)
        if fact[6] is not None:
            assert abs(row["predicted_arrivals"] - fact[6]) < 0.011, (key, row, fact)
    warnings = sum(1 for f in expected.values() if f[8] == "warning")
    print(f"PASS: {len(expected)} closed zone-hours match the CSV reference "
          f"({sum(f[3] for f in expected.values())} arrivals, {warnings} warnings)")


def grafana(path, body=None):
    request = urllib.request.Request(f"http://localhost:3000{path}", data=body)
    request.add_header("Authorization", "Basic " + base64.b64encode(b"admin:admin").decode())
    if body:
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def verify_dashboard():
    dashboard = json.loads(DASHBOARD.read_text())
    zone = dashboard["templating"]["list"][0]["current"]["value"]
    time_range = [str(int(datetime.fromisoformat(dashboard["time"][k].replace("Z", "+00:00")).timestamp() * 1000))
                  for k in ("from", "to")]
    provisioned = grafana(f"/api/dashboards/uid/{dashboard['uid']}")["dashboard"]
    assert provisioned["uid"] == dashboard["uid"]
    for panel in dashboard["panels"]:
        target = dict(panel["targets"][0], intervalMs=60000, maxDataPoints=1000)
        target["rawSql"] = target["rawSql"].replace("${zone}", zone)
        body = json.dumps({"queries": [target], "from": time_range[0], "to": time_range[1]}).encode()
        result = grafana("/api/ds/query", body)["results"]["A"]
        assert result.get("status", 200) == 200 and not result.get("error"), (panel["title"], result)
        frames = result.get("frames") or []
        assert frames and frames[0]["data"]["values"] and frames[0]["data"]["values"][0], (panel["title"], "no rows")
    print(f"PASS: Grafana dashboard provisioned and all {len(dashboard['panels'])} panel queries return data")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules-only", action="store_true", help="Only run the isolated ClickHouse checks")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    verify_rules()
    if not args.rules_only:
        verify_replay(args.timeout)
        verify_dashboard()


if __name__ == "__main__":
    main()
