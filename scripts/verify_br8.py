"""Verify ClickHouse business rules and the fully replayed BR8 source snapshot."""

import argparse
import base64
import csv
import json
import subprocess
import time
import urllib.request
import uuid
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


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
    database = "br8_verify_" + uuid.uuid4().hex
    sql(f"CREATE DATABASE {database}")
    try:
        sql((ROOT / "sql/clickhouse/ddl_br8.sql").read_text(), database, multi=True)
        sql("INSERT INTO dim_br8_places VALUES ('A', 'Museum A', 'Museum', 'Q1490', 'Tokyo', 'Japan', NULL, NULL), ('B', 'Museum B', 'Museum', 'Q1490', 'Tokyo', 'Japan', NULL, NULL)", database)
        # Three check-ins on one day remain one visit; a retry remains one visit.
        sql("INSERT INTO fact_br8_visits VALUES ('a1', 101, 'A', '2018-05-01', '2018-05-01 10:00:00', 3), ('a1', 101, 'A', '2018-05-01', '2018-05-01 10:00:00', 3), ('a2', 101, 'A', '2018-05-20', '2018-05-20 10:00:00', 1), ('a3', 102, 'A', '2018-05-03', '2018-05-03 10:00:00', 1), ('b1', 101, 'B', '2018-05-02', '2018-05-02 10:00:00', 1)", database)
        actual = rows("SELECT place_id, unique_visitors, returning_visitors, repeat_visit_rate FROM v_br8_place_retention ORDER BY place_id", database)
        assert actual == [
            {"place_id": "A", "unique_visitors": 2, "returning_visitors": 1, "repeat_visit_rate": 50},
            {"place_id": "B", "unique_visitors": 1, "returning_visitors": 0, "repeat_visit_rate": 0},
        ], actual
        history = rows("SELECT visit_date, unique_visitors, returning_visitors, repeat_visit_rate FROM v_br8_daily_retention WHERE place_id = 'A' ORDER BY visit_date", database)
        assert [r["repeat_visit_rate"] for r in history] == [0, 0, 50], history
        assert int(sql("SELECT count() FROM fact_br8_visits FINAL", database)) == 4
        print("PASS: daily visits, same-category different venues, retries, and historical rates")
    finally:
        sql(f"DROP DATABASE {database}")


def verify_snapshot(timeout):
    expected = defaultdict(lambda: defaultdict(set))
    for path in (ROOT / "data/br8/prepared/visits").glob("part-*.csv"):
        with path.open(newline="") as source:
            for record in csv.DictReader(source):
                expected[record["place_id"]][int(record["user_id"])].add(record["visit_date"])
    if not expected:
        raise ValueError("No prepared visits found")
    visits = sum(len(days) for users in expected.values() for days in users.values())
    deadline = time.monotonic() + timeout
    while True:
        actual_count = int(sql("SELECT count() FROM fact_br8_visits FINAL"))
        if actual_count == visits:
            break
        if time.monotonic() >= deadline:
            raise AssertionError(f"Expected {visits} visits, received {actual_count}; replay the full CSV")
        time.sleep(2)
    actual = {r["place_id"]: r for r in rows("SELECT * FROM v_br8_place_retention")}
    assert set(actual) == set(expected), (set(actual), set(expected))
    for place, users in expected.items():
        returning = sum(len(days) >= 2 for days in users.values())
        record = actual[place]
        assert record["unique_visitors"] == len(users), record
        assert record["returning_visitors"] == returning, record
        assert abs(record["repeat_visit_rate"] - returning * 100 / len(users)) <= 0.011, record
    # Execute each dashboard SQL query as well, so SQL/schema drift is visible.
    dashboard = json.loads((ROOT / "grafana/dashboards/br8-tourist-retention.json").read_text())
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            sql(target["rawSql"])
    print(f"PASS: {visits} visits and all {len(expected)} named places match the source reference; dashboard SQL passes")


def verify_grafana():
    request = urllib.request.Request("http://localhost:3000/api/dashboards/uid/co5173-br8-tourist-retention")
    request.add_header("Authorization", "Basic " + base64.b64encode(b"admin:admin").decode())
    with urllib.request.urlopen(request, timeout=15) as response:
        dashboard = json.load(response)["dashboard"]
    assert dashboard["uid"] == "co5173-br8-tourist-retention"
    for panel in dashboard["panels"]:
        if not panel.get("targets"):
            continue
        body = json.dumps({
            "queries": [dict(target, intervalMs=1000, maxDataPoints=500) for target in panel["targets"]],
            "from": "1506816000000", "to": "1540080000000",
        }).encode()
        query_request = urllib.request.Request("http://localhost:3000/api/ds/query", data=body)
        query_request.add_header("Authorization", "Basic " + base64.b64encode(b"admin:admin").decode())
        query_request.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(query_request, timeout=30) as response:
            result = json.load(response)["results"]
        for key, item in result.items():
            assert item.get("status", 200) == 200 and not item.get("error"), (panel["title"], key, item)
            assert item.get("frames"), (panel["title"], item)
    print("PASS: Grafana provisioning and every panel query through the ClickHouse datasource")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules-only", action="store_true", help="Only run isolated ClickHouse rule tests")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    verify_rules()
    if not args.rules_only:
        verify_snapshot(args.timeout)
        verify_grafana()


if __name__ == "__main__":
    main()
