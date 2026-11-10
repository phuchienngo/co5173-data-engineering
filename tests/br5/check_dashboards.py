"""Runs every panel query of the BR5 Grafana dashboards through Grafana's API
and reports errors and empty results (docs/plan/br5/plan-monitoring.md, row 2).

Grafana's query API doesn't fill in dashboard variables (the browser does), so
this script does: each variable takes its default, and "All" becomes every
value its own query returns. Run from the host with the stack up:

    python tests/br5/check_dashboards.py [--from now-30d]   (default: each dashboard's time range)
    python tests/br5/check_dashboards.py --br 6               (BR6's dashboards, docs/plan/br6/plan-monitoring.md H7)
    python tests/br5/check_dashboards.py --br 1                (BR1 dashboards; --br 2 for BR2)
    GRAFANA_AUTH=user:password python tests/br5/check_dashboards.py   (default admin:admin)

Exit code 1 when a query fails. Empty results are listed but don't fail:
some panels are only filled after certain runs (for example a failed run).
"""

import argparse
import base64
import json
import os
import re
import sys
import urllib.request

GRAFANA = "http://localhost:3000"
AUTH = "Basic " + base64.b64encode(os.environ.get("GRAFANA_AUTH", "admin:admin").encode()).decode()  # user:password
DASHBOARDS = ["br5-run-performance", "br5-resource-use", "br5-data-result"]
BR_DASHBOARDS = {"1": ["br1-tour", "br1-finder"], "2": ["br2-pricing", "br2-advisor"], "5": DASHBOARDS, "6": ["br6-run-performance", "br6-resource-use", "br6-data-result"]}


def call(path, body=None):
    request = urllib.request.Request(GRAFANA + path, data=json.dumps(body).encode() if body else None,
                                     headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read() or b"{}") | {"_http": exc.code}


def query(target, time_from):
    body = {"from": time_from, "to": "now",
            "queries": [{"intervalMs": 60_000, "maxDataPoints": 1000, **target, "refId": target.get("refId", "A")}]}
    result = call("/api/ds/query", body)
    if "_http" in result and "results" not in result:
        return None, result.get("message", f"HTTP {result['_http']}")
    res = result["results"][target.get("refId", "A")]
    if res.get("error"):
        return None, res["error"]
    frames = res.get("frames", [])
    rows = sum(len(f["data"]["values"][0]) if f.get("data", {}).get("values") else 0 for f in frames)
    return (rows, frames), None


def regex_escape(value):
    # As Grafana does for Prometheus: regex characters only ("-" stays as is).
    return re.sub(r"([\\^$*+?.()|{}\[\]])", r"\\\1", value)


def interpolate(text, values):
    def repl(match):
        name, fmt = match.group(1) or match.group(3), match.group(2)
        if name not in values:
            return match.group(0)
        vals = values[name]
        if isinstance(vals, str):
            return vals
        if fmt == "singlequote":
            return ",".join("'" + v.replace("'", "\\'") + "'" for v in vals)
        if fmt == "regex" or len(vals) > 1:
            return "(" + "|".join(regex_escape(v) for v in vals) + ")"
        return vals[0]

    return re.sub(r"\$\{(\w+)(?::(\w+))?\}|\$(\w+)", repl, text)


def variable_values(var, values, time_from):
    current = var.get("current", {}).get("value")
    current = current if isinstance(current, list) else [current]
    if current != ["$__all"]:
        return [str(v) for v in current]
    if var.get("allValue"):
        return var["allValue"]  # a str: Grafana inserts a custom all value raw, unformatted
    if var["type"] == "custom":
        return [o["value"] for o in var["options"]]
    found, error = query({"datasource": var["datasource"], "editorType": "sql", "format": 1, "queryType": "table",
                          "rawSql": interpolate(var["query"], values), "refId": "V"}, time_from)
    if error:
        raise RuntimeError(f"variable {var['name']}: {error}")
    _, frames = found
    return [str(v) for f in frames for v in f["data"]["values"][0]] or ["__none__"]


def interpolate_target(target, values):
    target = dict(target)
    for key in ("rawSql", "expr"):
        if key in target:
            target[key] = interpolate(target[key], values)
    if isinstance(target.get("query"), dict):
        target["query"] = interpolate(json.dumps(target["query"]), values)
    return target


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="time_from", help="Default: each dashboard's own time range")
    parser.add_argument("--br", choices=sorted(BR_DASHBOARDS), default="5", help="Whose dashboards (default: 5)")
    args = parser.parse_args()

    failures, empty, checked = [], [], 0
    for uid in BR_DASHBOARDS[args.br]:
        found = call(f"/api/dashboards/uid/{uid}")
        if "dashboard" not in found:
            failures.append(f"{uid}: dashboard not provisioned ({found.get('message')})")
            continue
        board = found["dashboard"]
        time_from = args.time_from or board["time"]["from"]
        print(f"{board['title']}  (folder: {found['meta'].get('folderTitle')}, from {time_from})")
        values = {}
        for var in board["templating"]["list"]:
            values[var["name"]] = variable_values(var, values, time_from)
        items = [(p["title"], t) for p in board["panels"] for t in p.get("targets", [])]
        items += [(f"annotation: {a['name']}", a["target"]) for a in board["annotations"]["list"] if "target" in a]
        for title, target in items:
            title = interpolate(title, values)
            checked += 1
            result, error = query(interpolate_target(target, values), time_from)
            if error:
                failures.append(f"{board['title']} / {title}: {error}")
                print(f"  FAIL  {title}: {error}")
                continue
            rows = result[0]
            if not rows:
                empty.append(f"{board['title']} / {title}")
            print(f"  ok    {title}: {rows} rows")
    print(f"\n{checked} queries, {len(failures)} failed, {len(empty)} empty")
    for item in empty:
        print(f"  empty: {item}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
