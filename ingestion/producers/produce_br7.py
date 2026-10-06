"""Replay the last 30 days of six core tourist sites into Kafka in event-time order."""

import argparse
import csv
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

from br7_sites import CORE_SITES, HOLDOUT_DAYS
from kafka_io import get_producer

SOURCE_PATH = Path(__file__).resolve().parents[2] / "data" / "br7" / "frequentation_par_heure.csv"
TOPIC = "tourist_arrivals"


def replay_rows():
    if not SOURCE_PATH.exists():
        sys.exit(f"{SOURCE_PATH} is missing; run scripts/prepare_br7.py")
    rows = []
    with SOURCE_PATH.open(encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            if row.get("site") not in CORE_SITES:
                continue
            try:
                event_time = datetime.fromisoformat(row["horodatage"])
                entries = int(row["entrees"])
            except (KeyError, TypeError, ValueError):
                continue
            if entries < 0:
                continue
            rows.append((event_time, row["site"], entries))
    if not rows:
        sys.exit("No valid core-site records in BR7 source")
    cutoff = max(row[0] for row in rows) - timedelta(days=HOLDOUT_DAYS)
    selected = sorted(row for row in rows if row[0] >= cutoff)
    print(f"Replaying {len(selected)} records from {cutoff.isoformat()} onward", flush=True)
    for event_time, site, entries in selected:
        yield {"site": site, "horodatage": event_time.isoformat(), "entrees": entries}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=0)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    warnings.filterwarnings("ignore", message=".*value_serializer.*", category=DeprecationWarning)

    producer = get_producer()
    sent = 0
    while True:
        for row in replay_rows():
            producer.send(TOPIC, row)
            sent += 1
            if sent % 500 == 0:
                print(f"sent {sent} tourist arrival hours (last: {row['site']})", flush=True)
            if args.limit and sent >= args.limit:
                producer.flush()
                print(f"finished sending {sent} tourist arrival hours")
                return
            time.sleep(args.interval)
        if not args.loop:
            break
    producer.flush()
    print(f"finished sending {sent} tourist arrival hours")


if __name__ == "__main__":
    main()
