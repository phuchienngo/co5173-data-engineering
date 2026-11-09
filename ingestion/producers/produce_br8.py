"""Replay normalized TripWorld daily visits in historical order."""

import argparse
import csv
import json
import os
import time
from pathlib import Path

from br8_rules import normalize_event

TOPIC = "br8_tourist_visits"
SOURCE = Path(os.environ.get("BR8_DATA_DIR", "/opt/data/br8")) / "prepared" / "visits"


def events(source=SOURCE):
    files = sorted(source.glob("part-*.csv"))
    if len(files) != 1:
        raise ValueError(f"Expected one sorted prepared CSV in {source}; run scripts/prepare_br8.py")
    with files[0].open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            yield normalize_event(row)


def main():
    from kafka import KafkaProducer

    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    if args.interval < 0 or (args.limit is not None and args.limit < 1):
        parser.error("interval must be nonnegative and limit must be positive")
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        acks="all", retries=5, max_in_flight_requests_per_connection=1,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    sent = 0
    try:
        while True:
            cycle = 0
            for event in events():
                producer.send(TOPIC, key=event["place_id"], value=event)
                sent += 1
                cycle += 1
                if args.interval:
                    time.sleep(args.interval)
                if args.limit is not None and sent >= args.limit:
                    break
            producer.flush(timeout=60)
            print(f"BR8 delivered {sent} daily visit events to {TOPIC}", flush=True)
            if not args.loop or cycle == 0 or (args.limit is not None and sent >= args.limit):
                break
    finally:
        producer.close(timeout=60)


if __name__ == "__main__":
    main()
