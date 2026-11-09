"""Replay cleaned TLC drop-offs in tourist zones into Kafka in event-time order."""

import argparse
import csv
import json
import os
import time
from pathlib import Path

from br7_rules import normalize_event

TOPIC = "br7_taxi_dropoffs"
SOURCE = Path(os.environ.get("BR7_DATA_DIR", "/opt/data/br7")) / "prepared" / "replay"


def events(source=SOURCE):
    files = sorted(source.glob("part-*.csv"))
    if len(files) != 1:
        raise ValueError(f"Expected one sorted replay CSV in {source}; run scripts/prepare_br7.py")
    with files[0].open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            yield normalize_event(row)


def main():
    from kafka import KafkaProducer

    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=0, help="Seconds between events")
    parser.add_argument("--limit", type=int, help="Stop after this many events")
    parser.add_argument("--loop", action="store_true", help="Replay again after the end")
    args = parser.parse_args()
    if args.interval < 0 or (args.limit is not None and args.limit < 1):
        parser.error("interval must be nonnegative and limit must be positive")

    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        acks="all", retries=5, max_in_flight_requests_per_connection=1,
        linger_ms=20, batch_size=256 * 1024,
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    sent = 0
    try:
        while True:
            for event in events():
                producer.send(TOPIC, key=event["dropoff_zone_id"], value=event)
                sent += 1
                if sent % 50000 == 0:
                    print(f"BR7 sent {sent} drop-offs (event time {event['dropoff_time']})", flush=True)
                if args.interval:
                    time.sleep(args.interval)
                if args.limit is not None and sent >= args.limit:
                    break
            producer.flush(timeout=120)
            print(f"BR7 delivered {sent} drop-off events to {TOPIC}", flush=True)
            if not args.loop or (args.limit is not None and sent >= args.limit):
                break
    finally:
        producer.close(timeout=60)


if __name__ == "__main__":
    main()
