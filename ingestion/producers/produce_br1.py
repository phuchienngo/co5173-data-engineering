import argparse
import csv
import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka_io import get_producer

LANDMARKS_PATH = os.environ.get("BR1_LANDMARKS", "/opt/data/br1/landmarks.csv")
SEED = int(os.environ.get("BR1_SEED", "42"))
TOPIC = "tour.requests"
DEFAULT_LIMIT = 20000

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=float, default=0)
parser.add_argument("--loop", action="store_true")
parser.add_argument("--limit", type=int)
args = parser.parse_args()


def stops_by_city():
    if not os.path.exists(LANDMARKS_PATH):
        sys.exit(f"{LANDMARKS_PATH} not found — run scripts/fetch_osm_landmarks.py first")
    by_city = defaultdict(list)
    with open(LANDMARKS_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_city[row["city"]].append(row["osm_id"])
    return {city: ids for city, ids in by_city.items() if len(ids) >= 4}


def tour_requests(stops, rng):
    cities = sorted(stops)
    request_id = 0
    while True:
        city = rng.choice(cities)
        ids = stops[city]
        yield {
            "request_id": request_id,
            "city": city,
            "stop_ids": rng.sample(ids, rng.randint(4, min(8, len(ids)))),
            "requested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        request_id += 1


producer = get_producer()
limit = args.limit or (0 if args.loop else DEFAULT_LIMIT)  # 0 = unlimited
sent = 0
for event in tour_requests(stops_by_city(), random.Random(SEED)):
    if limit and sent >= limit:
        break
    producer.send(TOPIC, event)
    sent += 1
    if sent % 500 == 0:
        print(f"sent {sent} tour requests so far (last: {event['request_id']})")
    time.sleep(args.interval)
producer.flush()
print(f"finished sending {sent} tour requests")
