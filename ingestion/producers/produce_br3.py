import argparse
import csv
import gzip
import sys
import time
import warnings
from pathlib import Path

from kafka_io import get_producer

# Real Inside Airbnb reviews (Bangkok, snapshot 2026-06-29), gzipped to keep the
# LFS object small; the producer streams it without unpacking to disk.
SAMPLE_PATH = Path(__file__).resolve().parents[2] / "data" / "br3" / "reviews.csv.gz"
TOPIC = "reviews"

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=float, default=0)
parser.add_argument("--loop", action="store_true")
parser.add_argument("--limit", type=int)
args = parser.parse_args()
warnings.filterwarnings("ignore", message=".*value_serializer.*", category=DeprecationWarning)


def review_rows():
    if not SAMPLE_PATH.exists():
        sys.exit(f"{SAMPLE_PATH} not found — run git lfs pull")

    with gzip.open(SAMPLE_PATH, "rt", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row.get("listing_id") or not row.get("id"):
                continue
            yield {
                "listing_id": row["listing_id"],
                "id": row["id"],
                "comments": row.get("comments") or "",
            }


producer = get_producer()
sent = 0
while True:
    for row in review_rows():
        producer.send(TOPIC, row)
        sent += 1
        if sent % 500 == 0:
            print(f"sent {sent} reviews so far (last: {row['id']})")
        if args.limit and sent >= args.limit:
            producer.flush()
            print(f"finished sending {sent} reviews")
            sys.exit(0)
        time.sleep(args.interval)
    if not args.loop:
        break
producer.flush()
print(f"finished sending {sent} reviews")
