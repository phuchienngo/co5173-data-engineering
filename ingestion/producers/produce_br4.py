import argparse
import csv
import sys
import time
import warnings
from pathlib import Path

from kafka_io import get_producer

SAMPLE_PATH = Path(__file__).resolve().parents[2] / "data" / "br4" / "Fact_Bookings.csv"
TOPIC = "bookings"

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=float, default=0)
parser.add_argument("--loop", action="store_true")
parser.add_argument("--limit", type=int)
args = parser.parse_args()
warnings.filterwarnings("ignore", message=".*value_serializer.*", category=DeprecationWarning)


def booking_rows():
    if not SAMPLE_PATH.exists():
        sys.exit(f"{SAMPLE_PATH} not found — see README \"Preparing data\"")

    with SAMPLE_PATH.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            listing_id = row.get("Property_ID")
            if not listing_id:
                continue
            yield {
                "listing_id": listing_id,
                "id": row.get("Booking_ID") or f"booking-{listing_id}",
                "revenue": row.get("Revenue"),
                "nights_booked": row.get("Nights_Booked"),
            }


producer = get_producer()
sent = 0
while True:
    for row in booking_rows():
        producer.send(TOPIC, row)
        sent += 1
        if sent % 500 == 0:
            print(f"sent {sent} bookings so far (last: {row['id']})")
        if args.limit and sent >= args.limit:
            producer.flush()
            print(f"finished sending {sent} bookings")
            sys.exit(0)
        time.sleep(args.interval)
    if not args.loop:
        break
producer.flush()
print(f"finished sending {sent} bookings")
