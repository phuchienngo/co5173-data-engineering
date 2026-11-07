import argparse
import os
import sys
import time
import warnings
from datetime import timezone

import pyarrow.dataset as ds

from kafka_io import get_producer

FACT_PATH = "/opt/data/br2/lake/fact_trips"
SERVICE = os.getenv("BR2_SERVICE", "yellow")
REPLAY_MONTH = os.getenv("BR2_REPLAY_MONTH", "2026-07")
TOPIC = "tlc.trips"

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=float, default=0)
parser.add_argument("--loop", action="store_true")
parser.add_argument("--limit", type=int)
args = parser.parse_args()
warnings.filterwarnings("ignore", message=".*value_serializer.*", category=DeprecationWarning)


def trip_rows():
    if not os.path.isdir(FACT_PATH):
        sys.exit(f"{FACT_PATH} not found — run the BR2 batch first (make demo-br BR=2)")

    table = ds.dataset(FACT_PATH, format="parquet", partitioning="hive").to_table(
        columns=["pickup_datetime", "pu_location_id", "do_location_id", "fare_amount"],
        filter=(ds.field("service") == SERVICE) & (ds.field("pickup_month") == REPLAY_MONTH),
    )
    if table.num_rows == 0:
        sys.exit(f"no fact rows for {SERVICE} {REPLAY_MONTH} in {FACT_PATH} — run the BR2 batch / download_tlc.sh first")

    # pickup order keeps the stream's watermark from dropping replayed trips
    for batch in table.sort_by("pickup_datetime").to_batches(max_chunksize=10_000):
        for row in batch.to_pylist():
            pickup = row["pickup_datetime"]
            if pickup.tzinfo is None:
                pickup = pickup.replace(tzinfo=timezone.utc)
            yield {
                "service": SERVICE,
                "pickup_datetime": pickup.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "pu_location_id": row["pu_location_id"],
                "do_location_id": row["do_location_id"],
                "fare_amount": row["fare_amount"],
            }


producer = get_producer()
sent = 0
while True:
    for row in trip_rows():
        producer.send(TOPIC, row)
        sent += 1
        if sent % 500 == 0:
            print(f"sent {sent} trips so far (last pickup: {row['pickup_datetime']})")
        if args.limit and sent >= args.limit:
            producer.flush()
            print(f"finished sending {sent} trips")
            sys.exit(0)
        time.sleep(args.interval)
    if not args.loop:
        break
producer.flush()
print(f"finished sending {sent} trips")
