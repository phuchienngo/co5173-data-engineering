#!/usr/bin/env python3
"""Replay normalized flight events into Kafka."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from common.config import settings


def load_events(month: str | None, start: str | None, days: int | None,
                airports: list[str] | None, limit: int) -> pd.DataFrame:
    root = Path(settings.lake_dir) / "flight_events"
    if not root.exists():
        print(f"✗ Missing {root} — run ./scripts/run_job.sh br9_02")
        sys.exit(1)

    months = sorted(p.name.split("=")[1] for p in root.glob("year_month=*"))
    month = month or months[-1]
    part = root / f"year_month={month}"
    if not part.exists():
        print(f"✗ Month {month} is unavailable. Available: {', '.join(months)}")
        sys.exit(1)

    df = ds.dataset(str(part), format="parquet").to_table().to_pandas()
    df["year_month"] = month
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    df["scheduled_ts"] = pd.to_datetime(df["scheduled_ts"], utc=True)

    if airports:
        df = df[df["airport"].isin(airports)]
    if start:
        t0 = pd.Timestamp(start, tz="UTC")
        df = df[df["event_ts"] >= t0]
        if days:
            df = df[df["event_ts"] < t0 + pd.Timedelta(days=days)]
    df = df.sort_values("event_ts", kind="stable")
    if limit:
        df = df.head(limit)
    return df.reset_index(drop=True)


def to_message(row) -> dict:
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    return {
        "event_id": row.event_id,
        "flight_key": row.flight_key,
        "airport": row.airport,
        "city_id": row.city_id,
        "event_type": row.event_type,
        "event_ts": row.event_ts.strftime(fmt)[:-3],
        "scheduled_ts": row.scheduled_ts.strftime(fmt)[:-3],
        "delay_min": None if pd.isna(row.delay_min) else int(row.delay_min),
        "is_delayed": int(row.is_delayed),
        "carrier": row.carrier,
        "flight_number": str(row.flight_number),
        "origin": row.origin,
        "dest": row.dest,
        "year_month": row.year_month,
        "produced_at": datetime.now(timezone.utc).strftime(fmt)[:-3],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=settings.kafka_bootstrap)
    ap.add_argument("--topic", default=settings.kafka_topic_flights)
    ap.add_argument("--month", help="YYYY-MM (default: latest lake month)")
    ap.add_argument("--start", help="YYYY-MM-DD (UTC) — replay start date")
    ap.add_argument("--days", type=int, help="number of days from --start")
    ap.add_argument("--airports", help="comma-separated IATA codes")
    ap.add_argument("--speedup", type=float, default=300, help="time compression factor; 0 = fastest")
    ap.add_argument("--max-sleep", type=float, default=2.0, help="maximum wait in seconds between events")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    airports = [a.strip().upper() for a in args.airports.split(",")] if args.airports else None
    events = load_events(args.month, args.start, args.days, airports, args.limit)

    print("=" * 72)
    print("  BR9 · Flight-event producer → Kafka")
    print("=" * 72)
    if events.empty:
        print("✗ No events match the filters.")
        return 1
    print(f"  {len(events):,} events · {events['airport'].nunique()} airports")
    print(f"  event time (UTC): {events['event_ts'].min()} → {events['event_ts'].max()}")
    print("  " + " · ".join(f"{k} {v:,}" for k, v in events["event_type"].value_counts().items()))

    if args.dry_run:
        print("\n  [DRY RUN] First three messages:")
        for r in events.head(3).itertuples():
            print("  " + json.dumps(to_message(r), ensure_ascii=False))
        return 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),

            key_serializer=lambda k: k.encode("utf-8"),
            acks="all", linger_ms=20, compression_type="gzip", retries=3,
        )
    except NoBrokersAvailable:
        print(f"✗ Cannot connect to Kafka at {args.bootstrap}")
        return 1

    speed = "as fast as possible" if args.speedup <= 0 else f"{args.speedup:g}×"
    print(f"\n→ Sending to {args.bootstrap} / {args.topic} · time compression {speed}")

    t0 = time.time()
    prev = None
    for i, r in enumerate(events.itertuples(), 1):
        if args.speedup > 0 and prev is not None:
            gap = (r.event_ts - prev).total_seconds() / args.speedup
            if gap > 0:
                time.sleep(min(gap, args.max_sleep))
        prev = r.event_ts
        producer.send(args.topic, key=r.airport, value=to_message(r))
        if i % 500 == 0:
            producer.flush()
            rate = i / max(time.time() - t0, 1e-3)
            print(f"  {i:>7,}/{len(events):,}  {rate:6.0f} events/second  "
                  f"event time {r.event_ts:%Y-%m-%d %H:%M} UTC", flush=True)

    producer.flush()
    producer.close()
    print(f"\n✓ Sent {len(events):,} events in {time.time() - t0:.1f}s")
    print("  Check:  SELECT count(), max(event_ts) FROM tourism.raw_flight_events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
