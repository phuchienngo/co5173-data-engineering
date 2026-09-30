#!/usr/bin/env python3
"""Download configured Inside Airbnb snapshots."""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

from common.config import settings
from common.destinations import DESTINATIONS, by_city, latest_snapshot

BASE = "https://data.insideairbnb.com"
ROOT = Path(settings.raw_dir) / "airbnb"
HEADERS = {"User-Agent": "Mozilla/5.0 (HCMUT data engineering assignment)"}


def url_for(dest: dict, snap: str, name: str) -> str:
    folder = "visualisations" if name.startswith("neighbourhoods") else "data"
    return f"{BASE}/{dest['airbnb_path']}/{snap}/{folder}/{name}"


def fetch(url: str, dst: Path) -> None:
    if dst.exists() and dst.stat().st_size > 0:
        print(f"    ✓ already present  {dst.name:24s} {dst.stat().st_size / 1e6:7.1f} MB")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
        if r.status_code in (403, 404):
            raise FileNotFoundError(f"HTTP {r.status_code} — snapshot not found: {url}")
        r.raise_for_status()
        total = 0
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                total += len(chunk)
                print(f"\r    ↓ {dst.name:24s} {total / 1e6:7.1f} MB", end="", flush=True)
    tmp.rename(dst)
    print()


def discover(dest: dict, days_back: int = 200) -> list[str]:
    """Discover."""
    found = []
    start = date.fromisoformat(latest_snapshot(dest))
    for i in range(days_back):
        d = (start - timedelta(days=i)).isoformat()
        try:
            r = requests.head(url_for(dest, d, "listings.csv.gz"), headers=HEADERS, timeout=10)
            if r.status_code == 200:
                found.append(d)
                print(f"    found {d}")
        except requests.RequestException:
            pass
    return sorted(found)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--city", action="append", help="city_id; repeat to select multiple cities")
    ap.add_argument("--discover", action="store_true", help="discover snapshots without downloading")
    args = ap.parse_args()

    dests = [by_city(c) for c in args.city] if args.city else DESTINATIONS

    print("=" * 72)
    print("  Ingest · Inside Airbnb")
    print("=" * 72)

    failed = 0
    for dest in dests:
        print(f"\n  ▸ {dest['city_name']}  ({dest['airbnb_path']})")
        if args.discover:
            snaps = discover(dest)
            print(f"    → update common/destinations.py: \"airbnb_snapshots\": {snaps}")
            continue

        newest = latest_snapshot(dest)
        for snap in sorted(dest["airbnb_snapshots"]):
            print(f"   snapshot {snap}{'  (latest)' if snap == newest else ''}")
            names = ["listings.csv.gz", "calendar.csv.gz"]
            if snap == newest:
                names += ["reviews.csv.gz", "neighbourhoods.geojson"]
            for name in names:
                try:
                    fetch(url_for(dest, snap, name), ROOT / dest["city_id"] / snap / name)
                except Exception as exc:  # noqa: BLE001
                    print(f"\n    ✗ {name}: {exc}")
                    failed += 1

    print()
    if failed:
        print(f"  ✗ {failed} files failed. Retry the command; existing files will be skipped.")
        print("    Snapshot unavailable? Inside Airbnb retains recent snapshots only; run --discover.")
        return 1
    if not args.discover:
        print(f"  ✓ Done. Data stored at {ROOT}")
        print("    Next step:  ./scripts/run_job.sh profile")
    return 0


if __name__ == "__main__":
    sys.exit(main())
