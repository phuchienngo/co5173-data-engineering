#!/usr/bin/env python3
"""Download OpenFlights airport reference data."""

from __future__ import annotations

import sys
from pathlib import Path

import requests

from common.config import settings

URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"


def main() -> int:
    dst = Path(settings.raw_dir) / "reference" / "airports.dat"
    dst.parent.mkdir(parents=True, exist_ok=True)
    print("=" * 72)
    print("  Ingest · OpenFlights airports")
    print("=" * 72)
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    dst.write_bytes(r.content)
    n = r.content.count(b"\n")
    print(f"  ✓ {n:,} airports → {dst}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
