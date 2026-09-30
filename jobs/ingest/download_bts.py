#!/usr/bin/env python3
"""Download and extract BTS monthly archives."""

from __future__ import annotations

import argparse
import sys
import zipfile
from datetime import date
from pathlib import Path

import requests

from common.config import settings

URL = ("https://transtats.bts.gov/PREZIP/"
       "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{y}_{m}.zip")

ROOT = Path(settings.raw_dir) / "bts"
ZIP_DIR = ROOT / "zip"
CSV_DIR = ROOT / "csv"

HEADERS = {"User-Agent": "Mozilla/5.0 (HCMUT data engineering assignment)"}


def month_iter(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m == 13:
            y, m = y + 1, 1


def shift_month(y: int, m: int, delta: int) -> tuple[int, int]:
    idx = y * 12 + (m - 1) + delta
    return idx // 12, idx % 12 + 1


def exists(y: int, m: int) -> bool:
    try:
        r = requests.head(URL.format(y=y, m=m), headers=HEADERS, timeout=30, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False


def find_latest() -> tuple[int, int]:
    """Find latest."""
    today = date.today()
    y, m = today.year, today.month
    for _ in range(8):
        if exists(y, m):
            return y, m
        y, m = shift_month(y, m, -1)
    raise RuntimeError("No published BTS month found in the past eight months; check network access.")


def download(y: int, m: int) -> Path:
    ZIP_DIR.mkdir(parents=True, exist_ok=True)
    dst = ZIP_DIR / f"ontime_{y}_{m:02d}.zip"
    if dst.exists() and dst.stat().st_size > 1_000_000:
        print(f"  ✓ already present  {dst.name}  ({dst.stat().st_size / 1e6:.1f} MB)")
        return dst

    tmp = dst.with_suffix(".part")
    with requests.get(URL.format(y=y, m=m), headers=HEADERS, stream=True, timeout=120) as r:
        if r.status_code == 404:
            raise FileNotFoundError(f"BTS has not published month {y}-{m:02d}")
        r.raise_for_status()
        total = 0
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                total += len(chunk)
                print(f"\r  ↓ {dst.name}  {total / 1e6:6.1f} MB", end="", flush=True)
    tmp.rename(dst)
    print()
    return dst


def extract(zip_path: Path, y: int, m: int) -> Path:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    dst = CSV_DIR / f"ontime_{y}_{m:02d}.csv"
    if dst.exists() and dst.stat().st_size > 10_000_000:
        print(f"  ✓ already extracted  {dst.name}")
        return dst
    with zipfile.ZipFile(zip_path) as z:
        member = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        tmp = dst.with_suffix(".part")
        with z.open(member) as src, open(tmp, "wb") as out:
            while True:
                buf = src.read(1 << 22)
                if not buf:
                    break
                out.write(buf)
        tmp.rename(dst)
    print(f"  ⇲ extracting  {dst.name}  ({dst.stat().st_size / 1e6:.0f} MB)")
    return dst


def parse_ym(s: str) -> tuple[int, int]:
    y, m = s.split("-")
    return int(y), int(m)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", type=int, default=12, help="number of months counting back from the latest")
    ap.add_argument("--from", dest="start", help="YYYY-MM")
    ap.add_argument("--to", dest="end", help="YYYY-MM (default: latest published month)")
    ap.add_argument("--latest", action="store_true", help="print latest month only")
    ap.add_argument("--no-extract", action="store_true", help="download ZIP only")
    args = ap.parse_args()

    print("=" * 72)
    print("  Ingest · BTS On-Time Performance")
    print("=" * 72)

    end = parse_ym(args.end) if args.end else find_latest()
    print(f"  Latest published month: {end[0]}-{end[1]:02d}")
    if args.latest:
        return 0

    start = parse_ym(args.start) if args.start else shift_month(*end, -(args.months - 1))
    months = list(month_iter(start, end))
    print(f"  Downloading {len(months)} months: {start[0]}-{start[1]:02d} → {end[0]}-{end[1]:02d}\n")

    failed = []
    for y, m in months:
        try:
            z = download(y, m)
            if not args.no_extract:
                extract(z, y, m)
        except Exception as exc:  # noqa: BLE001
            print(f"\n  ✗ {y}-{m:02d}: {exc}")
            failed.append(f"{y}-{m:02d}")

    print()
    print(f"  Zip:  {ZIP_DIR}")
    print(f"  CSV:  {CSV_DIR}")
    if failed:
        print(f"  ✗ Failed {len(failed)} months: {', '.join(failed)} — retry the command.")
        return 1
    print("  ✓ Xong. Next step:  ./scripts/run_job.sh br9_02")
    return 0


if __name__ == "__main__":
    sys.exit(main())
