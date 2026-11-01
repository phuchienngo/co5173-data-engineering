"""Download pinned NYC TLC yellow-taxi files and prepare BR7 with Spark.

Host side: standard library only (download + SHA-256 check).
Container side: jobs/br7_tourist_arrival_alerts/prepare.py via spark-submit.
"""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "br7"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

# Pinned inputs: TLC republishes files occasionally, so a checksum mismatch
# means the source changed and the run would no longer be reproducible.
TRAIN_MONTHS = tuple(f"{y}-{m:02d}" for y, m in [(2025, 9), (2025, 10), (2025, 11), (2025, 12)]
                     + [(2026, m) for m in range(1, 9)])  # 12 months: 2025-09 .. 2026-08
REPLAY_MONTH = "2026-09"  # latest month TLC had published on 2026-11-01
TRIP_SHA256 = {
    "2025-09": "cd39b9901f01d1fd752d45cbaec91ee5f68c4dddf9e261e938c1dbab404644c7",
    "2025-10": "e1f63caf331bc6230d9c6f757cb88e55fdc2bddde185fb414d2c947abdc773c3",
    "2025-11": "2cf2f3127f96d4d5dea30f8afe05119cf8e4ba57bd9e60b789d3f296fc8459ab",
    "2025-12": "eaba7ff61ebb6453fc6f673fda43d621380307f869b768b124992e8486c785e1",
    "2026-01": "8b3933fe6f0d7b6d8826613c0dd724edc680ff7c49e2bd4c7635c05102728637",
    "2026-02": "5352ca800f29c40a221ed5b0c6392c451fde7fcd236a73651bd5aea74713a26c",
    "2026-03": "d7af794a7ac06cbbb5afb4d1df9e86d84f2e878584de708ff2ffca4bd7d234d9",
    "2026-04": "e03b82d5722a6b21d4fbce011afd8837495202c0b5db77ff0d8d15dd80834ba8",
    "2026-05": "9aa5a1609e2bf07d9051b7d530de05b1019e12a560ecb2c59c137c8b3a8b6750",
    "2026-06": "f340c62a18885c56176433c9db28e97d0a8a14e6e49869276ad8afb20ebf088b",
    "2026-07": "3260bb629c2f334055f8a566cbb02c15476b15120c45d7ad0aae46fc4bb06612",
    "2026-08": "6683a025443afcbb28ac4f238fcbb6334767dc69fab4a16b1eb16a78dcd013cd",
    "2026-09": "32d8d883e3fb9c0d681f4d47b205afe0fcb2a4830739673b4dd3bafbac9b0086",
}
SOURCES = {
    "taxi_zone_lookup.csv": ("misc/taxi_zone_lookup.csv",
                             "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed"),
    **{f"yellow_tripdata_{month}.parquet": (f"trip-data/yellow_tripdata_{month}.parquet", sha)
       for month, sha in TRIP_SHA256.items()},
}
PREPARE_VERSION = 2  # bump when prepare.py rules change so cached output is rebuilt


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(name, relative, expected):
    destination = DATA / "raw" / name
    if destination.exists() and sha256(destination) == expected:
        print(f"Using cached {name}", flush=True)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(f"{BASE_URL}/{relative}", headers={"User-Agent": "co5173-br7/1.0"})
    temp_path = None
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as target:
                temp_path = Path(target.name)
                while chunk := response.read(1024 * 1024):
                    target.write(chunk)
        actual = sha256(temp_path)
        if actual != expected:
            raise ValueError(f"Checksum mismatch for {name}: {actual}")
        os.replace(temp_path, destination)
        print(f"Downloaded and verified {name}", flush=True)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


def cached_summary_matches():
    summary_path = DATA / "prepared" / "summary.json"
    if not summary_path.exists():
        return False
    summary = json.loads(summary_path.read_text())
    expected = {
        "prepare_version": PREPARE_VERSION,
        "train_months": list(TRAIN_MONTHS),
        "replay_month": REPLAY_MONTH,
        "source_sha256": {name: sha for name, (_, sha) in SOURCES.items()},
    }
    if any(summary.get(key) != value for key, value in expected.items()):
        return False
    prepared = DATA / "prepared"
    return all(list((prepared / part).glob("part-*.csv")) for part in ("replay", "baseline", "zones"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Rebuild prepared data from the pinned files")
    parser.add_argument("--download-only", action="store_true", help="Only download and verify source files")
    args = parser.parse_args()

    if not args.refresh and not args.download_only and cached_summary_matches():
        summary = (DATA / "prepared" / "summary.json").read_text()
        print(f"Using prepared BR7 data:\n{summary}")
        return
    for name, (relative, expected) in SOURCES.items():
        download(name, relative, expected)
    if args.download_only:
        return
    subprocess.run([
        "docker", "exec", "-e", "PYTHONPATH=/opt/jobs/common", "spark-local",
        "spark-submit", "--master", "local[2]", "--driver-memory", "2g",
        "--conf", "spark.sql.shuffle.partitions=8",
        "--py-files", "/opt/jobs/common/br7_rules.py",
        "/opt/jobs/br7_tourist_arrival_alerts/prepare.py",
        "--train-months", ",".join(TRAIN_MONTHS),
        "--replay-month", REPLAY_MONTH,
        "--prepare-version", str(PREPARE_VERSION),
        "--source-sha256", json.dumps({name: sha for name, (_, sha) in SOURCES.items()}),
    ], check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
