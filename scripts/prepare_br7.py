"""Download the public Orléans hourly arrivals export for the BR7 demo."""

import argparse
import csv
import os
import tempfile
import urllib.request
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "br7" / "frequentation_par_heure.csv"
SOURCE_URL = (
    "https://data.orleans-metropole.fr/api/explore/v2.1/catalog/datasets/"
    "frequentation-par-heure/exports/csv?delimiter=%2C"
)
REQUIRED_COLUMNS = {"entrees", "site", "horodatage"}


def validate(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not REQUIRED_COLUMNS.issubset(reader.fieldnames or []):
            raise ValueError(f"Unexpected Orléans CSV columns: {reader.fieldnames}")
        count = sum(1 for row in reader if row.get("site") and row.get("horodatage"))
    if count == 0:
        raise ValueError("Orléans CSV has no usable records")
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Fetch a new snapshot")
    args = parser.parse_args()

    if DATA_PATH.exists() and not args.refresh:
        print(f"BR7 source already present: {DATA_PATH} ({validate(DATA_PATH)} rows)")
        return

    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "co5173-br7-demo/1.0"})
    temp_name = None
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            with tempfile.NamedTemporaryFile(dir=DATA_PATH.parent, suffix=".csv", delete=False) as output:
                temp_name = output.name
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
        count = validate(Path(temp_name))
        os.replace(temp_name, DATA_PATH)
        print(f"Downloaded {count} hourly rows to {DATA_PATH}")
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)


if __name__ == "__main__":
    main()
