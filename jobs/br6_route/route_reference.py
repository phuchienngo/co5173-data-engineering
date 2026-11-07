"""BR6 reference files: cities and the category -> place type grouping.

Pure Python (no pyspark), so the app, the ClickHouse engine and the import job
all read the files the same way.
"""

import csv
import hashlib

CITIES_FILE = "/opt/data/br6/cities.csv"
GROUPING_FILE = "/opt/data/br6/category_place_type.csv"
NOT_OFFERED = "Not offered"
PLACE_TYPES = ["Sights & culture", "Nature & outdoors", "Food & drink", "Shopping",
               "Entertainment & nightlife", NOT_OFFERED]
OFFERED_TYPES = PLACE_TYPES[:-1]


class ReferenceDataError(Exception):
    pass


def load_cities(path=CITIES_FILE):
    with open(path, encoding="utf-8") as f:
        return {row["city_code"]: row for row in csv.DictReader(f)}


def load_grouping(path=GROUPING_FILE):
    """{category_id: place type}. Unknown place types are an error."""
    with open(path, encoding="utf-8") as f:
        grouping = {row["category_id"]: row["place_type"].strip() for row in csv.DictReader(f)}
    unknown = sorted({t for t in grouping.values() if t not in PLACE_TYPES})
    if unknown:
        raise ReferenceDataError(f"{path}: unknown place types {unknown}; allowed: {PLACE_TYPES}")
    return grouping


def file_hash(path=GROUPING_FILE):
    with open(path, "rb") as f:
        return hashlib.sha1(f.read()).hexdigest()


def place_type_rows(grouping, category_rows):
    """(dim_place_type rows, category_place_type rows, categories not grouped)."""
    types = [{"place_type_key": key, "place_type_name": name, "is_offered": int(name != NOT_OFFERED),
              "sort_order": key} for key, name in enumerate(PLACE_TYPES, start=1)]
    type_key = {t["place_type_name"]: t["place_type_key"] for t in types}
    mapping, missing = [], []
    for c in category_rows:
        place_type = grouping.get(c["category_id"])
        if place_type is None:
            missing.append(c["category_name"])
            place_type = NOT_OFFERED
        mapping.append({"category_key": c["category_key"], "place_type_key": type_key[place_type]})
    return types, mapping, missing
