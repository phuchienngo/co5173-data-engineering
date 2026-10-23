"""Reads the Massive-STEPS check-in files into typed columns without losing
any value (docs/plan/br6/plan-data-import.md, section 5)."""

import csv
import glob
import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

DATA_DIR = "/opt/data/br6"
SUFFIX = "_checkins.csv"

# (column, type, nullable). Types map to the ClickHouse columns of
# br6.raw_checkin; integer ranges are checked before writing.
COLUMNS = [
    ("trail_id", "str", False),
    ("user_id", "u32", False),
    ("venue_id", "u32", False),
    ("latitude", "f64", True),
    ("longitude", "f64", True),
    ("name", "str", True),
    ("address", "str", True),
    ("venue_category", "str", False),
    ("venue_category_id", "str", False),
    ("venue_category_id_code", "u16", False),
    ("venue_city", "str", False),
    ("venue_city_latitude", "f64", False),
    ("venue_city_longitude", "f64", False),
    ("venue_country", "str", False),
    ("timestamp", "ts", False),
]
COLUMN_NAMES = [name for name, _, _ in COLUMNS]
INT_MAX = {"u16": 65535, "u32": 4294967295}
RANGES = {"latitude": 90, "longitude": 180, "venue_city_latitude": 90, "venue_city_longitude": 180}

TEXT_SCHEMA = StructType([StructField(name, StringType()) for name in COLUMN_NAMES])


class SourceError(Exception):
    pass


def available_cities(data_dir=DATA_DIR):
    return sorted(os.path.basename(p)[:-len(SUFFIX)] for p in glob.glob(os.path.join(data_dir, "*" + SUFFIX)))


def city_files(cities, data_dir=DATA_DIR):
    """{city: path} for the chosen cities (all files when cities is empty)."""
    cities = cities or available_cities(data_dir)
    if not cities:
        raise SourceError(f"No *{SUFFIX} files in {data_dir}")
    files = {}
    for city in sorted(set(cities)):
        path = os.path.join(data_dir, city + SUFFIX)
        if not os.path.exists(path):
            raise SourceError(f"No file for city '{city}' ({path}). Available: {', '.join(available_cities(data_dir))}")
        files[city] = path
    return files


def check_file(path):
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        header = f.readline().strip()
    if header.startswith("version https://git-lfs"):
        raise SourceError(f"{path} is a Git LFS pointer, not the data. Run `git lfs pull` on the host.")
    names = header.split(",")
    if names != COLUMN_NAMES:
        missing = [n for n in COLUMN_NAMES if n not in names]
        extra = [n for n in names if n not in COLUMN_NAMES]
        raise SourceError(
            f"Unexpected header in {path}. Missing: {missing or 'none'}; unexpected: {extra or 'none'}"
            + ("" if missing or extra else "; columns are out of order")
        )


def file_profile(path):
    """(data rows, first data row, last data row) read with Python's csv
    module on the driver: the reference the Spark read is checked against.
    The files have no line breaks inside values, so one line is one row."""
    rows, first, last = 0, None, None
    with open(path, encoding="utf-8", newline="") as f:
        f.readline()
        for line in f:
            if first is None:
                first = line
            last = line
            rows += 1
    parse = (lambda line: next(csv.reader([line])) if line else None)
    return rows, parse(first), parse(last)


def read_text(spark, files):
    """Every column as text plus city and source_row. A quoted empty value and
    an empty one are both read as empty (null)."""
    # The files escape quotes by doubling them ("") and some names contain a
    # backslash, so the escape character must be the quote itself.
    df = spark.read.csv(list(files.values()), header=True, schema=TEXT_SCHEMA, mode="FAILFAST",
                        quote='"', escape='"', encoding="UTF-8")
    city = F.regexp_extract(F.input_file_name(), r"([^/]+)" + SUFFIX.replace(".", r"\.") + "$", 1)
    df = df.select(
        city.alias("city"),
        *[F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in COLUMN_NAMES],
        F.monotonically_increasing_id().alias("_order"),
    )
    # Row number inside its file: the ids grow with the file order within
    # each file's chunks, and the chunks of one file keep their order.
    in_file = Window.partitionBy("city").orderBy("_order")
    return df.withColumn("source_row", F.row_number().over(in_file)).drop("_order")


def _typed(name, kind):
    col = F.col(name)
    if kind == "str":
        return col
    if kind in INT_MAX:
        return col.cast("long")
    if kind == "f64":
        return col.cast("double")
    return F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss")  # the session time zone is UTC


def typed_columns():
    return [_typed(name, kind).alias(name) for name, kind, _ in COLUMNS]


def problem_counts(text):
    """Per column, the values that would be lost or wrong once typed: a value
    that doesn't parse (the shared session has ANSI off, so a bad cast returns
    null instead of failing), a number outside its range, or an empty value in
    a column that is never empty."""
    checks = []
    for name, kind, nullable in COLUMNS:
        raw, typed = F.col(name), _typed(name, kind)
        bad = raw.isNotNull() & typed.isNull()
        if kind in INT_MAX:
            bad = bad | (typed < 0) | (typed > INT_MAX[kind])
        if name in RANGES:
            bad = bad | (F.abs(typed) > RANGES[name])
        if not nullable:
            bad = bad | raw.isNull()
        checks.append(F.sum(F.when(bad, 1).otherwise(0)).alias(name))
    row = text.agg(*checks).first()
    return {name: row[name] for name in COLUMN_NAMES if row[name]}


def inconsistent_places(text):
    """Places (city, venue) with more than one name, address or position."""
    return (
        text.groupBy("city", "venue_id")
        .agg(F.countDistinct(F.struct("name", "address", "latitude", "longitude")).alias("versions"))
        .where("versions > 1")
        .count()
    )
