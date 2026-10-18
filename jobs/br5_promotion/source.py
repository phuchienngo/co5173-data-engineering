"""Reads hotel_bookings.csv into typed columns without losing any value."""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOURCE_FILE = "/opt/data/br5/hotel_bookings.csv"
NULL_TOKENS = ("NULL", "NA")

# (column, type, nullable). Types map to the ClickHouse columns of
# br5.raw_booking; integer ranges are checked before writing.
COLUMNS = [
    ("hotel", "str", False),
    ("is_canceled", "u8", False),
    ("lead_time", "u16", False),
    ("arrival_date_year", "u16", False),
    ("arrival_date_month", "str", False),
    ("arrival_date_week_number", "u8", False),
    ("arrival_date_day_of_month", "u8", False),
    ("stays_in_weekend_nights", "u8", False),
    ("stays_in_week_nights", "u8", False),
    ("adults", "u8", False),
    ("children", "u8", True),
    ("babies", "u8", False),
    ("meal", "str", False),
    ("country", "str", True),
    ("market_segment", "str", False),
    ("distribution_channel", "str", False),
    ("is_repeated_guest", "u8", False),
    ("previous_cancellations", "u8", False),
    ("previous_bookings_not_canceled", "u8", False),
    ("reserved_room_type", "str", False),
    ("assigned_room_type", "str", False),
    ("booking_changes", "u8", False),
    ("deposit_type", "str", False),
    ("agent", "u16", True),
    ("company", "u16", True),
    ("days_in_waiting_list", "u16", False),
    ("customer_type", "str", False),
    ("adr", "f64", False),
    ("required_car_parking_spaces", "u8", False),
    ("total_of_special_requests", "u8", False),
    ("reservation_status", "str", False),
    ("reservation_status_date", "date", False),
]
COLUMN_NAMES = [name for name, _, _ in COLUMNS]
INT_MAX = {"u8": 255, "u16": 65535}

TEXT_SCHEMA = StructType([StructField(name, StringType()) for name in COLUMN_NAMES])


class SourceError(Exception):
    pass


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


def read_text(spark, path):
    # FAILFAST: a row with the wrong number of fields stops the import instead
    # of being silently padded or cut.
    df = spark.read.csv(path, header=True, schema=TEXT_SCHEMA, mode="FAILFAST")
    return df.select(
        [F.when(F.col(c).isin(*NULL_TOKENS), None).otherwise(F.col(c)).alias(c) for c in COLUMN_NAMES]
    )


def _typed(name, kind):
    col = F.col(name)
    if kind == "str":
        return col
    if kind in INT_MAX:
        return col.cast("int")
    if kind == "f64":
        return col.cast("double")
    return F.to_date(col, "yyyy-MM-dd")


def typed_columns():
    return [_typed(name, kind).alias(name) for name, kind, _ in COLUMNS]


def problem_counts(text):
    # Counts, per column, the values that would be lost or wrong once typed:
    # a value that doesn't parse (the shared session has ANSI off, so a bad
    # cast returns null instead of failing), an integer outside its ClickHouse
    # type, or an empty value in a column that can't be empty.
    checks = []
    for name, kind, nullable in COLUMNS:
        raw, typed = F.col(name), _typed(name, kind)
        bad = raw.isNotNull() & typed.isNull()
        if kind in INT_MAX:
            bad = bad | (typed < 0) | (typed > INT_MAX[kind])
        if not nullable:
            bad = bad | raw.isNull()
        checks.append(F.sum(F.when(bad, 1).otherwise(0)).alias(name))
    row = text.agg(*checks).first()
    return {name: row[name] for name in COLUMN_NAMES if row[name]}
