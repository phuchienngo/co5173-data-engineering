"""Builds the BR5 star schema (docs/plan/br5/plan-data-model.md).

Dimensions are tiny, so they are built on the driver as plain rows: that keeps
the key assignment simple and deterministic (natural values sorted, numbered
from 1, 0 = Unknown). The fact is built in Spark with broadcast joins.
"""

import csv
import calendar
from datetime import date, timedelta

from pyspark.sql import functions as F

COUNTRIES_FILE = "/opt/data/br5/countries.csv"
MONTHS = {name: number for number, name in enumerate(calendar.month_name) if name}


def month_number(col):
    mapping = F.create_map(*[x for name, number in MONTHS.items() for x in (F.lit(name), F.lit(number))])
    return mapping[col]


def with_dates(df):
    arrival = F.make_date(
        F.col("arrival_date_year"), month_number(F.col("arrival_date_month")), F.col("arrival_date_day_of_month")
    )
    return (
        df.withColumn("arrival_date", arrival)
        .withColumn("booking_date", F.date_sub(F.col("arrival_date"), F.col("lead_time")))
    )


def date_key(col):
    return (F.year(col) * 10000 + F.month(col) * 100 + F.dayofmonth(col)).cast("int")


def _numbered(values, columns):
    return [dict(zip(columns, (key, *value))) for key, value in enumerate(sorted(values), start=1)]


def _distinct(df, *columns):
    return [tuple(row) for row in df.select(*columns).distinct().collect()]


def load_country_names(path=COUNTRIES_FILE):
    with open(path, encoding="utf-8") as f:
        return {row["code"]: row["name"] for row in csv.DictReader(f)}


def date_rows(first, last):
    first = first.replace(day=1)
    last = date(last.year, last.month, calendar.monthrange(last.year, last.month)[1])
    rows = []
    day = first
    while day <= last:
        rows.append({
            "date_key": day.year * 10000 + day.month * 100 + day.day,
            "date": day,
            "year": day.year,
            "quarter": (day.month - 1) // 3 + 1,
            "month": day.month,
            "month_name": calendar.month_name[day.month],
            "iso_week": day.isocalendar().week,
            "day_of_month": day.day,
            "day_of_week": day.isoweekday(),
            "is_weekend": int(day.isoweekday() >= 6),
        })
        day += timedelta(days=1)
    return rows


def build_dimensions(base, country_names):
    """base: the typed rows (not multiplied) with arrival_date/booking_date.
    Returns {table: [row dicts]}."""
    bounds = base.agg(
        F.least(F.min("arrival_date"), F.min("booking_date"), F.min("reservation_status_date")).alias("first"),
        F.greatest(F.max("arrival_date"), F.max("booking_date"), F.max("reservation_status_date")).alias("last"),
    ).first()

    room_types = {code for (code,) in _distinct(base, "reserved_room_type")}
    room_types |= {code for (code,) in _distinct(base, "assigned_room_type")}
    countries = [(code,) for (code,) in _distinct(base, "country") if code is not None]

    country_rows = [{"country_key": 0, "country_code": "", "country_name": "Unknown"}]
    for row in _numbered(countries, ["country_key", "country_code"]):
        row["country_name"] = country_names.get(row["country_code"], row["country_code"])
        country_rows.append(row)

    return {
        "dim_date": date_rows(bounds["first"], bounds["last"]),
        "dim_hotel": _numbered(_distinct(base, "hotel"), ["hotel_key", "hotel_name"]),
        "dim_country": country_rows,
        "dim_room_type": _numbered([(c,) for c in room_types], ["room_type_key", "room_type_code"]),
        "dim_channel": _numbered(
            _distinct(base, "market_segment", "distribution_channel"),
            ["channel_key", "market_segment", "distribution_channel"],
        ),
        "dim_booking_profile": _numbered(
            _distinct(base, "meal", "deposit_type", "customer_type", "is_repeated_guest", "reservation_status"),
            ["profile_key", "meal", "deposit_type", "customer_type", "is_repeated_guest", "reservation_status"],
        ),
    }


def build_fact(spark, rows, dims):
    """rows: the (multiplied) typed rows with arrival_date/booking_date and
    lineage columns."""

    def dim(name, rename=None):
        df = spark.createDataFrame(dims[name])
        for old, new in (rename or {}).items():
            df = df.withColumnRenamed(old, new)
        return F.broadcast(df)

    hotel = dim("dim_hotel", {"hotel_name": "hotel"})
    country = dim("dim_country", {"country_code": "country"}).drop("country_name")
    reserved = dim("dim_room_type", {"room_type_key": "reserved_room_type_key", "room_type_code": "reserved_room_type"})
    assigned = dim("dim_room_type", {"room_type_key": "assigned_room_type_key", "room_type_code": "assigned_room_type"})
    channel = dim("dim_channel")
    profile = dim("dim_booking_profile")

    joined = (
        rows.join(hotel, "hotel")
        .join(country, "country", "left")
        .join(reserved, "reserved_room_type")
        .join(assigned, "assigned_room_type")
        .join(channel, ["market_segment", "distribution_channel"])
        .join(profile, ["meal", "deposit_type", "customer_type", "is_repeated_guest", "reservation_status"])
    )
    return joined.select(
        date_key(F.col("arrival_date")).alias("arrival_date_key"),
        date_key(F.col("booking_date")).alias("booking_date_key"),
        date_key(F.col("reservation_status_date")).alias("status_date_key"),
        "hotel_key",
        F.coalesce(F.col("country_key"), F.lit(0)).alias("country_key"),
        "reserved_room_type_key",
        "assigned_room_type_key",
        "channel_key",
        "profile_key",
        F.col("agent").alias("agent_id"),
        F.col("company").alias("company_id"),
        F.col("arrival_date_week_number").alias("arrival_week_number"),
        "is_canceled",
        "lead_time",
        F.col("stays_in_weekend_nights").alias("weekend_nights"),
        F.col("stays_in_week_nights").alias("week_nights"),
        (F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights")).alias("stay_nights"),
        "adults",
        "children",
        "babies",
        "adr",
        "previous_cancellations",
        "previous_bookings_not_canceled",
        "booking_changes",
        "days_in_waiting_list",
        "required_car_parking_spaces",
        "total_of_special_requests",
        "is_duplicate",
        "import_id",
        "source_row",
        "copy_no",
    )
