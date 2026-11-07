"""Builds the BR6 star schema (docs/plan/br6/plan-data-model.md).

Keys are small integers assigned by sorting the natural values, numbered from
1, so the same files always give the same keys. Places and visitors are
numbered per city and offset by the counts of the cities before, so no window
runs without PARTITION BY (which would move every row into one Spark task).
The small dimensions are built on the driver as plain rows.
"""

import calendar
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

# The reference files are read by route_reference (no pyspark), re-exported here.
from route_reference import (  # noqa: F401
    CITIES_FILE, GROUPING_FILE, NOT_OFFERED, PLACE_TYPES, ReferenceDataError, load_cities, load_grouping,
    place_type_rows,
)


def date_key(col):
    return (F.year(col) * 10000 + F.month(col) * 100 + F.dayofmonth(col)).cast("int")


def city_rows(spark_cities, reference):
    """dim_city rows. spark_cities: [(city_code, country_code, centre_lat, centre_lon)]."""
    rows = []
    for key, (code, country, lat, lon) in enumerate(sorted(spark_cities), start=1):
        ref = reference[code]
        if ref["country_code"] != country:
            raise ReferenceDataError(f"{code}: country {country} in the data, {ref['country_code']} in cities.csv")
        rows.append({
            "city_key": key, "city_code": code, "city_name": ref["city_name"], "country_code": country,
            "country_name": ref["country_name"], "timezone": ref["timezone"],
            "centre_lat": float(lat), "centre_lon": float(lon),
        })
    return rows


def date_rows(first, last):
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


def with_city(typed, cities):
    """Adds city_key and the local date (the city's time zone) to the typed
    rows. cities: dim_city rows (15 at most, so a literal map, not a join)."""
    key_map = F.create_map(*[x for c in cities for x in (F.lit(c["city_code"]), F.lit(c["city_key"]))])
    tz_map = F.create_map(*[x for c in cities for x in (F.lit(c["city_code"]), F.lit(c["timezone"]))])
    local = F.to_date(F.from_utc_timestamp(F.col("timestamp"), tz_map[F.col("city")]))
    return typed.withColumn("city_key", key_map[F.col("city")].cast("int")).withColumn("local_date", local)


def _keyed_per_city(df, order_col, key_name, city_counts):
    """Numbers df's rows per city by order_col, offset by the counts of the
    cities before it (city_counts: {city_key: rows}, all cities)."""
    offsets, total = {}, 0
    for city_key in sorted(city_counts):
        offsets[city_key] = total
        total += city_counts[city_key]
    offset_map = F.create_map(*[x for k, v in offsets.items() for x in (F.lit(k), F.lit(v))])
    in_city = Window.partitionBy("city_key").orderBy(order_col)
    return df.withColumn(key_name, (offset_map[F.col("city_key")] + F.row_number().over(in_city)).cast("long"))


def build_places(rows, categories):
    """dim_place as a DataFrame. rows: typed rows with city_key; categories:
    dim_category rows."""
    category_key = F.create_map(*[x for c in categories for x in (F.lit(c["category_id"]), F.lit(c["category_key"]))])
    # Main category: most check-ins; a tie goes to the category of the latest check-in.
    per_category = rows.groupBy("city_key", "venue_id", "venue_category_id").agg(
        F.count("*").alias("checkins"),
        F.max(F.struct("timestamp", "source_row")).alias("latest"),
    )
    ranked = Window.partitionBy("city_key", "venue_id").orderBy(F.desc("checkins"), F.desc("latest"))
    main = (
        per_category
        .withColumn("rank", F.row_number().over(ranked))
        .withColumn("categories_seen", F.count("*").over(Window.partitionBy("city_key", "venue_id")))
        .where("rank = 1")
        .select("city_key", "venue_id", category_key[F.col("venue_category_id")].alias("main_category_key"),
                "categories_seen")
    )
    # A venue has one name, address and position (checked at import), so max() just picks it.
    attributes = rows.groupBy("city_key", "venue_id").agg(
        F.max("name").alias("name"), F.max("address").alias("address"),
        F.max("latitude").alias("latitude"), F.max("longitude").alias("longitude"),
    )
    places = attributes.join(main, ["city_key", "venue_id"])
    counts = {r["city_key"]: r["n"] for r in places.groupBy("city_key").agg(F.count("*").alias("n")).collect()}
    return _keyed_per_city(places, "venue_id", "place_key", counts).select(
        "place_key", "city_key", "venue_id", "name", "address", "latitude", "longitude",
        F.col("latitude").isNotNull().cast("int").alias("has_location"),
        "main_category_key", "categories_seen",
    )


def build_visitors(rows):
    visitors = rows.select("city_key", "user_id").distinct()
    counts = {r["city_key"]: r["n"] for r in visitors.groupBy("city_key").agg(F.count("*").alias("n")).collect()}
    return _keyed_per_city(visitors, "user_id", "visitor_key", counts).select("visitor_key", "city_key", "user_id")


def build_place_stats(rows, places):
    """place_stats: check-ins and different visitors per place, from the
    original rows (before the multiplier), so the Route Finder needs no pass
    over the fact table (docs/plan/br6/plan-pipeline.md, F4)."""
    per_venue = rows.groupBy("city_key", "venue_id").agg(
        F.count("*").alias("checkins"), F.countDistinct("user_id").alias("visitors"))
    return per_venue.join(places.select("city_key", "venue_id", "place_key"), ["city_key", "venue_id"]).select(
        "city_key", "place_key", "checkins", "visitors")


def small_dimensions(rows, cities, grouping):
    """The dimensions small enough to build on the driver: {table: rows} and
    the categories that are not grouped."""
    categories = [
        {"category_key": key, "category_id": cid, "category_name": name}
        for key, (cid, name) in enumerate(
            sorted(tuple(r) for r in rows.select("venue_category_id", "venue_category").distinct().collect()),
            start=1)
    ]
    areas = rows.groupBy("city_key", "venue_city").agg(
        F.max("venue_city_latitude").alias("lat"), F.max("venue_city_longitude").alias("lon")).collect()
    area_rows = [
        {"area_key": key, "city_key": city_key, "area_name": name, "area_lat": lat, "area_lon": lon}
        for key, (city_key, name, lat, lon) in enumerate(sorted(tuple(r) for r in areas), start=1)
    ]
    bounds = rows.agg(F.min("local_date").alias("first"), F.max("local_date").alias("last")).first()
    types, mapping, missing = place_type_rows(grouping, categories)
    return {
        "dim_city": cities,
        "dim_category": categories,
        "dim_place_type": types,
        "category_place_type": mapping,
        "dim_area": area_rows,
        "dim_date": date_rows(bounds["first"], bounds["last"]),
    }, missing


def build_fact(spark, rows, places, visitors, dims, multiplier):
    """rows: typed rows with city_key and local_date (not multiplied). The
    joins run once on the original rows; the copies are added after them."""

    def small(name, columns, rename=None):
        df = spark.createDataFrame(dims[name]).select(*columns)
        for old, new in (rename or {}).items():
            df = df.withColumnRenamed(old, new)
        return F.broadcast(df)

    category = small("dim_category", ["category_key", "category_id"], {"category_id": "venue_category_id"})
    area = small("dim_area", ["area_key", "city_key", "area_name"], {"area_name": "venue_city"})
    joined = (
        rows.join(places.select("city_key", "venue_id", "place_key"), ["city_key", "venue_id"])
        .join(visitors, ["city_key", "user_id"])
        .join(category, "venue_category_id")
        .join(area, ["city_key", "venue_city"])
    )
    if multiplier > 1:
        copies = spark.range(multiplier).select(F.col("id").cast("int").alias("copy_no"))
        joined = joined.crossJoin(F.broadcast(copies))
    else:
        joined = joined.withColumn("copy_no", F.lit(0))
    return joined.select(
        "city_key", "place_key", "visitor_key", "category_key", "area_key",
        date_key(F.col("local_date")).alias("date_key"),
        F.col("timestamp").alias("checkin_time"),
        "trail_id", "import_id", "source_row", "copy_no",
    )
