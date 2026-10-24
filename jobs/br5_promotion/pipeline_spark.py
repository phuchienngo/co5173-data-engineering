"""BR5 analysis, Spark engine (the main pipeline).

Reads the needed fact columns from ClickHouse (filters pushed down), flags the
cleaning reason of every booking, then aggregates. The rules themselves are in
rules.py; analyze.py drives the steps. Plan: docs/plan/br5/plan-pipeline.md.
"""

import clickhouse_connect
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from clickhouse_io import connection_settings

# Read as signed types: Spark's Arrow conversion has no unsigned integers.
FACT_COLUMNS = [
    ("hotel_key", "Int32", IntegerType()),
    ("country_key", "Int32", IntegerType()),
    ("reserved_room_type_key", "Int32", IntegerType()),
    ("arrival_date_key", "Int32", IntegerType()),
    ("channel_key", "Int32", IntegerType()),
    ("is_canceled", "Int32", IntegerType()),
    ("stay_nights", "Int32", IntegerType()),
    ("adults", "Int32", IntegerType()),
    ("children", "Nullable(Int32)", IntegerType()),
    ("babies", "Int32", IntegerType()),
    ("adr", "Float64", DoubleType()),
    ("lead_time", "Int32", IntegerType()),
    ("is_duplicate", "Int32", IntegerType()),
]
SCHEMA = StructType([StructField(name, spark_type, True) for name, _, spark_type in FACT_COLUMNS])
SELECT = ", ".join(f"CAST({name} AS {ch_type}) AS {name}" for name, ch_type, _ in FACT_COLUMNS)
NAMES = [name for name, _, _ in FACT_COLUMNS]
REASONS = ["canceled", "zero_nights", "zero_guests", "duplicate"]


def fact_query(ctx, extra_where=""):
    return f"SELECT {SELECT} FROM br5.fact_booking WHERE {ctx.where}{extra_where}"


def read_parallel(spark, ctx, partitions):
    """Each Spark task reads its own slice (source_row % N) as Arrow batches,
    with its own ClickHouse client. Connection settings are taken here on the
    driver: the worker container has no CLICKHOUSE_* variables."""
    conn = {**connection_settings(), "autogenerate_session_id": False}
    query_settings = dict(ctx.query_settings)
    base = fact_query(ctx, f" AND source_row % {partitions} = ")

    def read_slices(batches):
        client = clickhouse_connect.get_client(**conn)
        try:
            for batch in batches:
                for slice_no in batch.column(0).to_pylist():
                    table = client.query_arrow(base + str(slice_no), settings=query_settings)
                    yield from table.select(NAMES).to_batches()
        finally:
            client.close()

    return spark.range(0, partitions, 1, partitions).mapInArrow(read_slices, SCHEMA)


def read_driver(spark, ctx):
    """Baseline: one query on the driver; the rows are then shipped to the
    executors from there."""
    table = ctx.client.query_arrow(fact_query(ctx), settings=ctx.query_settings)
    return spark.createDataFrame(table.select(NAMES))


def dim_frame(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


class SparkEngine:
    name = "spark"

    def __init__(self, spark, ctx):
        self.spark = spark
        self.ctx = ctx
        self.flagged = None
        self.cached = False

    # --- step: read ---------------------------------------------------------
    def read(self):
        ctx, pipeline = self.ctx, self.ctx.opts["pipeline"]
        if pipeline["reader"] == "parallel":
            source = read_parallel(self.spark, ctx, pipeline["read_partitions"])
        else:
            source = read_driver(self.spark, ctx)
        source = source.withColumn("unknown_country", (F.col("country_key") == 0).cast("int"))

        if pipeline["join"] == "early":
            # Baseline: every booking joined with the lookup tables first. The
            # broadcast threshold decides broadcast vs sort-merge join.
            # Salting (salt_buckets > 1): each booking gets a random bucket and
            # each lookup row is copied once per bucket, so a frequent key is
            # split over several join partitions. Every booking still matches
            # exactly one copy.
            d = ctx.dims
            buckets = pipeline["salt_buckets"]

            def lookup(rows, schema):
                if buckets == 1:
                    return dim_frame(self.spark, rows, schema)
                return dim_frame(self.spark, [(*r, s) for r in rows for s in range(buckets)], schema + ", salt int")

            rooms = lookup([(k, v) for k, v in d.rooms.items()], "reserved_room_type_key int, room string")
            countries = lookup([(k, v[0]) for k, v in d.countries.items()], "country_key int, country string")
            channels = lookup([(k, v) for k, v in d.channels.items()], "channel_key int, segment string")
            dates = lookup(d.date_months, "arrival_date_key int, month int")
            salt = []
            if buckets > 1:
                source = source.withColumn("salt", F.floor(F.rand() * buckets).cast("int"))
                salt = ["salt"]
            prepared = (
                source.join(rooms, ["reserved_room_type_key", *salt])
                .join(countries, ["country_key", *salt])
                .join(channels, ["channel_key", *salt])
                .join(dates, ["arrival_date_key", *salt])
            )
        else:
            # Integer keys only; names are attached to the few result rows.
            prepared = (
                source.withColumnRenamed("reserved_room_type_key", "room")
                .withColumnRenamed("country_key", "country")
                .withColumnRenamed("channel_key", "segment")
                .withColumn("month", (F.floor(F.col("arrival_date_key") / 100) % 100).cast("int"))
            )

        reason = (
            F.when(F.col("is_canceled") == 1, "canceled")
            .when(F.col("stay_nights") == 0, "zero_nights")
            .when(F.col("adults") + F.coalesce(F.col("children"), F.lit(0)) + F.col("babies") == 0, "zero_guests")
        )
        if ctx.opts["rules"]["duplicates"] == "remove":
            reason = reason.when(F.col("is_duplicate") == 1, "duplicate")
        self.flagged = prepared.select(
            "room", "country", "segment", "month", "unknown_country", "stay_nights", "lead_time", "adr",
            ((F.coalesce(F.col("children"), F.lit(0)) + F.col("babies")) > 0).cast("int").alias("with_children"),
            reason.alias("exclusion"),
        )
        if pipeline["cache"]:
            # Read once here; clean, aggregate and profile then use the cache.
            self.flagged = self.flagged.persist(getattr(StorageLevel, pipeline["storage_level"]))
            self.flagged.count()
            self.cached = True

    # --- step: clean --------------------------------------------------------
    def clean(self):
        excl = F.col("exclusion")
        used = excl.isNull()
        row = self.flagged.agg(
            F.count(F.lit(1)).alias("read"),
            *[F.sum((excl == r).cast("long")).alias(r) for r in REASONS],
            F.sum((used & (F.col("unknown_country") == 1)).cast("long")).alias("kept_unknown_country"),
            F.sum((used & (F.col("adr") <= 0)).cast("long")).alias("kept_no_price"),
        ).first().asDict()
        return {k: int(v or 0) for k, v in row.items()}

    @property
    def cleaned(self):
        return self.flagged.filter(F.col("exclusion").isNull())

    # --- step: aggregate ----------------------------------------------------
    def aggregate(self):
        group = self.ctx.group_columns
        totals = self.cleaned.groupBy(*group, "month").agg(
            F.sum("stay_nights").alias("room_nights"),
            F.count(F.lit(1)).alias("bookings"),
            F.sum(F.when(F.col("adr") > 0, F.col("adr"))).alias("adr_sum"),
            F.sum((F.col("adr") > 0).cast("long")).alias("adr_count"),
        )
        print("[br5] plan of the aggregate step:", flush=True)
        totals.explain(mode="formatted")
        return [self.ctx.target_row(row.asDict()) for row in totals.collect()]

    # --- step: profile ------------------------------------------------------
    def profile(self, seasons):
        """seasons: [(target, month, season)] for the kept targets. Returns
        rows per (target, season) and low-season bookings per segment."""
        ctx = self.ctx
        group = ctx.group_columns
        keyed = [(*ctx.target_keys(target), month, season) for target, month, season in seasons]
        schema = ", ".join([f"{c} {ctx.key_type}" for c in group] + ["month int", "season string"])
        season_df = self.spark.createDataFrame(keyed, schema)
        joined = self.cleaned.join(season_df, [*group, "month"])

        profile = joined.groupBy(*group, "season").agg(
            F.count(F.lit(1)).alias("bookings"),
            F.percentile(F.col("lead_time"), F.lit(0.5)).alias("median_lead"),
            F.sum("stay_nights").alias("nights"),
            F.sum("with_children").alias("family_bookings"),
            F.sum(F.when(F.col("adr") > 0, F.col("adr"))).alias("adr_sum"),
            F.sum((F.col("adr") > 0).cast("long")).alias("adr_count"),
        )
        print("[br5] plan of the profile step:", flush=True)
        profile.explain(mode="formatted")
        segments = (
            joined.filter(F.col("season") == "low")
            .groupBy(*group, "segment")
            .agg(F.count(F.lit(1)).alias("bookings"))
        )
        return (
            [ctx.target_row(row.asDict()) for row in profile.collect()],
            [ctx.target_row(row.asDict()) for row in segments.collect()],
        )

    def close(self):
        if self.cached:
            self.flagged.unpersist()
