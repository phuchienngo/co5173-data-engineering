"""BR8 benchmark run, Spark engine (the main pipeline).

The steps of scripts/prepare_br8.py build(), unchanged in meaning, on the
imported tables instead of the parquet files:
  clean      user > 0, place and time present; exact duplicate check-ins collapsed
  match      named places only (fsq name, else google name); br8_rules.ATTRACTION_PATTERN
             on venue schema + place categories
  aggregate  optional top-N places by distinct visitors; one visit per user/place/UTC day
             (first time, raw check-ins, sha256 event id); one row per place
  write      br8.run_visit and br8.run_place
The settings only change how (reader, attraction test, cache, joins, writer),
never what.
"""

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

import br8_bench_runs as runs
from br8_rules import ATTRACTION_PATTERN, is_attraction
from clickhouse_io import get_client, write_partitions
from clickhouse_read import read_driver, read_parallel

CHECKIN_SCHEMA = StructType([
    StructField("user_id", LongType()), StructField("region_id", StringType()),
    StructField("place_id", StringType()), StructField("visit_time", TimestampType()),
    StructField("venue_schema", StringType()),
])
CHECKIN_SELECT = f"SELECT user_id, region_id, place_id, visit_time, venue_schema FROM {runs.DB}.raw_checkin"
PLACE_SCHEMA = StructType([
    StructField("place_id", StringType()), StructField("fsq_name", StringType()),
    StructField("google_name", StringType()), StructField("categories", ArrayType(StringType())),
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
])
# Array elements as Nullable: Spark's ArrayType(StringType()) allows null elements.
PLACE_SELECT = (f"SELECT place_id, fsq_name, google_name, CAST(categories, 'Array(Nullable(String))') "
                f"AS categories, latitude, longitude FROM {runs.DB}.place")
VISIT_COLUMNS = ["event_id", "user_id", "place_id", "visit_date", "visit_time", "raw_checkins"]
PLACE_COLUMNS = ["place_id", "place_name", "category", "region_id", "city_name", "country_name", "latitude",
                 "longitude", "visitors", "returning_visitors", "visits"]
NULLABLE_FLOATS = {"latitude", "longitude"}


@F.udf(returnType=BooleanType())
def python_attraction(schema, categories):
    # Baseline: Spark sends every check-in to Python one at a time.
    return is_attraction(schema, categories)


@F.pandas_udf(BooleanType())
def pandas_attraction(schema: pd.Series, categories: pd.Series) -> pd.Series:
    text = schema.fillna("") + " " + categories.map(lambda c: " ".join(c) if c is not None else "")
    return text.str.contains(ATTRACTION_PATTERN, regex=True)


def native_attraction(schema, categories):
    # concat_ws skips a null schema, as is_attraction's "" does; the pattern has no anchors.
    return F.concat_ws(" ", schema, F.array_join(categories, " ")).rlike(ATTRACTION_PATTERN)


ATTRACTION_TESTS = {"native": native_attraction, "python": python_attraction, "pandas": pandas_attraction}


class SparkEngine:
    name = "spark"

    def __init__(self, spark, ctx):
        self.spark = spark
        self.ctx = ctx
        self.cached = []
        self.counts = {}

    def _persist(self, df):
        pipeline = self.ctx.opts["pipeline"]
        if not pipeline["cache"]:
            return df
        df = df.persist(getattr(StorageLevel, pipeline["storage_level"]))
        self.cached.append(df)
        return df

    def _read(self, select, where, schema):
        ctx, pipeline = self.ctx, self.ctx.opts["pipeline"]
        if pipeline["reader"] == "parallel":
            return read_parallel(self.spark, select, where, "source_row", pipeline["read_partitions"], schema,
                                 ctx.query_settings)
        return read_driver(self.spark, ctx.client, select, where, schema, ctx.query_settings)

    # --- step: read ---------------------------------------------------------
    def read(self):
        ctx = self.ctx
        self.checkins = self._persist(self._read(CHECKIN_SELECT, ctx.where, CHECKIN_SCHEMA))
        self.places_raw = self._persist(self._read(PLACE_SELECT, "1", PLACE_SCHEMA))
        regions = ctx.client.query(f"SELECT region_id, city_name, country_name FROM {runs.DB}.region",
                                   settings=ctx.query_settings).result_rows
        self.regions = self.spark.createDataFrame(regions, "region_id string, city_name string, country_name string")
        self.counts["read"] = self.checkins.count()
        self.counts["places_read"] = self.places_raw.count()
        return self.counts["read"]

    # --- step: clean --------------------------------------------------------
    def clean(self):
        valid = (F.col("user_id") > 0) & F.col("place_id").isNotNull() & F.col("visit_time").isNotNull()
        valid_rows = self.checkins.agg(F.sum(valid.cast("long")).alias("n")).first()["n"] or 0
        # A trail can be repeated across travel records: collapse exact check-ins first.
        self.clean_rows = self._persist(
            self.checkins.filter(valid).dropDuplicates(["user_id", "place_id", "visit_time"]))
        used = self.clean_rows.count()
        return {"invalid": self.counts["read"] - valid_rows, "duplicate": valid_rows - used}, used

    # --- step: match --------------------------------------------------------
    def match(self):
        places = self.places_raw.select(
            "place_id",
            F.coalesce(F.expr("nullif(trim(fsq_name), '')"), F.expr("nullif(trim(google_name), '')"))
            .alias("place_name"),
            F.coalesce(F.col("categories"), F.array().cast("array<string>")).alias("categories"),
            "latitude", "longitude",
        ).filter(F.col("place_name").isNotNull()).dropDuplicates(["place_id"])
        test = ATTRACTION_TESTS[self.ctx.opts["pipeline"]["udf"]]
        flagged = self._persist(
            # Sort-merge, as prepare_br8.py's plan on parquet: rows read through mapInArrow have no
            # size estimate, so Spark could otherwise broadcast all 687k places through the driver.
            self.clean_rows.join(places.hint("merge"), "place_id", "inner")
            .withColumn("is_attraction", test(F.col("venue_schema"), F.col("categories"))))
        found = {r["is_attraction"]: r["count"] for r in flagged.groupBy("is_attraction").count().collect()}
        self.named = flagged.filter(F.col("is_attraction")).drop("is_attraction")
        named = found.get(True, 0)
        return named + found.get(False, 0), named  # (check-ins at named places, at attractions)

    # --- step: aggregate ----------------------------------------------------
    def aggregate(self, run_id):
        max_places = self.ctx.opts["rules"]["max_places"]
        selected = self.named
        if max_places:
            top = (self.named.groupBy("place_id").agg(F.countDistinct("user_id").alias("users"))
                   .orderBy(F.desc("users"), "place_id").limit(max_places))
            selected = self.named.join(top.select("place_id"), "place_id", "inner")
        selected = self._persist(selected)
        visits = (
            selected.withColumn("visit_date", F.to_date("visit_time"))
            .groupBy("user_id", "place_id", "visit_date")
            .agg(F.min("visit_time").alias("visit_time"), F.count("*").cast("long").alias("raw_checkins"))
            .withColumn("event_id", F.sha2(F.concat_ws("|", "user_id", "place_id", "visit_date"), 256))
        )
        self.visits = self._persist(visits.select(F.lit(run_id).alias("run_id"), *VISIT_COLUMNS))
        per_user = self.visits.groupBy("place_id", "user_id").agg(F.count("*").alias("days"))
        stats = per_user.groupBy("place_id").agg(
            F.count("*").cast("long").alias("visitors"),
            F.sum(F.when(F.col("days") >= 2, 1).otherwise(0)).cast("long").alias("returning_visitors"),
            F.sum("days").cast("long").alias("visits"),
        )
        dims = selected.groupBy("place_id", "place_name", "latitude", "longitude").agg(
            F.min("region_id").alias("region_id"),
            F.min(F.array_join("categories", " | ")).alias("category"),
        ).join(self.regions, "region_id", "left")
        self.places = dims.join(stats, "place_id", "inner").select(
            F.lit(run_id).alias("run_id"), "place_id", "place_name", "category", "region_id",
            F.coalesce("city_name", "region_id").alias("city_name"),
            F.coalesce("country_name", F.lit("Unknown")).alias("country_name"),
            "latitude", "longitude", "visitors", "returning_visitors", "visits",
        )
        return self.visits.count()

    # --- step: write --------------------------------------------------------
    def write(self):
        pipeline = self.ctx.opts["pipeline"]
        for df, table in ((self.visits, "run_visit"), (self.places, "run_place")):
            if pipeline["writer"] == "partitions":
                write_partitions(df.repartition(pipeline["write_partitions"]), table, database=runs.DB)
                continue
            pdf = df.toPandas()  # baseline: every row through the driver
            for column in NULLABLE_FLOATS & set(pdf.columns):
                pdf[column] = pdf[column].astype(object).where(pdf[column].notna(), None)
            get_client(database=runs.DB).insert_df(table, pdf)

    def close(self):
        for df in self.cached:
            df.unpersist()
