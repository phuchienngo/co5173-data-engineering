"""BR7 benchmark run, Spark engine (the main pipeline).

The steps of prepare.py main(), unchanged in meaning, on the imported trips
instead of the parquet files (prepare.py's helpers and constants are imported,
the rest is copied line for line):
  quality   label every trip of the input months; counts per month and label
  clean     tourist zones, label ok, New York -> UTC, duplicates removed on the
            natural trip key, sha256 event id; counts per month before and after
  baseline  zero-filled hourly arrivals of the training months; mean, p90 and
            training hours per zone and New York hour of week (>= 8 hours)
  replay    the replay month's trips, counts per zone, zone names and landmarks
  write     br7.run_quality, run_month, run_baseline, run_replay, run_zone
The settings only change how (reader, quality label, cache, joins, writer),
never what.
"""

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

import br7_bench_runs as runs
from br7_bench_quality import LABELS, quality_label
from br7_rules import LOCAL_TZ, MIN_TRAINING_HOURS, TOURIST_ZONES
from clickhouse_io import connection_settings, get_client, write_partitions
from prepare import MAX_TRIP_MILES, MAX_TRIP_SECONDS, local_hour_of_week, month_bounds

TRIP_SCHEMA = StructType([
    StructField("source_month", StringType()), StructField("vendor_id", IntegerType()),
    StructField("pickup_local", TimestampType()), StructField("dropoff_local", TimestampType()),
    StructField("pickup_zone_id", IntegerType()), StructField("dropoff_zone_id", IntegerType()),
    StructField("passenger_count", IntegerType()), StructField("trip_distance", DoubleType()),
    StructField("total_amount", DoubleType()),
])
TRIP_SELECT = f"SELECT {', '.join(TRIP_SCHEMA.fieldNames())} FROM {runs.DB}.raw_trip"
# prepare.py's natural trip key.
KEY = ["vendor_id", "pickup_time", "dropoff_time", "pickup_zone_id",
       "dropoff_zone_id", "trip_distance", "total_amount"]
REPLAY_COLUMNS = ["event_id", "dropoff_zone_id", "pickup_zone_id", "dropoff_time", "passenger_count"]
BASELINE_COLUMNS = ["zone_id", "hour_of_week", "mean_arrivals", "p90_arrivals", "training_hours"]


def read_trips(spark, where, partitions, query_settings):
    """Each task streams its own slice (source_row % N = i) from ClickHouse as
    Arrow batches. Like clickhouse_read.read_parallel, but streamed: a slice of
    25 M trips would not fit in memory as one Arrow table. With one partition it
    is the single reader (one connection, one task)."""
    conn = {**connection_settings(), "autogenerate_session_id": False}
    settings = dict(query_settings or {})
    names = TRIP_SCHEMA.fieldNames()
    base = f"{TRIP_SELECT} WHERE ({where}) AND source_row % {int(partitions)} = "

    def read_slices(batches):
        import clickhouse_connect
        import pyarrow as pa

        client = clickhouse_connect.get_client(**conn)
        try:
            for batch in batches:
                for slice_no in batch.column(0).to_pylist():
                    with client.query_arrow_stream(base + str(slice_no), settings=settings) as stream:
                        for block in stream:
                            yield pa.RecordBatch.from_arrays([block.column(n) for n in names], names=names)
        finally:
            client.close()

    return spark.range(0, partitions, 1, partitions).mapInArrow(read_slices, TRIP_SCHEMA)


# --- quality label: prepare.py load_month()'s rule, three ways ---------------

def month_start(month_col):
    # prepare.py: F.lit(month_bounds(month)[0]).cast("timestamp"), per row here.
    return F.concat(month_col, F.lit("-01 00:00:00")).cast("timestamp")


def month_end(month_col):
    return F.add_months(F.concat(month_col, F.lit("-01")).cast("date"), 1).cast("timestamp")


def native_quality(c):
    seconds = c("dropoff_local").cast("long") - c("pickup_local").cast("long")
    return (
        F.when(c("pickup_local").isNull() | c("dropoff_local").isNull()
               | c("pickup_zone_id").isNull() | c("dropoff_zone_id").isNull(), "missing_field")
        .when((c("dropoff_local") < month_start(c("source_month")))
              | (c("dropoff_local") >= month_end(c("source_month"))), "dropoff_outside_file_month")
        .when(seconds <= 0, "dropoff_not_after_pickup")
        .when(seconds > MAX_TRIP_SECONDS, "trip_longer_than_6h")
        .when((c("trip_distance") <= 0) | (c("trip_distance") > MAX_TRIP_MILES), "invalid_distance")
        .when(c("total_amount") <= 0, "nonpositive_amount")
        .otherwise("ok")
    )


python_quality_udf = F.udf(quality_label, StringType())  # baseline: one Python call per trip


@F.pandas_udf(StringType())
def pandas_quality_udf(start: pd.Series, end: pd.Series, pickup: pd.Series, dropoff: pd.Series,
                       pickup_zone: pd.Series, dropoff_zone: pd.Series, distance: pd.Series,
                       amount: pd.Series) -> pd.Series:
    # Nullable longs arrive as float64 with NaN; epoch microseconds stay exact (< 2^53).
    seconds = np.floor(dropoff / 1e6) - np.floor(pickup / 1e6)
    conditions = [
        pickup.isna() | dropoff.isna() | pickup_zone.isna() | dropoff_zone.isna(),
        (dropoff < start) | (dropoff >= end),
        seconds <= 0,
        seconds > MAX_TRIP_SECONDS,
        (distance <= 0) | (distance > MAX_TRIP_MILES),  # NaN (null) compares False, as Spark's null
        amount <= 0,
    ]
    return pd.Series(np.select(conditions, LABELS, "ok"), index=start.index)


def udf_quality(udf):
    def label(c):
        micros = F.unix_micros
        return udf(micros(month_start(c("source_month"))), micros(month_end(c("source_month"))),
                   micros(c("pickup_local")), micros(c("dropoff_local")), c("pickup_zone_id"),
                   c("dropoff_zone_id"), c("trip_distance"), c("total_amount"))
    return label


QUALITY = {"native": native_quality, "python": udf_quality(python_quality_udf),
           "pandas": udf_quality(pandas_quality_udf)}


class SparkEngine:
    name = "spark"

    def __init__(self, spark, ctx):
        self.spark = spark
        self.ctx = ctx
        self.cached = []

    def _persist(self, df):
        pipeline = self.ctx.opts["pipeline"]
        if not pipeline["cache"]:
            return df
        df = df.persist(getattr(StorageLevel, pipeline["storage_level"]))
        self.cached.append(df)
        return df

    # --- step: quality ------------------------------------------------------
    def quality(self):
        pipeline = self.ctx.opts["pipeline"]
        partitions = pipeline["read_partitions"] if pipeline["reader"] == "parallel" else 1
        raw = read_trips(self.spark, self.ctx.where, partitions, self.ctx.query_settings)
        self.labelled = raw.withColumn("quality", QUALITY[pipeline["udf"]](F.col))
        # One pass over all rows for the quality report; not cached, as prepare.py.
        return {(r["source_month"], r["quality"]): r["count"]
                for r in self.labelled.groupBy("source_month", "quality").count().collect()}

    # --- step: clean --------------------------------------------------------
    def clean(self):
        zones = list(TOURIST_ZONES)
        trips = (
            self.labelled.filter(F.col("dropoff_zone_id").isin(zones))
            .filter(F.col("quality") == "ok")
            .withColumn("dropoff_time", F.to_utc_timestamp("dropoff_local", LOCAL_TZ))
            .withColumn("pickup_time", F.to_utc_timestamp("pickup_local", LOCAL_TZ))
        )
        before = {r["source_month"]: r["count"] for r in trips.groupBy("source_month").count().collect()}
        self.trips = self._persist(trips.dropDuplicates(KEY).withColumn(
            "event_id",
            F.sha2(F.concat_ws("|",
                               F.date_format("pickup_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                               F.date_format("dropoff_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                               "pickup_zone_id", "dropoff_zone_id", "trip_distance", "total_amount"), 256),
        ))
        after = {r["source_month"]: r["count"] for r in self.trips.groupBy("source_month").count().collect()}
        return {m: (n, after.get(m, 0)) for m, n in before.items()}

    # --- step: baseline -----------------------------------------------------
    def baseline(self):
        train_months = self.ctx.train_months
        zones = list(TOURIST_ZONES)
        train_start, _ = month_bounds(train_months[0])
        _, train_end = month_bounds(train_months[-1])
        hours = self.spark.sql(
            f"SELECT explode(sequence(to_utc_timestamp(timestamp'{train_start}', '{LOCAL_TZ}'), "
            f"to_utc_timestamp(timestamp'{train_end}', '{LOCAL_TZ}') - INTERVAL 1 HOUR, INTERVAL 1 HOUR)) AS hour_start"
        )
        zone_frame = self.spark.createDataFrame([(zone,) for zone in zones], ["zone_id"])
        grid = zone_frame.crossJoin(hours)
        counted = (
            self.trips.filter(F.col("source_month").isin(train_months))
            .groupBy(F.col("dropoff_zone_id").alias("zone_id"),
                     F.date_trunc("hour", "dropoff_time").alias("hour_start"))
            .agg(F.count("*").alias("arrivals"))
        )
        hourly = grid.join(counted, ["zone_id", "hour_start"], "left").fillna(0, ["arrivals"])
        self.baseline_rows = self._persist(
            hourly.withColumn("hour_of_week", local_hour_of_week(F.col("hour_start")))
            .groupBy("zone_id", "hour_of_week")
            .agg(
                F.round(F.avg("arrivals"), 4).alias("mean_arrivals"),
                F.round(F.expr("percentile(arrivals, 0.9)"), 4).alias("p90_arrivals"),
                F.count("*").alias("training_hours"),
            )
            .filter(F.col("training_hours") >= MIN_TRAINING_HOURS)
        )
        return self.baseline_rows.count()

    # --- step: replay -------------------------------------------------------
    def replay(self):
        replay_month = self.ctx.replay_month
        self.replay_rows = (
            self.trips.filter(F.col("source_month") == replay_month)
            .select(
                "event_id", "dropoff_zone_id", "pickup_zone_id",
                # prepare.py formats it to whole seconds; stored as DateTime.
                F.date_trunc("second", "dropoff_time").alias("dropoff_time"),
                F.coalesce("passenger_count", F.lit(0)).alias("passenger_count"),
            )
        )
        counts = {r["dropoff_zone_id"]: r["count"] for r in
                  self.trips.filter(F.col("source_month") == replay_month).groupBy("dropoff_zone_id").count().collect()}
        lookup = self.ctx.client.query(f"SELECT zone_id, zone_name, borough FROM {runs.DB}.zone",
                                       settings=self.ctx.query_settings).result_rows
        lookup_frame = self.spark.createDataFrame(lookup, "zone_id int, zone_name string, borough string")
        landmarks = self.spark.createDataFrame(list(TOURIST_ZONES.items()), ["zone_id", "landmark"])
        zone_dim = landmarks.join(lookup_frame, "zone_id", "left").select("zone_id", "zone_name", "borough", "landmark")
        dims = zone_dim.collect()
        missing = [r["zone_id"] for r in dims if r["zone_name"] is None]
        if missing:
            raise ValueError(f"Tourist zones missing from taxi_zone_lookup.csv: {missing}")
        self.zone_rows = [(int(r["zone_id"]), r["zone_name"], r["borough"] or "", r["landmark"],
                           int(counts.get(r["zone_id"], 0))) for r in sorted(dims, key=lambda r: r["zone_id"])]
        return counts

    # --- step: write --------------------------------------------------------
    def write(self, run_id, quality, months):
        pipeline = self.ctx.opts["pipeline"]
        for df, table, columns in ((self.replay_rows, "run_replay", REPLAY_COLUMNS),
                                   (self.baseline_rows, "run_baseline", BASELINE_COLUMNS)):
            out = df.select(F.lit(run_id).alias("run_id"), *columns)
            if pipeline["writer"] == "partitions":
                write_partitions(out.repartition(pipeline["write_partitions"]), table, database=runs.DB)
            else:
                get_client(database=runs.DB).insert_df(table, out.toPandas())  # baseline: through the driver
        runs.write_small(self.ctx.client, run_id, quality, months, self.zone_rows)

    def close(self):
        for df in self.cached:
            df.unpersist()

