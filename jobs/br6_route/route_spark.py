"""BR6 Route Finder, Spark engine (the main pipeline).

Reads the city's places with their statistics, flags the cleaning reason of
each place, and finds the places near the route (grid or every place; native,
pandas_udf or Python UDF distances). The scoring is in route_rules.py and
route_run.py drives the steps. Plan: docs/plan/br6/plan-pipeline.md, section 2.
"""

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

import route_rules as rules
from clickhouse_read import read_driver, read_parallel

DB = "br6"
DISTANCE = StructType([StructField("distance", DoubleType()), StructField("t", DoubleType())])
FACT_SCHEMA = StructType([StructField(c, IntegerType()) for c in ("place_key", "visitor_key", "copy_no")])
FACT_SELECT = ("SELECT CAST(place_key AS Int32) AS place_key, CAST(visitor_key AS Int32) AS visitor_key, "
               f"CAST(copy_no AS Int32) AS copy_no FROM {DB}.fact_checkin")


def frame(spark, ctx, sql):
    """A small query (a dimension, the place statistics) read on the driver."""
    return spark.createDataFrame(ctx.client.query_arrow(sql, settings=ctx.query_settings))


# --- The distance from a place (x, y) to a leg (ax, ay)-(bx, by) --------------
# Three implementations of route_rules.leg_distance with the same operations
# in the same order, so they return the same numbers (spec C9).

def native_distance(x, y, ax, ay, bx, by):
    dx = bx - ax
    dy = by - ay
    l2 = dx * dx + dy * dy
    t = F.when(l2 == 0, F.lit(0.0)).otherwise(
        F.least(F.greatest(((x - ax) * dx + (y - ay) * dy) / l2, F.lit(0.0)), F.lit(1.0)))
    ex = x - (ax + t * dx)
    ey = y - (ay + t * dy)
    return F.struct(F.sqrt(ex * ex + ey * ey).alias("distance"), t.alias("t"))


@F.udf(returnType=DISTANCE)
def python_distance(x, y, ax, ay, bx, by):
    # Baseline: Spark sends every (place, leg) row to Python one at a time.
    return rules.leg_distance(x, y, ax, ay, bx, by)


@F.pandas_udf(DISTANCE)
def pandas_distance(x: pd.Series, y: pd.Series, ax: pd.Series, ay: pd.Series, bx: pd.Series,
                    by: pd.Series) -> pd.DataFrame:
    dx = bx - ax
    dy = by - ay
    l2 = dx * dx + dy * dy
    with np.errstate(divide="ignore", invalid="ignore"):
        t = np.where(l2 == 0, 0.0, np.minimum(np.maximum(((x - ax) * dx + (y - ay) * dy) / l2, 0.0), 1.0))
    ex = x - (ax + t * dx)
    ey = y - (ay + t * dy)
    return pd.DataFrame({"distance": np.sqrt(ex * ex + ey * ey), "t": t})


DISTANCE_FUNCTIONS = {"native": native_distance, "python_udf": python_distance, "pandas_udf": pandas_distance}


class SparkEngine:
    name = "spark"

    def __init__(self, spark, ctx):
        self.spark = spark
        self.ctx = ctx
        self.checkins = None  # the city's fact rows (statistics every run)
        self.places = None    # one row per place with its statistics and cleaning reason
        self.cached = []

    def _persist(self, df):
        pipeline = self.ctx.opts["pipeline"]
        if not pipeline["cache"]:
            return df
        df = df.persist(getattr(StorageLevel, pipeline["storage_level"]))
        df.count()
        self.cached.append(df)
        return df

    # --- step: read ---------------------------------------------------------
    def read(self):
        ctx, pipeline = self.ctx, self.ctx.opts["pipeline"]
        city = f"city_key = {int(ctx.city_key)}"
        places = frame(self.spark, ctx, f"""
            SELECT CAST(place_key AS Int32) AS place_key, CAST(has_location AS Int32) AS has_location,
                   latitude, longitude, CAST(main_category_key AS Int32) AS category_key
            FROM {DB}.dim_place WHERE {city}""")
        grouping = frame(self.spark, ctx, f"""
            SELECT CAST(g.category_key AS Int32) AS category_key, CAST(t.place_type_key AS Int32) AS place_type_key,
                   CAST(t.is_offered AS Int32) AS is_offered
            FROM {DB}.category_place_type AS g INNER JOIN {DB}.dim_place_type AS t USING (place_type_key)""")

        if pipeline["place_stats"] == "import":
            stats = frame(self.spark, ctx, f"""
                SELECT CAST(place_key AS Int32) AS place_key, CAST(checkins AS Int64) AS checkins,
                       CAST(visitors AS Int64) AS visitors
                FROM {DB}.place_stats WHERE {city}""")
        else:
            if pipeline["reader"] == "parallel":
                checkins = read_parallel(self.spark, FACT_SELECT, city, "source_row", pipeline["read_partitions"],
                                         FACT_SCHEMA, ctx.query_settings)
            else:
                checkins = read_driver(self.spark, ctx.client, FACT_SELECT, city, FACT_SCHEMA, ctx.query_settings)
            # Read once here; the statistics and the visitor step then use the cache.
            self.checkins = self._persist(checkins)
            # A copy counts as another visitor at the same place, so dividing by
            # the multiplier gives the original figures exactly.
            n = ctx.multiplier
            stats = self.checkins.groupBy("place_key").agg(
                (F.count(F.lit(1)) / n).cast("long").alias("checkins"),
                (F.countDistinct("visitor_key", "copy_no") / n).cast("long").alias("visitors"),
            )

        m = ctx.flat
        x = (F.col("longitude") - F.lit(m.lon0)) * F.lit(m.cos0) * F.lit(rules.KM_PER_DEGREE)
        y = (F.col("latitude") - F.lit(m.lat0)) * F.lit(rules.KM_PER_DEGREE)
        flagged = (
            places.join(stats, "place_key")
            .join(F.broadcast(grouping), "category_key")
            .withColumn("x", x).withColumn("y", y)
        )
        reason = (
            F.when(F.col("has_location") == 0, "no_location")
            .when(F.sqrt(F.col("x") * F.col("x") + F.col("y") * F.col("y")) > ctx.opts["rules"]["max_centre_km"],
                  "far_from_city")
            .when(F.col("is_offered") == 0, "not_offered")
        )
        self.places = self._persist(flagged.select(
            "place_key", "x", "y", "place_type_key", "is_offered", "checkins", "visitors", reason.alias("reason")))
        print("[br6] plan of the place statistics:", flush=True)
        self.places.explain(mode="formatted")

    # --- step: clean --------------------------------------------------------
    def clean(self):
        """{reason or None: check-ins} over the city's places (spec 3.5, S1)."""
        rows = self.places.groupBy("reason").agg(F.sum("checkins").alias("n")).collect()
        return {r["reason"]: int(r["n"]) for r in rows}

    # --- step: match --------------------------------------------------------
    def match(self):
        """Places with a location within the maximum detour, any type: one
        dict per place with detour, leg, position and statistics."""
        ctx, pipeline = self.ctx, self.ctx.opts["pipeline"]
        max_detour = ctx.opts["rules"]["max_detour_km"]
        near_city = self.places.filter(F.col("reason").isNull() | (F.col("reason") == "not_offered"))
        if pipeline["matching"] == "grid":
            size = pipeline["cell_km"]
            cells = self.spark.createDataFrame(rules.route_cells(ctx.legs, max_detour, size), "cx int, cy int")
            near_city = (
                near_city.withColumn("cx", F.floor(F.col("x") / size).cast("int"))
                .withColumn("cy", F.floor(F.col("y") / size).cast("int"))
                .join(F.broadcast(cells), ["cx", "cy"])
            )
        legs = self.spark.createDataFrame(
            [(leg["leg"], leg["ax"], leg["ay"], leg["bx"], leg["by"], leg["length"], leg["start_km"]) for leg in ctx.legs],
            "leg int, ax double, ay double, bx double, by double, length double, start_km double")
        distance = DISTANCE_FUNCTIONS[pipeline["distance"]](
            F.col("x"), F.col("y"), F.col("ax"), F.col("ay"), F.col("bx"), F.col("by"))
        per_leg = near_city.crossJoin(F.broadcast(legs)).withColumn("d", distance)
        # The closest leg; on a tie the earliest one (the struct compares in field order).
        best = per_leg.groupBy("place_key").agg(F.min(F.struct(
            F.col("d.distance").alias("detour"), F.col("leg"),
            (F.col("start_km") + F.col("d.t") * F.col("length")).alias("position"),
            "visitors", "checkins", "place_type_key", "is_offered",
        )).alias("best")).select("place_key", "best.*")
        found = best.filter(F.col("detour") <= max_detour + rules.MATCH_SLACK_KM)
        print("[br6] plan of the route matching:", flush=True)
        found.explain(mode="formatted")
        names = ctx.type_names
        return [{**r.asDict(), "place_type": names[r["place_type_key"]]} for r in found.collect()]

    # --- step: visitor ------------------------------------------------------
    def visitor(self):
        """The chosen visitor's used check-ins per place: [(place_key,
        place_type_key, check-ins)] (copy 0 = the original visitor, S10)."""
        ctx = self.ctx
        if self.checkins is not None:
            visits = (self.checkins.filter((F.col("visitor_key") == ctx.visitor_key) & (F.col("copy_no") == 0))
                      .groupBy("place_key").agg(F.count(F.lit(1)).alias("n")))
        else:
            visits = frame(self.spark, ctx, f"""
                SELECT CAST(place_key AS Int32) AS place_key, CAST(count() AS Int64) AS n FROM {DB}.fact_checkin
                WHERE city_key = {int(ctx.city_key)} AND visitor_key = {int(ctx.visitor_key)} AND copy_no = 0
                GROUP BY place_key""")
        used = visits.join(self.places.filter(F.col("reason").isNull()), "place_key")
        return [(r["place_key"], r["place_type_key"], int(r["n"]))
                for r in used.select("place_key", "place_type_key", "n").collect()]

    def close(self):
        for df in self.cached:
            df.unpersist()
