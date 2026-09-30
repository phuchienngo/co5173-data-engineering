"""Shared flight time conversion, event creation, and scoring."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from common.config import ALERT_LEVELS, settings

# ══════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════

def _local_ts(date_col: str, hhmm_col: str) -> Column:
    """ local ts."""
    hh = (F.col(hhmm_col) / 100).cast("int")
    mm = F.col(hhmm_col) % 100
    secs = F.unix_timestamp(F.col(date_col).cast("timestamp")) + hh * 3600 + mm * 60
    return F.timestamp_seconds(secs)


def _plus_minutes(ts: Column, minutes: Column) -> Column:
    return F.timestamp_seconds(F.unix_timestamp(ts) + (minutes * 60).cast("long"))


def add_utc_times(flights: DataFrame, airports: DataFrame) -> DataFrame:
    """Add utc times."""
    tz_o = airports.select(F.col("iata").alias("Origin"), F.col("timezone").alias("origin_tz"))
    tz_d = airports.select(F.col("iata").alias("Dest"), F.col("timezone").alias("dest_tz"))

    df = (flights
          .join(F.broadcast(tz_o), "Origin", "left")
          .join(F.broadcast(tz_d), "Dest", "left"))

    sched_dep_local = _local_ts("FlightDate", "CRSDepTime")
    df = df.withColumn("sched_dep_utc", F.to_utc_timestamp(sched_dep_local, F.col("origin_tz")))
    df = (df
          .withColumn("dep_utc", F.when(F.col("DepDelay").isNotNull(),
                                        _plus_minutes(F.col("sched_dep_utc"), F.col("DepDelay"))))
          .withColumn("sched_arr_utc", _plus_minutes(F.col("sched_dep_utc"), F.col("CRSElapsedTime")))
          .withColumn("arr_utc", F.when(F.col("ArrDelay").isNotNull(),
                                        _plus_minutes(F.col("sched_arr_utc"), F.col("ArrDelay")))))
    return df


# ══════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════

EVENT_COLUMNS = [
    "event_id", "flight_key", "airport", "city_id", "event_type",
    "event_ts", "scheduled_ts", "delay_min", "is_delayed",
    "carrier", "flight_number", "origin", "dest", "year_month",
]


def build_events(flights_utc: DataFrame, monitored: dict[str, str]) -> DataFrame:
    """Build events."""
    city_map = F.create_map(*[x for kv in monitored.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    base = flights_utc.withColumn(
        "flight_key",
        F.concat_ws("|", F.date_format("FlightDate", "yyyy-MM-dd"), "Reporting_Airline",
                    "Flight_Number_Reporting_Airline", "Origin", "Dest",
                    F.lpad(F.col("CRSDepTime").cast("string"), 4, "0")))

    common = [
        "flight_key",
        F.col("Reporting_Airline").alias("carrier"),
        F.col("Flight_Number_Reporting_Airline").alias("flight_number"),
        F.col("Origin").alias("origin"),
        F.col("Dest").alias("dest"),
        F.date_format("FlightDate", "yyyy-MM").alias("year_month"),
    ]

    arrivals = (base.filter(F.col("Dest").isin(list(monitored)))
                .select(
                    *common,
                    F.col("Dest").alias("airport"),
                    F.when(F.col("Cancelled") == 1, "cancelled_arrival")
                     .when(F.col("Diverted") == 1, "diverted_arrival")
                     .otherwise("arrival").alias("event_type"),
                    F.when((F.col("Cancelled") == 1) | (F.col("Diverted") == 1), F.col("sched_arr_utc"))
                     .otherwise(F.col("arr_utc")).alias("event_ts"),
                    F.col("sched_arr_utc").alias("scheduled_ts"),
                    F.col("ArrDelay").cast("int").alias("delay_min"),
                    F.coalesce(F.col("ArrDel15"), F.lit(0)).cast("int").alias("is_delayed")))

    departures = (base.filter(F.col("Origin").isin(list(monitored)))
                  .select(
                      *common,
                      F.col("Origin").alias("airport"),
                      F.when(F.col("Cancelled") == 1, "cancelled_departure")
                       .otherwise("departure").alias("event_type"),
                      F.when(F.col("Cancelled") == 1, F.col("sched_dep_utc"))
                       .otherwise(F.col("dep_utc")).alias("event_ts"),
                      F.col("sched_dep_utc").alias("scheduled_ts"),
                      F.col("DepDelay").cast("int").alias("delay_min"),
                      F.coalesce(F.col("DepDel15"), F.lit(0)).cast("int").alias("is_delayed")))

    events = arrivals.unionByName(departures)
    return (events
            .filter(F.col("event_ts").isNotNull())
            .withColumn("city_id", city_map[F.col("airport")])
            .withColumn("event_id", F.concat_ws("|", "flight_key", "event_type"))
            .select(*EVENT_COLUMNS))


# ══════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════

def aggregate_exprs() -> list[Column]:
    """Aggregate exprs."""
    is_arr = F.col("event_type") == "arrival"
    return [
        F.sum(is_arr.cast("int")).alias("arrivals"),
        F.sum((F.col("event_type") == "departure").cast("int")).alias("departures"),
        F.sum(F.when(is_arr, F.col("is_delayed")).otherwise(0)).alias("delayed_arrivals"),
        F.sum(F.col("event_type").startswith("cancelled").cast("int")).alias("cancelled"),
        F.sum((F.col("event_type") == "diverted_arrival").cast("int")).alias("diverted"),
        F.avg(F.when(is_arr, F.col("delay_min"))).alias("avg_arr_delay"),
    ]


def add_local_slot(df: DataFrame, ts_col: str = "window_ts") -> DataFrame:
    """Add local slot."""
    local = F.from_utc_timestamp(F.col(ts_col), F.col("timezone"))
    return (df
            .withColumn("window_local", local)
            .withColumn("dow_local", ((F.dayofweek(local) + 5) % 7 + 1).cast("int"))
            .withColumn("slot_local", (F.hour(local) * 4 + F.floor(F.minute(local) / 15)).cast("int")))


def score_windows(windows: DataFrame, baseline: DataFrame) -> DataFrame:
    """
    windows  : airport, window_ts, timezone, arrivals, departures, delayed_arrivals, …
    baseline : airport, dow_local, slot_local, baseline_p50, baseline_p95
    """
    s = settings
    df = add_local_slot(windows).join(
        F.broadcast(baseline), ["airport", "dow_local", "slot_local"], "left")

    capacity = F.greatest(F.coalesce(F.col("baseline_p95"), F.lit(0.0)), F.lit(float(s.baseline_floor)))
    df = (df
          .withColumn("capacity", capacity)
          .withColumn("load_pct", F.round(100.0 * F.col("arrivals") / capacity, 1))
          .withColumn("delayed_share",
                      F.round(F.col("delayed_arrivals") / F.greatest(F.col("arrivals"), F.lit(1)), 3)))

    flow_sev = (F.when(F.col("load_pct") >= s.flow_emergency, 4)
                 .when(F.col("load_pct") >= s.flow_critical, 3)
                 .when(F.col("load_pct") >= s.flow_warning, 2)
                 .when(F.col("load_pct") >= s.flow_watch, 1)
                 .otherwise(0))
    delay_sev = F.when((F.col("arrivals") >= s.delay_min_arrivals)
                       & (F.col("delayed_share") >= s.delay_share_watch), 1).otherwise(0)
    sev = F.greatest(flow_sev, delay_sev)

    level = F.lit(ALERT_LEVELS[0])
    for k, name in sorted(ALERT_LEVELS.items()):
        if k:
            level = F.when(sev == k, name).otherwise(level)

    return (df
            .withColumn("flow_severity", sev.cast("int"))
            .withColumn("alert_level", level)
            .withColumn("baseline_p50", F.coalesce(F.col("baseline_p50"), F.lit(0.0)))
            .withColumn("baseline_p95", F.coalesce(F.col("baseline_p95"), F.lit(0.0))))


FLOW_FACT_COLUMNS = [
    "window_ts", "airport", "city_id", "window_local", "dow_local", "slot_local",
    "arrivals", "departures", "delayed_arrivals", "cancelled", "diverted", "avg_arr_delay",
    "baseline_p50", "baseline_p95", "capacity", "load_pct", "delayed_share",
    "flow_severity", "alert_level", "source",
]
