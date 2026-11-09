"""BR7 benchmark run, ClickHouse engine (the alternative solution).

The same steps and the same rows as br7_bench_spark.py, as SQL on the imported
trips, run as plain Python without Spark (so prepare.py, which needs pyspark,
is not imported: its two limits come from br7_bench_quality.py). Where
ClickHouse differs from Spark, the SQL follows Spark:
  - New York -> UTC as Spark's to_utc_timestamp: a wall-clock time in the
    autumn overlap takes daylight time (the earlier offset), one in the spring
    gap is shifted forward by the hour;
  - a double in the event id is written as Spark's cast to string writes it
    (12.0 -> "12.0"; the stored values are 0.01 to 323,820.17, where Java's
    toString and ClickHouse's toString agree otherwise);
  - concat_ws skips nulls.
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import br7_bench_runs as runs
from br7_bench_quality import MAX_TRIP_MILES, MAX_TRIP_SECONDS
from br7_rules import LOCAL_TZ, MIN_TRAINING_HOURS, TOURIST_ZONES

DB = runs.DB


def utc(column):
    """Spark's to_utc_timestamp(column, 'America/New_York') for New York's two offsets."""
    edt = f"({column} + toIntervalHour(4))"
    est = f"({column} + toIntervalHour(5))"
    return (f"if(toYYYYMMDDhhmmss(toTimeZone({edt}, '{LOCAL_TZ}')) = toYYYYMMDDhhmmss({column}), "
            f"{edt}, {est})")


def floor_seconds(column):
    micros = f"toUnixTimestamp64Micro({column})"
    return f"(intDiv({micros}, 1000000) - ({micros} % 1000000 < 0))"


def spark_double(column):
    return f"if({column} = floor({column}), concat(toString(toInt64({column})), '.0'), toString({column}))"


def iso_utc(column):
    return f"formatDateTime({column}, '%Y-%m-%dT%H:%i:%SZ', 'UTC')"


QUALITY_SQL = f"""
    multiIf(pickup_local IS NULL OR dropoff_local IS NULL OR pickup_zone_id IS NULL OR dropoff_zone_id IS NULL,
                'missing_field',
            dropoff_local < m_start OR dropoff_local >= addMonths(m_start, 1), 'dropoff_outside_file_month',
            secs <= 0, 'dropoff_not_after_pickup',
            secs > {MAX_TRIP_SECONDS}, 'trip_longer_than_6h',
            trip_distance <= 0 OR trip_distance > {MAX_TRIP_MILES}, 'invalid_distance',
            total_amount <= 0, 'nonpositive_amount',
            'ok')"""
TRIP_KEY = "tuple(vendor_id, pickup_time, dropoff_time, pickup_zone_id, dropoff_zone_id, trip_distance, total_amount)"
EVENT_ID = ("lower(hex(SHA256(arrayStringConcat(arrayFilter(x -> isNotNull(x), ["
            f"{iso_utc('pickup_time')}, {iso_utc('dropoff_time')}, toString(pickup_zone_id), "
            f"toString(dropoff_zone_id), {spark_double('trip_distance')}, {spark_double('total_amount')}]), '|'))))")


def training_hours(train_months):
    """First training hour (UTC) and the number of hours, as prepare.py's sequence()."""
    tz = ZoneInfo(LOCAL_TZ)
    first = datetime.strptime(train_months[0], "%Y-%m")
    last = datetime.strptime(train_months[-1], "%Y-%m")
    end = last.replace(year=last.year + (last.month == 12), month=last.month % 12 + 1)
    start_utc = first.replace(tzinfo=tz).astimezone(timezone.utc)
    end_utc = end.replace(tzinfo=tz).astimezone(timezone.utc)
    return start_utc.replace(tzinfo=None), int((end_utc - start_utc) / timedelta(hours=1))


class SqlEngine:
    name = "clickhouse"

    def __init__(self, ctx):
        self.ctx = ctx

    def _rows(self, sql, params=None):
        return list(self.ctx.client.query(sql, parameters=params or {}, settings=self.ctx.query_settings)
                    .named_results())

    def _command(self, sql, params=None):
        self.ctx.client.command(sql, parameters=params or {}, settings=self.ctx.query_settings)

    def _labelled(self):
        return f"""
            SELECT *, {QUALITY_SQL} AS quality
            FROM (SELECT *, toDateTime64(concat(source_month, '-01 00:00:00'), 6, 'UTC') AS m_start,
                         {floor_seconds('dropoff_local')} - {floor_seconds('pickup_local')} AS secs
                  FROM {DB}.raw_trip WHERE {self.ctx.where})"""

    def _trips(self, months_where="1"):
        zones = ", ".join(str(z) for z in TOURIST_ZONES)
        return f"""
            SELECT source_month, vendor_id, pickup_zone_id, dropoff_zone_id, passenger_count, trip_distance,
                   total_amount, {utc('pickup_local')} AS pickup_time, {utc('dropoff_local')} AS dropoff_time
            FROM ({self._labelled()})
            WHERE dropoff_zone_id IN ({zones}) AND quality = 'ok' AND ({months_where})"""

    # --- step: quality ------------------------------------------------------
    def quality(self):
        rows = self._rows(f"SELECT source_month, quality, count() AS n FROM ({self._labelled()}) "
                          "GROUP BY source_month, quality")
        return {(r["source_month"], r["quality"]): int(r["n"]) for r in rows}

    # --- step: clean --------------------------------------------------------
    def clean(self):
        # A trip key holds the drop-off time, which lies in its file's month (label ok),
        # so counting distinct keys per month is prepare.py's dropDuplicates per month.
        rows = self._rows(f"SELECT source_month, count() AS ok_trips, uniqExact({TRIP_KEY}) AS trips "
                          f"FROM ({self._trips()}) GROUP BY source_month")
        return {r["source_month"]: (int(r["ok_trips"]), int(r["trips"])) for r in rows}

    # --- step: baseline -----------------------------------------------------
    def baseline(self, run_id):
        months = ", ".join(f"'{m}'" for m in self.ctx.train_months)
        start, hours = training_hours(self.ctx.train_months)
        zones = ", ".join(str(z) for z in TOURIST_ZONES)
        local = f"toTimeZone(hour_start, '{LOCAL_TZ}')"
        self._command(f"""
            INSERT INTO {DB}.run_baseline
            SELECT {{run_id:UUID}}, zone_id, (toDayOfWeek({local}) - 1) * 24 + toHour({local}) AS how,
                   round(avg(arrivals), 4), round(quantileExactInclusive(0.9)(arrivals), 4), count() AS n
            FROM (
                SELECT g.zone_id AS zone_id, g.hour_start AS hour_start, c.arrivals AS arrivals
                FROM (SELECT toInt32(z) AS zone_id, toDateTime({{start:String}}, 'UTC') + toIntervalHour(number) AS hour_start
                      FROM numbers({hours}) AS h CROSS JOIN (SELECT arrayJoin([{zones}]) AS z) AS zz) AS g
                LEFT JOIN (
                    -- A key holds the drop-off zone and time: distinct keys per zone and hour
                    -- are the deduplicated trips.
                    SELECT assumeNotNull(dropoff_zone_id) AS zone_id,
                           toDateTime(toStartOfHour(assumeNotNull(dropoff_time)), 'UTC') AS hour_start,
                           uniqExact({TRIP_KEY}) AS arrivals
                    FROM ({self._trips(f"source_month IN ({months})")})
                    GROUP BY zone_id, hour_start
                ) AS c ON c.zone_id = g.zone_id AND c.hour_start = g.hour_start
            )
            GROUP BY zone_id, how
            HAVING n >= {MIN_TRAINING_HOURS}""", {"run_id": run_id, "start": start.strftime("%Y-%m-%d %H:%M:%S")})
        return int(self._rows(f"SELECT count() AS n FROM {DB}.run_baseline WHERE run_id = {{run_id:UUID}}",
                              {"run_id": run_id})[0]["n"])

    # --- step: replay -------------------------------------------------------
    def replay(self, run_id):
        self._command(f"""
            INSERT INTO {DB}.run_replay (run_id, event_id, dropoff_zone_id, pickup_zone_id, dropoff_time,
                                         passenger_count)
            SELECT {{run_id:UUID}}, {EVENT_ID}, assumeNotNull(dropoff_zone_id), assumeNotNull(pickup_zone_id),
                   toDateTime(assumeNotNull(dropoff_time), 'UTC'),
                   -- dropDuplicates keeps any one row of a group: any() here.
                   coalesce(any(passenger_count), 0)
            FROM ({self._trips(f"source_month = '{self.ctx.replay_month}'")})
            GROUP BY vendor_id, pickup_time, dropoff_time, pickup_zone_id, dropoff_zone_id, trip_distance,
                     total_amount""", {"run_id": run_id})
        counts = {int(r["z"]): int(r["n"]) for r in self._rows(
            f"SELECT dropoff_zone_id AS z, count() AS n FROM {DB}.run_replay WHERE run_id = {{run_id:UUID}} "
            "GROUP BY z", {"run_id": run_id})}
        lookup = {int(r["zone_id"]): r for r in self._rows(f"SELECT zone_id, zone_name, borough FROM {DB}.zone")}
        missing = [z for z in TOURIST_ZONES if z not in lookup or lookup[z]["zone_name"] is None]
        if missing:
            raise ValueError(f"Tourist zones missing from taxi_zone_lookup.csv: {missing}")
        self.zone_rows = [(z, lookup[z]["zone_name"], lookup[z]["borough"] or "", TOURIST_ZONES[z],
                           counts.get(z, 0)) for z in sorted(TOURIST_ZONES)]
        return counts

    # --- step: write --------------------------------------------------------
    def write(self, run_id, quality, months):
        runs.write_small(self.ctx.client, run_id, quality, months, self.zone_rows)

    def close(self):
        pass
