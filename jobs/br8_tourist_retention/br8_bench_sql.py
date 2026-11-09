"""BR8 benchmark run, ClickHouse engine (the alternative solution).

The same steps and the same rows as br8_bench_spark.py, as SQL on the imported
tables, run as plain Python without Spark. The read step only builds the
queries; clean and match count, aggregate writes br8.run_visit and write
br8.run_place.
"""

import br8_bench_runs as runs
from br8_rules import ATTRACTION_PATTERN

DB = runs.DB

PLACES_SQL = f"""
    SELECT place_id,
           coalesce(nullIf(trimBoth(fsq_name), ''), nullIf(trimBoth(google_name), '')) AS place_name,
           categories, latitude, longitude
    FROM {DB}.place
    WHERE place_name IS NOT NULL"""


class SqlEngine:
    name = "clickhouse"

    def __init__(self, ctx):
        self.ctx = ctx
        self.params = {"pattern": ATTRACTION_PATTERN}

    def _rows(self, sql, params=None):
        return list(self.ctx.client.query(sql, parameters={**self.params, **(params or {})},
                                          settings=self.ctx.query_settings).named_results())

    def _command(self, sql, params=None):
        self.ctx.client.command(sql, parameters={**self.params, **(params or {})}, settings=self.ctx.query_settings)

    # --- step: read ---------------------------------------------------------
    def read(self):
        where = self.ctx.where
        self.raw_sql = f"SELECT * FROM {DB}.raw_checkin WHERE {where}"
        # A trail can be repeated across travel records: collapse exact check-ins first.
        # Aliases differ from the column names: ClickHouse would otherwise use an
        # alias (an aggregate) where the WHERE clause means the column.
        self.clean_sql = f"""
            SELECT user_id, pid AS place_id, vt AS visit_time, rid AS region_id, vs AS venue_schema
            FROM (
                SELECT user_id, assumeNotNull(place_id) AS pid, assumeNotNull(visit_time) AS vt,
                       any(region_id) AS rid, any(venue_schema) AS vs
                FROM (SELECT user_id, place_id, visit_time, region_id, venue_schema FROM {DB}.raw_checkin
                      WHERE ({where}) AND user_id > 0 AND place_id IS NOT NULL AND visit_time IS NOT NULL)
                GROUP BY user_id, pid, vt
            )"""
        self.flagged_sql = f"""
            SELECT c.user_id AS user_id, c.place_id AS place_id, c.visit_time AS visit_time,
                   c.region_id AS region_id, p.place_name AS place_name, p.categories AS categories,
                   p.latitude AS latitude, p.longitude AS longitude,
                   match(concat(ifNull(c.venue_schema, ''), ' ', arrayStringConcat(p.categories, ' ')),
                         {{pattern:String}}) AS is_attraction
            FROM ({self.clean_sql}) AS c
            INNER JOIN ({PLACES_SQL}) AS p ON p.place_id = c.place_id"""
        named = f"SELECT * FROM ({self.flagged_sql}) WHERE is_attraction"
        max_places = self.ctx.opts["rules"]["max_places"]
        if max_places:
            named = f"""
                SELECT * FROM ({named}) WHERE place_id IN (
                    SELECT place_id FROM ({named}) GROUP BY place_id
                    ORDER BY uniqExact(user_id) DESC, place_id LIMIT {int(max_places)})"""
        self.selected_sql = named
        return int(self._rows(f"SELECT count() AS n FROM {DB}.raw_checkin WHERE {where}")[0]["n"])

    # --- step: clean --------------------------------------------------------
    def clean(self):
        row = self._rows(f"""
            SELECT countIf(user_id > 0 AND place_id IS NOT NULL AND visit_time IS NOT NULL) AS valid,
                   count() AS total,
                   (SELECT count() FROM ({self.clean_sql})) AS used
            FROM {DB}.raw_checkin WHERE {self.ctx.where}""")[0]
        valid, used = int(row["valid"]), int(row["used"])
        return {"invalid": int(row["total"]) - valid, "duplicate": valid - used}, used

    # --- step: match --------------------------------------------------------
    def match(self):
        found = {bool(r["is_attraction"]): int(r["n"]) for r in self._rows(
            f"SELECT is_attraction, count() AS n FROM ({self.flagged_sql}) GROUP BY is_attraction")}
        named = found.get(True, 0)
        return named + found.get(False, 0), named

    # --- step: aggregate ----------------------------------------------------
    def aggregate(self, run_id):
        self._command(f"""
            INSERT INTO {DB}.run_visit (run_id, event_id, user_id, place_id, visit_date, visit_time, raw_checkins)
            SELECT {{run_id:UUID}},
                   lower(hex(SHA256(concat(toString(user_id), '|', place_id, '|', toString(d))))),
                   user_id, place_id, d, t, n
            FROM (
                SELECT user_id, place_id, toDate(visit_time, 'UTC') AS d, min(visit_time) AS t, count() AS n
                FROM ({self.selected_sql})
                GROUP BY user_id, place_id, d
            )""", {"run_id": run_id})
        return int(self._rows(f"SELECT count() AS n FROM {DB}.run_visit WHERE run_id = {{run_id:UUID}}",
                              {"run_id": run_id})[0]["n"])

    # --- step: write --------------------------------------------------------
    def write(self, run_id):
        self._command(f"""
            INSERT INTO {DB}.run_place
            SELECT {{run_id:UUID}}, d.place_id, d.place_name, d.category, d.rid,
                   coalesce(r.city_name, d.rid), coalesce(r.country_name, 'Unknown'),
                   d.latitude, d.longitude, s.visitors, s.returning_visitors, s.visits
            FROM (
                SELECT place_id, place_name, latitude, longitude, min(region_id) AS rid,
                       min(arrayStringConcat(categories, ' | ')) AS category
                FROM ({self.selected_sql})
                GROUP BY place_id, place_name, latitude, longitude
            ) AS d
            LEFT JOIN {DB}.region AS r ON r.region_id = d.rid
            INNER JOIN (
                SELECT place_id, count() AS visitors, countIf(days >= 2) AS returning_visitors,
                       sum(days) AS visits
                FROM (SELECT place_id, user_id, count() AS days FROM {DB}.run_visit
                      WHERE run_id = {{run_id:UUID}} GROUP BY place_id, user_id)
                GROUP BY place_id
            ) AS s ON s.place_id = d.place_id""", {"run_id": run_id})

    def close(self):
        pass
