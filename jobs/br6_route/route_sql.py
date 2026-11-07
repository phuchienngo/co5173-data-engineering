"""BR6 Route Finder, ClickHouse engine (the alternative solution, plan F7).

The same outputs as route_spark.py from SQL queries, run as plain Python
without Spark. Every place near the city is compared with the route (no grid);
distances use the same flat-map formula in the same order, so both engines give
the same list (spec C10). The queries run in the clean, match and visitor
steps; the read step only builds them.
"""

import route_rules as rules

DB = "br6"


def num(value):
    # repr() of a float reads back as exactly the same number.
    return repr(float(value))


class SqlEngine:
    name = "clickhouse"

    def __init__(self, ctx):
        self.ctx = ctx
        self.places_sql = None

    def _rows(self, sql):
        return self.ctx.client.query(sql, settings=self.ctx.query_settings).named_results()

    # --- step: read ---------------------------------------------------------
    def read(self):
        ctx, pipeline = self.ctx, self.ctx.opts["pipeline"]
        city, n, m = int(ctx.city_key), int(ctx.multiplier), ctx.flat
        if pipeline["place_stats"] == "import":
            stats = f"SELECT place_key, checkins, visitors FROM {DB}.place_stats WHERE city_key = {city}"
        else:
            # A copy counts as another visitor at the same place, so dividing by
            # the multiplier gives the original figures exactly.
            stats = (f"SELECT place_key, intDiv(count(), {n}) AS checkins, "
                     f"intDiv(uniqExact(visitor_key, copy_no), {n}) AS visitors "
                     f"FROM {DB}.fact_checkin WHERE city_key = {city} GROUP BY place_key")
        k = num(rules.KM_PER_DEGREE)
        self.places_sql = f"""
            SELECT p.place_key AS place_key, s.checkins AS checkins, s.visitors AS visitors,
                   g.place_type_key AS place_type_key, t.is_offered AS is_offered,
                   (assumeNotNull(p.longitude) - {num(m.lon0)}) * {num(m.cos0)} * {k} AS x,
                   (assumeNotNull(p.latitude) - {num(m.lat0)}) * {k} AS y,
                   multiIf(p.has_location = 0, 'no_location',
                           sqrt(x * x + y * y) > {num(ctx.opts['rules']['max_centre_km'])}, 'far_from_city',
                           t.is_offered = 0, 'not_offered', '') AS reason
            FROM {DB}.dim_place AS p
            INNER JOIN ({stats}) AS s ON s.place_key = p.place_key
            INNER JOIN {DB}.category_place_type AS g ON g.category_key = p.main_category_key
            INNER JOIN {DB}.dim_place_type AS t ON t.place_type_key = g.place_type_key
            WHERE p.city_key = {city}"""

    # --- step: clean --------------------------------------------------------
    def clean(self):
        rows = self._rows(f"SELECT reason, sum(checkins) AS n FROM ({self.places_sql}) GROUP BY reason")
        return {(r["reason"] or None): int(r["n"]) for r in rows}

    # --- step: match --------------------------------------------------------
    def match(self):
        ctx = self.ctx
        legs = ", ".join(
            f"({leg['leg']}, {num(leg['ax'])}, {num(leg['ay'])}, {num(leg['bx'])}, {num(leg['by'])}, "
            f"{num(leg['length'])}, {num(leg['start_km'])})" for leg in ctx.legs)
        limit = num(ctx.opts["rules"]["max_detour_km"] + rules.MATCH_SLACK_KM)
        rows = self._rows(f"""
            SELECT place_key, any(visitors) AS visitors, any(checkins) AS checkins,
                   any(place_type_key) AS place_type_key, any(is_offered) AS is_offered,
                   min(distance) AS detour,
                   argMin(tuple(leg_no, position), tuple(distance, leg_no)) AS best
            FROM (
                SELECT place_key, visitors, checkins, place_type_key, is_offered, l.1 AS leg_no,
                       l.4 - l.2 AS dx, l.5 - l.3 AS dy, dx * dx + dy * dy AS l2,
                       if(l2 = 0, 0., least(greatest(((x - l.2) * dx + (y - l.3) * dy) / l2, 0.), 1.)) AS t,
                       x - (l.2 + t * dx) AS ex, y - (l.3 + t * dy) AS ey,
                       sqrt(ex * ex + ey * ey) AS distance,
                       l.7 + t * l.6 AS position
                FROM ({self.places_sql}) ARRAY JOIN [{legs}] AS l
                WHERE reason IN ('', 'not_offered')
            )
            GROUP BY place_key
            HAVING detour <= {limit}""")
        names = ctx.type_names
        return [{"place_key": r["place_key"], "detour": r["detour"], "leg": r["best"][0], "position": r["best"][1],
                 "visitors": int(r["visitors"]), "checkins": int(r["checkins"]),
                 "place_type_key": r["place_type_key"], "place_type": names[r["place_type_key"]],
                 "is_offered": r["is_offered"]} for r in rows]

    # --- step: visitor ------------------------------------------------------
    def visitor(self):
        ctx = self.ctx
        rows = self._rows(f"""
            SELECT v.place_key AS place_key, p.place_type_key AS place_type_key, v.n AS n
            FROM (SELECT place_key, count() AS n FROM {DB}.fact_checkin
                  WHERE city_key = {int(ctx.city_key)} AND visitor_key = {int(ctx.visitor_key)} AND copy_no = 0
                  GROUP BY place_key) AS v
            INNER JOIN ({self.places_sql}) AS p ON p.place_key = v.place_key
            WHERE p.reason = ''""")
        return [(r["place_key"], r["place_type_key"], int(r["n"])) for r in rows]

    def close(self):
        pass
