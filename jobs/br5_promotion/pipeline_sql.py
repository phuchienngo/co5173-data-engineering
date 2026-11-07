"""BR5 analysis, ClickHouse engine (the alternative solution).

The same outputs as pipeline_spark.py, computed by ClickHouse SQL on the star
schema, without a Spark application. Its results go through the same rules.py,
so both engines must give the same cards (spec-pipeline-config.md C13).
"""

MONTH = "intDiv(arrival_date_key, 100) % 100"
KEY_COLUMNS = {"room": "reserved_room_type_key", "country": "country_key"}


class SqlEngine:
    name = "clickhouse"

    def __init__(self, ctx):
        self.ctx = ctx
        remove = ctx.opts["rules"]["duplicates"] == "remove"
        self.exclusion = (
            "multiIf(is_canceled = 1, 'canceled', stay_nights = 0, 'zero_nights', "
            "adults + ifNull(children, 0) + babies = 0, 'zero_guests', "
            f"{int(remove)} AND is_duplicate = 1, 'duplicate', '')"
        )
        # The bookings of the selection with their key columns and cleaning reason.
        group = ", ".join(f"{KEY_COLUMNS[c]} AS {c}" for c in ctx.group_columns)
        self.cleaned = (
            f"SELECT {group + ', ' if group else ''}{MONTH} AS month, channel_key AS segment, stay_nights, lead_time, "
            "adr, ifNull(children, 0) + babies > 0 AS with_children "
            f"FROM br5.fact_booking WHERE {ctx.where} AND {self.exclusion} = ''"
        )

    def query(self, sql):
        return list(self.ctx.client.query(sql, settings=self.ctx.query_settings).named_results())

    def read(self):
        pass  # nothing to load: every later step is one query on the stored data

    def clean(self):
        reasons = ", ".join(f"countIf(exclusion = '{r}') AS {r}"
                            for r in ("canceled", "zero_nights", "zero_guests", "duplicate"))
        row = self.query(
            f"SELECT count() AS read, {reasons}, "
            "countIf(exclusion = '' AND country_key = 0) AS kept_unknown_country, "
            "countIf(exclusion = '' AND adr <= 0) AS kept_no_price "
            f"FROM (SELECT {self.exclusion} AS exclusion, country_key, adr "
            f"FROM br5.fact_booking WHERE {self.ctx.where})"
        )[0]
        return {k: int(v) for k, v in row.items()}

    def _group(self, *extra):
        return ", ".join([*self.ctx.group_columns, *extra])

    def aggregate(self):
        rows = self.query(
            f"SELECT {self._group('month')}, sum(stay_nights) AS room_nights, count() AS bookings, "
            "sumIf(adr, adr > 0) AS adr_sum, countIf(adr > 0) AS adr_count "
            f"FROM ({self.cleaned}) GROUP BY {self._group('month')}"
        )
        return [self.ctx.target_row(r) for r in rows]

    def profile(self, seasons):
        ctx = self.ctx
        group = ctx.group_columns
        values = ", ".join(
            "(" + ", ".join(str(v) for v in ctx.target_keys(target)) + (", " if group else "")
            + f"{int(month)}, '{season}')"
            for target, month, season in seasons
        )
        structure = ", ".join([f"{c} UInt32" for c in group] + ["month UInt32", "season String"])
        on = " AND ".join(f"b.{c} = s.{c}" for c in [*group, "month"])
        joined = (
            f"FROM ({self.cleaned}) AS b INNER JOIN (SELECT * FROM VALUES('{structure}', {values})) AS s ON {on}"
        )
        group_b = ", ".join(f"b.{c} AS {c}" for c in group)
        prefix = group_b + ", " if group else ""
        group_by = ", ".join([*group, "season"])
        profile = self.query(
            f"SELECT {prefix}s.season AS season, count() AS bookings, "
            "quantileExactInclusive(0.5)(b.lead_time) AS median_lead, sum(b.stay_nights) AS nights, "
            "countIf(b.with_children) AS family_bookings, sumIf(b.adr, b.adr > 0) AS adr_sum, "
            f"countIf(b.adr > 0) AS adr_count {joined} GROUP BY {group_by}"
        )
        segments = self.query(
            f"SELECT {prefix}b.segment AS segment, count() AS bookings {joined} "
            f"WHERE s.season = 'low' GROUP BY {', '.join([*group, 'segment'])}"
        )
        return [ctx.target_row(r) for r in profile], [ctx.target_row(r) for r in segments]

    def close(self):
        pass
