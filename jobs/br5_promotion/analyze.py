"""BR5 analysis run: what promotion to offer, when and to whom.

Plan: docs/plan/br5/plan-pipeline.md. Steps: read, clean, aggregate, rank,
profile, recommend, write. The engine (Spark, or ClickHouse SQL as the
alternative) does the heavy steps; rules.py turns the totals into cards.

Started by launch.py (the app's "Find promotions" button and `make br5-run`):
    spark-submit analyze.py --hotel "City Hotel" --country All --room-type A --settings-json '{...}'
With the ClickHouse engine it runs as plain python3, without Spark.
"""

import argparse
import json
import sys

import rules
import run_settings
import runs
from clickhouse_io import get_client

ALL = "All"
ALL_HOTELS = "All hotels"


def parse_args():
    parser = argparse.ArgumentParser(description="BR5 low-season promotion run")
    parser.add_argument("--hotel", default=ALL, help="Hotel name or All")
    parser.add_argument("--country", default=ALL, help="Country code or All")
    parser.add_argument("--room-type", default=ALL, help="Reserved room type code or All")
    parser.add_argument("--run-id", help="Run id to record (the launcher passes its own)")
    parser.add_argument("--settings-json", help="Resolved settings; default: the current settings")
    parser.add_argument("--settings-label", default="current settings", help="Where the settings came from")
    return parser.parse_args()


class Dims:
    """The small dimension tables, read on the driver."""

    def __init__(self, client):
        def pairs(sql):
            return client.query(sql).result_rows

        self.hotels = dict(pairs("SELECT hotel_key, hotel_name FROM br5.dim_hotel"))
        self.countries = {k: (code, name) for k, code, name in
                          pairs("SELECT country_key, country_code, country_name FROM br5.dim_country")}
        self.rooms = dict(pairs("SELECT room_type_key, room_type_code FROM br5.dim_room_type"))
        self.channels = dict(pairs("SELECT channel_key, market_segment FROM br5.dim_channel"))
        self.date_months = [tuple(r) for r in pairs("SELECT date_key, month FROM br5.dim_date")]
        self.hotel_key = {v: k for k, v in self.hotels.items()}
        self.country_key = {code: k for k, (code, _) in self.countries.items()}
        self.room_key = {v: k for k, v in self.rooms.items()}

    def country_name(self, code):
        key = self.country_key.get(code)
        return self.countries[key][1] if key is not None else code


class Context:
    """What the engines need: filters, grouping, settings and the key ↔ code
    mapping (late join: engines group on keys, names are attached here)."""

    def __init__(self, client, criteria, dims, values, multiplier, coverage, codes):
        self.client = client
        self.criteria = criteria
        self.dims = dims
        self.opts = run_settings.options(values)
        self.query_settings = run_settings.clickhouse_query_settings(values)
        self.multiplier = multiplier
        self.coverage = coverage
        self.codes = codes  # True: the engine groups on codes (Spark, early join)
        self.key_type = "string" if codes else "int"
        self.group_columns = [c for c, crit in (("room", "room_type"), ("country", "country"))
                              if criteria[crit] == ALL]
        where = []
        if criteria["hotel"] != ALL:
            where.append(f"hotel_key = {dims.hotel_key[criteria['hotel']]}")
        if criteria["country"] != ALL:
            where.append(f"country_key = {dims.country_key[criteria['country']]}")
        if criteria["room_type"] != ALL:
            where.append(f"reserved_room_type_key = {dims.room_key[criteria['room_type']]}")
        self.where = " AND ".join(where) or "1"

    def _code(self, column, value):
        if self.codes:
            return value
        return self.dims.rooms[value] if column == "room" else self.dims.countries[value][0]

    def _key(self, column, code):
        if self.codes:
            return code
        return self.dims.room_key[code] if column == "room" else self.dims.country_key[code]

    def target_row(self, row):
        """Engine row → row with target = (room code | None, country code | None)."""
        row = dict(row)
        target = [None, None]
        for i, column in enumerate(("room", "country")):
            if column in self.group_columns:
                target[i] = self._code(column, row.pop(column))
        row["target"] = tuple(target)
        if "segment" in row and not self.codes:
            row["segment"] = self.dims.channels[row["segment"]]
        return row

    def target_keys(self, target):
        return [self._key(c, target[0 if c == "room" else 1]) for c in self.group_columns]


def check_criteria(criteria, dims):
    problems = []
    if criteria["hotel"] != ALL and criteria["hotel"] not in dims.hotel_key:
        problems.append(f"unknown hotel '{criteria['hotel']}'")
    if criteria["country"] != ALL and criteria["country"] not in dims.country_key:
        problems.append(f"unknown country code '{criteria['country']}'")
    if criteria["room_type"] != ALL and criteria["room_type"] not in dims.room_key:
        problems.append(f"unknown room type '{criteria['room_type']}'")
    if problems:
        sys.exit("Invalid choice: " + "; ".join(problems))


def month_rows(rank, demand, info, adr):
    peak = info["peak_demand"] if info else 0
    low = set(info["low_months"]) if info else set()
    return [
        [rank, m, d["demand"], d["bookings"], adr.get(m), d["demand"] / peak if peak else 0.0,
         int(m in low), int(info is not None and m == info["peak_month"])]
        for m, d in sorted(demand.items())
    ]


MONTH_COLUMNS = ["target_rank", "month", "demand", "bookings", "avg_adr", "share_of_peak", "is_low", "is_peak"]
CARD_COLUMNS = [
    "target_rank", "hotel", "room_type", "country_code", "country_name", "valid_bookings", "low_months",
    "when_text", "peak_month", "peak_demand", "low_demand", "demand_ratio", "potential", "offers", "evidence",
    "discount", "channels", "low_bookings", "peak_bookings", "low_median_lead", "peak_median_lead",
    "low_avg_stay", "peak_avg_stay", "low_family_share", "peak_family_share", "low_avg_adr", "peak_avg_adr",
]


def adr_by_month(rows):
    sums = {}
    for r in rows:
        s = sums.setdefault(r["month"], [0.0, 0])
        s[0] += r["adr_sum"] or 0.0
        s[1] += r["adr_count"]
    return {m: (s / c if c else None) for m, (s, c) in sums.items()}


def no_card_message(criteria, evaluated, rule_values, group_columns):
    selection = evaluated["selection"]
    minimum = int(rule_values["min_bookings"])
    threshold = f"{rule_values['low_season_threshold']:.0%}"
    if selection["valid_bookings"] < minimum:
        return (f"Not enough data to recommend: {selection['valid_bookings']:,.0f} valid bookings for this choice "
                f"(minimum {minimum}).")
    if not group_columns:
        return f"No month falls below {threshold} of the peak for this choice, so there is no low season to fill."
    what = {("room",): "room type", ("country",): "country"}.get(tuple(group_columns), "room type × country")
    return (f"No {what} with at least {minimum} valid bookings has a month below {threshold} of its peak "
            "for this choice.")


def analyze(ctx, engine, run):
    rule_values = ctx.opts["rules"]
    with run.step("read"):
        engine.read()
    with run.step("clean"):
        counts = engine.clean()
        excluded = {r: counts[r] for r in ("canceled", "zero_nights", "zero_guests", "duplicate")}
        used = counts["read"] - sum(excluded.values())
        run.update(rows_read=counts["read"], rows_used=used,
                   rows_excluded={**excluded, "kept_unknown_country": counts["kept_unknown_country"],
                                  "kept_no_price": counts["kept_no_price"]})
        print(f"[br5] {counts['read']} bookings read, {used} used, excluded {excluded}", flush=True)
    with run.step("aggregate"):
        totals = engine.aggregate()
    with run.step("rank"):
        evaluated = rules.evaluate_targets(totals, ctx.coverage, ctx.multiplier, rule_values)
        top = evaluated["top"]
        print(f"[br5] {evaluated['targets_found']} targets, {evaluated['targets_qualified']} qualify, "
              f"{len(top)} kept", flush=True)
    profiles, segments = {}, {}
    if top:
        with run.step("profile"):
            seasons = []
            for c in top:
                seasons += [(c["target"], m, "low") for m in c["season"]["low_months"]]
                seasons.append((c["target"], c["season"]["peak_month"], "peak"))
            profile_rows, segment_rows = engine.profile(seasons)
            for r in profile_rows:
                profiles[(r["target"], r["season"])] = r
            for r in segment_rows:
                by_segment = segments.setdefault(r["target"], {})
                by_segment[r["segment"]] = by_segment.get(r["segment"], 0) + r["bookings"]
    with run.step("recommend"):
        empty = {"bookings": 0}
        cards = []
        for rank, c in enumerate(top, start=1):
            low = rules.profile_figures(profiles.get((c["target"], "low"), empty), ctx.multiplier)
            peak = rules.profile_figures(profiles.get((c["target"], "peak"), empty), ctx.multiplier)
            cards.append(rules.build_card(rank, c, low, peak, segments.get(c["target"], {}), rule_values))
        message = None if cards else no_card_message(ctx.criteria, evaluated, rule_values, ctx.group_columns)
    with run.step("write"):
        write_results(ctx, run, totals, evaluated, cards, message)
    return cards, message


def write_results(ctx, run, totals, evaluated, cards, message):
    selection = evaluated["selection"]
    months = month_rows(0, selection["demand"], selection["season"], adr_by_month(totals))
    by_target = {}
    for r in totals:
        by_target.setdefault(r["target"], []).append(r)
    card_rows = []
    for card in cards:
        target = card["target"]
        months += month_rows(card["rank"], card["demand"], {**card, "ranges": None}, adr_by_month(by_target[target]))
        country = target[1] if target[1] is not None else ctx.criteria["country"]
        low, peak = card["low"], card["peak"]
        card_rows.append([
            card["rank"],
            ctx.criteria["hotel"] if ctx.criteria["hotel"] != ALL else ALL_HOTELS,
            target[0] if target[0] is not None else ctx.criteria["room_type"],
            country,
            ctx.dims.country_name(country),
            card["valid_bookings"], card["low_months"], card["when_text"], card["peak_month"],
            card["peak_demand"], card["low_demand"], card["demand_ratio"], card["potential"],
            card["offers"], card["evidence"], card["discount"], card["channels"],
            low["bookings"], peak["bookings"], low["median_lead"], peak["median_lead"],
            low["avg_stay"], peak["avg_stay"], low["family_share"], peak["family_share"],
            low["avg_adr"], peak["avg_adr"],
        ])
    run_id = run.run_id
    ctx.client.insert("run_month", [[run_id, *r] for r in months], column_names=["run_id", *MONTH_COLUMNS],
                      database=runs.DB)
    if card_rows:
        ctx.client.insert("run_card", [[run_id, *r] for r in card_rows], column_names=["run_id", *CARD_COLUMNS],
                          database=runs.DB)
    summary = {
        "message": message,
        "season_texts": selection.get("texts", []),
        "selection_bookings": selection["valid_bookings"],
        "targets_found": evaluated["targets_found"],
        "targets_qualified": evaluated["targets_qualified"],
        "multiplier": ctx.multiplier,
    }
    run.update(result_count=len(cards), result_summary=json.dumps(summary))


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "run_card"):
        sys.exit("BR5 tables are missing or outdated: run `make br5-up`.")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(f"Busy: an {busy['run_type']} run is in progress (step: {busy['step']}).")
    import_id, multiplier = runs.stored_import(client)
    if import_id is None:
        sys.exit("No data yet: import it first (Data page or `make br5-import`).")

    if args.settings_json:
        values = run_settings.resolve(json.loads(args.settings_json))
    else:
        values = run_settings.resolve(runs.load_settings(client, run_settings.CURRENT))
    cluster = runs.cluster_info()
    errors, _ = run_settings.validate(values, cluster)
    if errors:
        sys.exit("Settings can't be used: " + "; ".join(f"{k} {v}" for k, v in errors.items()))

    criteria = {"hotel": args.hotel, "country": args.country, "room_type": args.room_type}
    dims = Dims(client)
    check_criteria(criteria, dims)
    coverage = rules.month_coverage(
        r[0] for r in client.query("SELECT DISTINCT intDiv(arrival_date_key, 100) FROM br5.fact_booking").result_rows
    )
    opts = run_settings.options(values)
    engine_name = opts["pipeline"]["engine"]

    run = runs.Run(get_client(autogenerate_session_id=False), "analysis", run_id=args.run_id, criteria=criteria,
                   settings=run_settings.snapshot(values, cluster, label=args.settings_label),
                   import_id=import_id, multiplier=multiplier)
    run.start()
    runs.stop_on_sigterm()
    print(f"[br5] analysis run {run.run_id} started: {criteria}, engine {engine_name}", flush=True)

    spark = None
    engine = None
    try:
        if engine_name == "spark":
            # Imported here: the ClickHouse engine runs without pyspark.
            from pipeline_spark import SparkEngine
            from spark_session import get_spark_session

            spark = get_spark_session("br5_analysis", conf=run_settings.spark_conf(values))
            spark.sparkContext.setLogLevel("WARN")
            run.update(settings=run_settings.snapshot(values, cluster, spark, args.settings_label))
            ctx = Context(client, criteria, dims, values, multiplier, coverage,
                          codes=opts["pipeline"]["join"] == "early")
            engine = SparkEngine(spark, ctx)
        else:
            from pipeline_sql import SqlEngine

            ctx = Context(client, criteria, dims, values, multiplier, coverage, codes=False)
            engine = SqlEngine(ctx)
        cards, message = analyze(ctx, engine, run)
    except BaseException as exc:
        error = runs.failure_message(exc)
        print(f"[br5] analysis failed: {error}", flush=True)
        run.finish("failed", error=error[:4000])
        raise
    finally:
        if engine is not None:
            engine.close()
        if spark is not None:
            spark.stop()

    run.finish("succeeded")
    print(f"[br5] analysis succeeded: {len(cards)} cards in {run.row['duration_ms'] / 1000:.1f}s"
          + (f" ({message})" if message else ""), flush=True)
    for card in cards:
        print(f"[br5]  #{card['rank']} {card['target']}: {card['when_text']} | potential "
              f"{card['potential']:.1f} room-nights/month | {'; '.join(card['offers'])}", flush=True)


if __name__ == "__main__":
    main()
