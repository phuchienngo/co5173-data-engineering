"""BR6 Route Finder run: good stops along a route.

Plan: docs/plan/br6/plan-pipeline.md. Steps: read, clean, match (find places
near the route), visitor, score, write. The engine (Spark, or ClickHouse SQL as
the alternative) does the data steps; route_rules.py ranks the places found.

Started by route_launch.py (the app's "Find stops" button and `make br6-run`):
    spark-submit route_run.py --city jakarta --start 123 --end 456 [--stops 7,8] [--types "Food & drink"]
                              [--visitor 4242] --settings-json '{...}'
Route points are venue ids and the visitor a user id (plan F8). With the
ClickHouse engine it runs as plain python3, without Spark.
"""

import argparse
import json
import sys

import route_reference as reference
import route_rules as rules
import route_runs as runs
import route_settings
import run_figures
from clickhouse_io import get_client

CATALOG = route_settings.CATALOG
REASONS = ["no_location", "far_from_city", "not_offered"]
MAX_STOPS = 3


def parse_args():
    parser = argparse.ArgumentParser(description="BR6 Route Finder run")
    parser.add_argument("--city", required=True, help="City code, for example jakarta")
    parser.add_argument("--start", type=int, required=True, help="venue_id of the start")
    parser.add_argument("--end", type=int, required=True, help="venue_id of the end")
    parser.add_argument("--stops", default="", help="Comma-separated venue_ids of up to 3 stops, in order")
    parser.add_argument("--types", default="", help="Comma-separated place types; default: all offered types")
    parser.add_argument("--visitor", type=int, help="user_id of the visitor; default: General visitor")
    parser.add_argument("--run-id", help="Run id to record (the launcher passes its own)")
    parser.add_argument("--settings-json", help="Resolved settings; default: the current settings")
    parser.add_argument("--settings-label", default="current settings", help="Where the settings came from")
    args = parser.parse_args()
    args.stops = [int(s) for s in args.stops.split(",") if s.strip()]
    args.types = [t.strip() for t in args.types.split(",") if t.strip()] or list(reference.OFFERED_TYPES)
    return args


def log(message):
    print(f"{runs.PREFIX} {message}", flush=True)


class Context:
    """What the engines need: the city, the route, the settings."""

    def __init__(self, client, city, points, visitor_key, values, multiplier):
        self.client = client
        self.city = city
        self.city_key = city["city_key"]
        self.flat = rules.FlatMap(city["centre_lat"], city["centre_lon"])
        self.points = points
        self.legs = rules.build_legs([self.flat.xy(p["latitude"], p["longitude"]) for p in points])
        self.visitor_key = visitor_key
        self.opts = CATALOG.options(values)
        self.query_settings = route_settings.clickhouse_query_settings(values)
        self.multiplier = multiplier
        self.type_names = dict(enumerate(reference.PLACE_TYPES, start=1))


def route_points(client, city, args, max_centre_km):
    """The route's places in order (start, stops, end), checked."""
    venues = [args.start, *args.stops, args.end]
    problems = []
    if len(args.stops) > MAX_STOPS:
        problems.append(f"at most {MAX_STOPS} stops")
    if args.start == args.end:
        problems.append("the end must differ from the start")
    if any(a == b for a, b in zip(venues, venues[1:])):
        problems.append("two consecutive points are the same place")
    found = runs.places_by_venue(client, city["city_key"], venues)
    flat = rules.FlatMap(city["centre_lat"], city["centre_lon"])
    points = []
    for seq, venue in enumerate(venues):
        place = found.get(venue)
        if place is None:
            problems.append(f"venue {venue} is not a place of {city['city_name']}")
            continue
        if not place["has_location"]:
            problems.append(f"{place['name'] or venue} has no known location")
            continue
        x, y = flat.xy(place["latitude"], place["longitude"])
        if (x * x + y * y) ** 0.5 > max_centre_km:
            problems.append(f"{place['name'] or venue} is more than {max_centre_km:g} km from the city centre")
        role = "start" if seq == 0 else "end" if seq == len(venues) - 1 else "stop"
        points.append({**place, "seq": seq, "role": role})
    if problems:
        sys.exit("Invalid choice: " + "; ".join(problems))
    return points


def visitor_habits(rows, type_names):
    """rows: [(place_key, place_type_key, check-ins)] of the visitor's used
    check-ins -> habits for route_rules.rank_places."""
    per_type, visits = {}, {}
    for place_key, type_key, n in rows:
        per_type[type_names[type_key]] = per_type.get(type_names[type_key], 0) + n
        visits[place_key] = visits.get(place_key, 0) + n
    return {"type_checkins": per_type, "visits": visits}


def place_details(client, city_key, keys):
    """{place_key: venue id, name, address, category} for the places found."""
    if not keys:
        return {}
    rows = client.query(f"""
        SELECT p.place_key AS place_key, p.venue_id AS venue_id, ifNull(p.name, '') AS name,
               ifNull(p.address, '') AS address, c.category_name AS category_name
        FROM {runs.DB}.dim_place AS p INNER JOIN {runs.DB}.dim_category AS c ON c.category_key = p.main_category_key
        WHERE p.city_key = {{city:UInt8}} AND p.place_key IN {{keys:Array(UInt32)}}
    """, parameters={"city": city_key, "keys": sorted(keys)}).named_results()
    return {r["place_key"]: r for r in rows}


def find_stops(ctx, engine, run, criteria):
    rule_values = ctx.opts["rules"]
    with run.step("read"):
        engine.read()
    with run.step("clean"):
        counts = engine.clean()
        excluded = {r: counts.get(r, 0) for r in REASONS}
        used = counts.get(None, 0)
        read = used + sum(excluded.values())
        run.update(rows_read=read, rows_used=used, rows_excluded=excluded)
        log(f"{read:,} check-ins read, {used:,} used, excluded {excluded}")
    with run.step("match"):
        found = engine.match()
        log(f"{len(found):,} places with a location within {rule_values['max_detour_km']:g} km of the route")
    habits = None
    if ctx.visitor_key is not None:
        with run.step("visitor"):
            habits = visitor_habits(engine.visitor(), ctx.type_names)
            used_checkins = sum(habits["type_checkins"].values())
            if used_checkins < rule_values["min_visitor_checkins"]:
                raise ValueError(f"Visitor {criteria['visitor']} has {used_checkins} used check-ins in this city "
                                 f"(minimum {rule_values['min_visitor_checkins']})")
    with run.step("score"):
        route_keys = {p["place_key"] for p in ctx.points}
        ranked, counts = rules.rank_places(found, route_keys, criteria["types"].split(","), habits, rule_values)
        message = rules.no_result_message(counts, rule_values)
    with run.step("write"):
        write_results(ctx, run, ranked, counts, habits, message)
    return ranked, counts, message


POINT_COLUMNS = ["seq", "role", "place_key", "venue_id", "name", "latitude", "longitude", "km_from_start"]
PLACE_COLUMNS = ["place_key", "venue_id", "name", "address", "category_name", "place_type", "detour_km",
                 "position_km", "leg", "visitors", "checkins", "visitor_visits", "type_share", "popularity", "fit",
                 "closeness", "score", "rank", "status"]


def write_results(ctx, run, ranked, counts, habits, message):
    run_id = run.run_id
    km = [0.0] + [leg["start_km"] + leg["length"] for leg in ctx.legs]
    ctx.client.insert("run_point", [
        [run_id, p["seq"], p["role"], p["place_key"], p["venue_id"], p["name"], p["latitude"], p["longitude"],
         round(km[p["seq"]], rules.DECIMALS)] for p in ctx.points
    ], column_names=["run_id", *POINT_COLUMNS], database=runs.DB)
    details = place_details(ctx.client, ctx.city_key, [r["place_key"] for r in ranked])
    rows = []
    for r in ranked:
        d = details[r["place_key"]]
        rows.append([run_id, r["place_key"], d["venue_id"], d["name"], d["address"], d["category_name"],
                     r["place_type"], r["detour"], r["position"], r["leg"], r["visitors"], r["checkins"],
                     r["visitor_visits"], r["type_share"], r["popularity"], r["fit"], r["closeness"], r["score"],
                     r["rank"], r["status"]])
    if rows:
        ctx.client.insert("run_place", rows, column_names=["run_id", *PLACE_COLUMNS], database=runs.DB)
    summary = {
        "route_km": round(rules.route_km(ctx.legs), 3),
        "points": [p["name"] for p in ctx.points],
        "found": counts["found"],
        "after_filters": counts["after_filters"],
        "status_counts": {s: counts[s] for s in rules.STATUSES},
        "suggestions_per_type": rules.suggestions_per_type(ranked),
        "message": message,
        "multiplier": ctx.multiplier,
    }
    if habits:
        summary["visitor"] = {"used_checkins": sum(habits["type_checkins"].values()),
                              "type_checkins": habits["type_checkins"],
                              "favourite_types": rules.favourite_types(habits["type_checkins"])}
    run.update(result_count=counts["suggested"], result_summary=json.dumps(summary))


def main():
    args = parse_args()
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "run_place"):
        sys.exit("BR6 tables are missing or outdated: run `make br6-up`.")
    busy = runs.busy_run(client)
    if busy:
        sys.exit(runs.busy_message(busy))
    import_id, multiplier = runs.stored_import(client)
    if import_id is None:
        sys.exit("No data yet: import it first (Data page or `make br6-import`).")

    if args.settings_json:
        values = CATALOG.resolve(json.loads(args.settings_json))
    else:
        values = CATALOG.resolve(runs.load_settings(client, runs.CURRENT))
    cluster = runs.run_store.cluster_info()
    errors, _ = CATALOG.validate(values, cluster)
    # ClickHouse's own size: its default threads per query, "auto(N)" for N CPUs.
    threads = client.command("SELECT value FROM system.settings WHERE name = 'max_threads'")
    cluster = {**(cluster or {}), "clickhouse_max_threads": str(threads).strip("'")}
    if errors:
        sys.exit("Settings can't be used: " + "; ".join(f"{CATALOG.by_key[k].label} {v}" for k, v in errors.items()))
    opts = CATALOG.options(values)
    if opts["pipeline"]["place_stats"] == "import" and not runs.place_stats_ready(client):
        sys.exit("Place statistics are missing: re-import, or set Place statistics to 'run' in Advanced Settings.")

    city = runs.city_by_code(client, args.city)
    if city is None:
        sys.exit(f"Invalid choice: city '{args.city}' is not in the stored data.")
    unknown = [t for t in args.types if t not in reference.OFFERED_TYPES]
    if unknown:
        sys.exit(f"Invalid choice: unknown place types {unknown}; choose from {reference.OFFERED_TYPES}")
    points = route_points(client, city, args, opts["rules"]["max_centre_km"])
    visitor_key = None
    if args.visitor is not None:
        visitor_key = runs.visitor_by_user(client, city["city_key"], args.visitor)
        if visitor_key is None:
            sys.exit(f"Invalid choice: visitor {args.visitor} has no check-ins in {city['city_name']}.")
    grouping = runs.refresh_grouping(client)
    if grouping["reloaded"]:
        log(f"category grouping reloaded from {reference.GROUPING_FILE}")

    criteria = {
        "city": args.city, "start": str(args.start), "end": str(args.end),
        "stops": ",".join(map(str, args.stops)), "types": ",".join(args.types),
        "visitor": "" if args.visitor is None else str(args.visitor),
    }
    engine_name = opts["pipeline"]["engine"]

    def snapshot(spark=None):
        snap = CATALOG.snapshot(values, cluster, spark, args.settings_label)
        snap["grouping.sha1"] = grouping["sha1"]
        snap.update({f"grouping.{t}": str(n) for t, n in grouping["per_type"].items()})
        return snap

    run = runs.Run(get_client(autogenerate_session_id=False), "run", run_id=args.run_id, criteria=criteria,
                   settings=snapshot(), import_id=import_id, multiplier=multiplier)
    run.start()
    runs.stop_on_sigterm()
    log(f"route run {run.run_id} started: {city['city_name']}, {len(points)} points, "
        f"visitor {criteria['visitor'] or 'general'}, engine {engine_name}")

    spark = None
    engine = None
    ui = None
    try:
        ctx = Context(client, city, points, visitor_key, values, multiplier)
        # Tags the run's ClickHouse queries in system.query_log.
        ctx.query_settings["log_comment"] = run.run_id
        if engine_name == "spark":
            # Imported here: the ClickHouse engine runs without pyspark.
            from route_spark import SparkEngine
            from spark_session import get_spark_session

            conf = route_settings.spark_conf(values)
            run_figures.prepare_event_log(conf)
            spark = get_spark_session("BR6 route run", conf=conf)
            spark.sparkContext.setLogLevel("WARN")
            ui = run_figures.spark_ui(spark)
            run.update(settings=snapshot(spark), spark_app_id=ui[1])
            # Spark jobs are grouped by step, for the per-step figures and the Spark page.
            run.on_step = lambda name: spark.sparkContext.setJobGroup(name, name)
            engine = SparkEngine(spark, ctx)
        else:
            from route_sql import SqlEngine

            engine = SqlEngine(ctx)
        ranked, counts, message = find_stops(ctx, engine, run, criteria)
        run.finish("succeeded")
    except BaseException as exc:
        error = runs.failure_message(exc)
        log(f"route run failed: {error}")
        run.finish("failed", error=error[:4000])
        raise
    finally:
        # After finish (not part of the run time), before Spark stops.
        run_figures.record(run, client, runs.STAGING_DB, ui)
        if engine is not None:
            engine.close()
        if spark is not None:
            spark.stop()

    log(f"route run succeeded: {counts['suggested']} suggestions of {counts['found']} places found in "
        f"{run.row['duration_ms'] / 1000:.1f}s" + (f" ({message})" if message else ""))
    for r in ranked[:counts["suggested"]]:
        log(f" #{r['rank']} place {r['place_key']} {r['place_type']}: score {r['score']:.3f}, detour "
            f"{r['detour']:.3f} km, {r['visitors']} visitors")


if __name__ == "__main__":
    main()
