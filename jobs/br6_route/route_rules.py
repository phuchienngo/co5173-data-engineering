"""BR6 Route Finder rules: route geometry, the grid and the scoring.

Pure Python, shared by both engines (Spark and ClickHouse SQL) and unit-tested
without Spark. Plan: docs/plan/br6/plan-pipeline.md, section 3.

Distances are straight lines on a flat map of the city: km east (x) and north
(y) of the city centre. Every engine computes x, y and the distance to a leg
with the same operations in the same order, so all of them get the same
numbers (spec C9, C10).
"""

import math

# km per degree of latitude (mean Earth radius 6371.0088 km).
KM_PER_DEGREE = math.pi * 6371.0088 / 180
# Distances are rounded to 1 mm and scores to 6 decimals before they are
# filtered, ordered, stored or compared, so the last bits of floating-point
# sums never change a list.
DECIMALS = 6
# Engines return places up to this much beyond the maximum detour; the exact
# filter is applied here, on the rounded distance.
MATCH_SLACK_KM = 1e-6
STATUSES = ("suggested", "below the top N", "few visitors", "type not chosen", "not offered")


class FlatMap:
    """lat/lon -> km east and north of the city centre."""

    def __init__(self, centre_lat, centre_lon):
        self.lat0 = float(centre_lat)
        self.lon0 = float(centre_lon)
        self.cos0 = math.cos(math.radians(self.lat0))

    def xy(self, lat, lon):
        return (lon - self.lon0) * self.cos0 * KM_PER_DEGREE, (lat - self.lat0) * KM_PER_DEGREE


def leg_distance(qx, qy, ax, ay, bx, by):
    """(distance from Q to the leg A-B, t in [0, 1] of the nearest point)."""
    dx = bx - ax
    dy = by - ay
    l2 = dx * dx + dy * dy
    t = 0.0 if l2 == 0 else min(max(((qx - ax) * dx + (qy - ay) * dy) / l2, 0.0), 1.0)
    ex = qx - (ax + t * dx)
    ey = qy - (ay + t * dy)
    return math.sqrt(ex * ex + ey * ey), t


def build_legs(points):
    """points: [(x, y)] start, stops, end. Returns one dict per leg with its
    ends, length and the distance along the route where it starts."""
    legs, start_km = [], 0.0
    for i, ((ax, ay), (bx, by)) in enumerate(zip(points, points[1:])):
        length = math.sqrt((bx - ax) ** 2 + (by - ay) ** 2)
        legs.append({"leg": i, "ax": ax, "ay": ay, "bx": bx, "by": by, "length": length, "start_km": start_km})
        start_km += length
    return legs


def route_km(legs):
    return sum(leg["length"] for leg in legs)


def nearest(qx, qy, legs):
    """(detour, leg, position) of a place: the closest leg (the earliest one on
    a tie), and the distance along the route to the nearest point."""
    best = None
    for leg in legs:
        d, t = leg_distance(qx, qy, leg["ax"], leg["ay"], leg["bx"], leg["by"])
        if best is None or d < best[0]:
            best = (d, leg["leg"], leg["start_km"] + t * leg["length"])
    return best


def cell_of(x, y, size):
    return math.floor(x / size), math.floor(y / size)


def route_cells(legs, max_detour, size):
    """Grid squares that can hold a place within max_detour of the route.

    A place within D of a leg lies in a square whose centre is within
    D + half the square's diagonal of that leg, so keeping exactly those
    squares never loses a candidate."""
    reach = max_detour + size * math.sqrt(2) / 2 + 1e-9
    cells = set()
    for leg in legs:
        x0, y0 = cell_of(min(leg["ax"], leg["bx"]) - max_detour, min(leg["ay"], leg["by"]) - max_detour, size)
        x1, y1 = cell_of(max(leg["ax"], leg["bx"]) + max_detour, max(leg["ay"], leg["by"]) + max_detour, size)
        for cx in range(x0, x1 + 1):
            for cy in range(y0, y1 + 1):
                d, _ = leg_distance((cx + 0.5) * size, (cy + 0.5) * size, leg["ax"], leg["ay"], leg["bx"], leg["by"])
                if d <= reach:
                    cells.add((cx, cy))
    return sorted(cells)


# --- Labels -------------------------------------------------------------------

def point_labels(points):
    """Labels of the route points: the start, stop 1.., the end."""
    stops = len(points) - 2
    return ["the start", *[f"stop {i}" for i in range(1, stops + 1)], "the end"]


def position_text(position, leg, labels):
    return f"{position:.1f} km from the start, between {labels[leg]} and {labels[leg + 1]}"


# --- Visitor habits -----------------------------------------------------------

def type_shares(type_checkins):
    """{place type: share of the visitor's used check-ins}."""
    total = sum(type_checkins.values())
    return {t: n / total for t, n in type_checkins.items()} if total else {}


def favourite_types(type_checkins, n=2):
    ordered = sorted(type_checkins.items(), key=lambda kv: (-kv[1], kv[0]))
    return [t for t, count in ordered[:n] if count > 0]


def fit(place_type, shares):
    """The visitor's share in this type ÷ their share in their favourite type
    (spec 3.3), so the favourite type scores 1. Earlier visits don't count."""
    top = max(shares.values(), default=0)
    return shares.get(place_type, 0.0) / top if top else 0.0


# --- Ranking ------------------------------------------------------------------

def weights(rules, personal):
    if personal:
        return {"popularity": rules["weight_popularity"], "fit": rules["weight_fit"],
                "closeness": rules["weight_closeness"]}
    return {"popularity": rules["general_weight_popularity"], "fit": 0.0,
            "closeness": rules["general_weight_closeness"]}


def rank_places(found, route_keys, chosen_types, habits, rules):
    """Filters, scores and orders the places an engine found near the route.

    found: one dict per place within the maximum detour (plus the engine's
    slack): place_key, detour, leg, position, visitors, checkins, place_type,
    is_offered. visitors and checkins are counted in the original data.
    route_keys: place keys of the route points (never suggested).
    habits: None (general visitor) or {"type_checkins": {type: n},
    "visits": {place_key: n}}, the visitor's used check-ins.
    Returns (rows, counts): every place found with its status and scores, best
    first; and the counts for the route summary."""
    max_detour = rules["max_detour_km"]
    chosen = set(chosen_types)
    shares = type_shares(habits["type_checkins"]) if habits else {}
    visits = habits["visits"] if habits else {}
    w = weights(rules, habits is not None)

    rows = []
    for place in found:
        if place["place_key"] in route_keys:
            continue
        detour = round(place["detour"], DECIMALS)
        if detour > max_detour:
            continue
        row = {**place, "detour": detour, "position": round(place["position"], DECIMALS),
               "visitor_visits": visits.get(place["place_key"], 0),
               "type_share": shares.get(place["place_type"], 0.0) if habits else 0.0,
               "popularity": 0.0, "fit": 0.0, "closeness": 0.0, "score": 0.0, "rank": 0}
        if not place["is_offered"]:
            row["status"] = "not offered"
        elif place["place_type"] not in chosen:
            row["status"] = "type not chosen"
        elif place["visitors"] < rules["min_place_visitors"]:
            row["status"] = "few visitors"
        else:
            row["status"] = None  # decided below
        rows.append(row)

    ranked = [r for r in rows if r["status"] is None]
    most = max((r["visitors"] for r in ranked), default=0)
    for r in ranked:
        r["popularity"] = round(r["visitors"] / most, DECIMALS)
        r["fit"] = round(fit(r["place_type"], shares), DECIMALS) if habits else 0.0
        r["closeness"] = round(1 - r["detour"] / max_detour, DECIMALS)
        r["score"] = round(w["popularity"] * r["popularity"] + w["fit"] * r["fit"]
                           + w["closeness"] * r["closeness"], DECIMALS)
    ranked.sort(key=lambda r: (-r["score"], -r["visitors"], -r["checkins"], r["place_key"]))
    for position, r in enumerate(ranked, start=1):
        if position <= rules["top_n"]:
            r["status"], r["rank"] = "suggested", position
        else:
            r["status"] = "below the top N"

    order = {s: i for i, s in enumerate(STATUSES)}
    others = sorted((r for r in rows if r["rank"] == 0 and r["status"] != "below the top N"),
                    key=lambda r: (order[r["status"]], r["detour"], r["place_key"]))
    rows = ranked + others

    counts = {s: sum(1 for r in rows if r["status"] == s) for s in STATUSES}
    counts["found"] = len(rows)
    counts["after_filters"] = len(ranked)
    return rows, counts


def suggestions_per_type(rows):
    per_type = {}
    for r in rows:
        if r["status"] == "suggested":
            per_type[r["place_type"]] = per_type.get(r["place_type"], 0) + 1
    return per_type


def no_result_message(counts, rules):
    """Why nothing is suggested, or None (spec 3.2: never an empty page)."""
    if counts["suggested"]:
        return None
    base = "No places within the allowed detour for these choices."
    if counts["few visitors"]:
        return (f"{base} {counts['few visitors']} places of the chosen types are near the route, but each has fewer "
                f"than {rules['min_place_visitors']:g} different visitors.")
    if counts["type not chosen"]:
        return (f"{base} {counts['type not chosen']} places of other place types are near the route: "
                "try choosing more place types.")
    return f"{base} Try choosing more place types or a route through a busier area."
