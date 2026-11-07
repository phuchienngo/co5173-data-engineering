"""BR5 promotion rules (spec.md sections 3.3-3.5), in plain Python.

Both engines (Spark and ClickHouse SQL) aggregate the cleaned bookings and
hand the small results to these functions, so the business rules live in one
place and can be unit-tested without Spark (tests/br5/test_rules.py).

A *target* is a tuple (room_type_code, country_code). A part is None when the
user picked a value for it (or Hotel = All): it is then fixed, not grouped.
Country "" is Unknown.
"""

import calendar
import math

MONTHS = range(1, 13)
MONTH_ABBR = list(calendar.month_abbr)  # MONTH_ABBR[1] == "Jan"
UNKNOWN_COUNTRY = ""

DEFAULT_RULES = {
    "low_season_threshold": 0.60,
    "early_bird_days": 30,
    "last_minute_days": 14,
    "shorter_stay": 0.25,
    "family_ratio": 0.50,
    "price_already_low": 0.30,
    "tier1_below": 0.40,
    "tier1_label": "Strong: 20–30%",
    "tier2_below": 0.50,
    "tier2_label": "Moderate: 10–20%",
    "tier3_label": "Light: 5–10%",
    "min_bookings": 100,
    "top_n": 10,
}


def month_coverage(year_months):
    """{month: number of years in the data that have this month}.
    year_months: iterable of YYYYMM integers present in the whole data."""
    coverage = {m: 0 for m in MONTHS}
    for ym in set(year_months):
        coverage[ym % 100] += 1
    return coverage


def monthly_demand(rows, coverage, multiplier=1):
    """rows: dicts with month, room_nights, bookings (summed over years) for one
    target. Returns {month: {"demand", "bookings"}}: the average per year over
    the years where the month exists, per original row (÷ multiplier).
    Months that never exist in the data are left out."""
    totals = {m: [0.0, 0.0] for m in MONTHS if coverage.get(m)}
    for row in rows:
        if row["month"] in totals:
            totals[row["month"]][0] += row["room_nights"]
            totals[row["month"]][1] += row["bookings"]
    return {
        m: {"demand": nights / coverage[m] / multiplier, "bookings": bookings / coverage[m] / multiplier}
        for m, (nights, bookings) in totals.items()
    }


def season(demand, threshold):
    """Peak month, low-season months and the figures derived from them.
    demand: {month: {"demand": ...}} as returned by monthly_demand."""
    if not demand:
        return None
    peak_month = min(demand, key=lambda m: (-demand[m]["demand"], m))
    peak = demand[peak_month]["demand"]
    if peak <= 0:
        return None
    low_months = [m for m in sorted(demand) if demand[m]["demand"] < threshold * peak]
    low_demand = sum(demand[m]["demand"] for m in low_months) / len(low_months) if low_months else None
    return {
        "peak_month": peak_month,
        "peak_demand": peak,
        "low_months": low_months,
        "low_demand": low_demand,
        "demand_ratio": low_demand / peak if low_months else None,
        "potential": peak - low_demand if low_months else 0.0,
        "ranges": month_ranges(low_months),
    }


def month_ranges(months):
    """Adjacent months grouped into ranges, wrapping from Dec to Jan.
    [1, 2, 11, 12] -> [[11, 12, 1, 2]]."""
    months = sorted(set(months))
    if not months:
        return []
    if len(months) == 12:
        return [list(MONTHS)]
    ranges = []
    for m in months:
        if ranges and m == ranges[-1][-1] + 1:
            ranges[-1].append(m)
        else:
            ranges.append([m])
    if len(ranges) > 1 and ranges[0][0] == 1 and ranges[-1][-1] == 12:
        ranges[0] = ranges.pop() + ranges[0]
    # Ranges in calendar order of their first month.
    return sorted(ranges, key=lambda r: r[0])


def range_label(months):
    if len(months) == 1:
        return MONTH_ABBR[months[0]]
    return f"{MONTH_ABBR[months[0]]} – {MONTH_ABBR[months[-1]]}"


def season_texts(demand, info):
    """One line per low-season range: "Nov – Feb: demand is 40% of peak"."""
    texts = []
    for months in info["ranges"]:
        average = sum(demand[m]["demand"] for m in months) / len(months)
        texts.append(f"{range_label(months)}: demand is {average / info['peak_demand']:.0%} of peak")
    return texts


def discount_tier(ratio, rules):
    if ratio < rules["tier1_below"]:
        return rules["tier1_label"]
    if ratio < rules["tier2_below"]:
        return rules["tier2_label"]
    # S8: anything above the last bound (threshold raised above 60%) gets the lightest tier.
    return rules["tier3_label"]


def offers(low, peak, info, rules):
    """Suggested offers for one target (spec 3.4).

    low, peak: season profiles with bookings, median_lead, avg_stay,
    family_share, avg_adr (None when not computable).
    Returns (list of {"offer", "evidence"}, discount tier or "")."""
    found = []
    peak_name = MONTH_ABBR[info["peak_month"]]
    lead = low.get("median_lead")
    if lead is not None:
        if lead >= rules["early_bird_days"]:
            days = max(5, int(lead // 5 * 5))
            found.append({
                "offer": f"Early-bird offer: book at least {days} days ahead",
                "evidence": f"In low season guests typically book {lead:.0f} days ahead (median)",
            })
        elif lead <= rules["last_minute_days"]:
            found.append({
                "offer": "Last-minute deal shown close to arrival",
                "evidence": f"In low season guests typically book only {lead:.0f} days ahead (median)",
            })

    low_stay, peak_stay = low.get("avg_stay"), peak.get("avg_stay")
    if low_stay is not None and peak_stay and low_stay <= (1 - rules["shorter_stay"]) * peak_stay:
        found.append({
            "offer": "Stay-longer offer (for example third night free)",
            "evidence": f"Low-season stays average {low_stay:.1f} nights vs {peak_stay:.1f} in {peak_name} "
                        f"({1 - low_stay / peak_stay:.0%} shorter)",
        })

    low_family, peak_family = low.get("family_share"), peak.get("family_share")
    if low_family is not None and peak_family and low_family < rules["family_ratio"] * peak_family:
        found.append({
            "offer": "Family package (kids stay free)",
            "evidence": f"{low_family:.0%} of low-season bookings include children vs {peak_family:.0%} in {peak_name}",
        })

    tier = discount_tier(info["demand_ratio"], rules)
    low_adr, peak_adr = low.get("avg_adr"), peak.get("avg_adr")
    if low_adr is not None and peak_adr:
        drop = 1 - low_adr / peak_adr
        prices = f"average daily rate {low_adr:.0f} vs {peak_adr:.0f} in {peak_name}"
        if drop >= rules["price_already_low"]:
            found.append({
                "offer": "Add value, don't cut the price further: free breakfast or a room upgrade",
                "evidence": f"Low-season price is already {drop:.0%} below peak ({prices})",
            })
            tier = ""
        else:
            price_change = f"only {drop:.0%} lower" if drop >= 0.005 else (
                "about the same" if drop > -0.005 else f"{-drop:.0%} higher")
            found.append({
                "offer": f"Discount: {tier}",
                "evidence": f"Low-season demand is {info['demand_ratio']:.0%} of peak while the price is "
                            f"{price_change} ({prices})",
            })
    else:
        # S7: only without usable prices can no price rule apply.
        found.append({
            "offer": f"No specific offer pattern; consider a general discount: {tier}",
            "evidence": "No booking with a price above 0 to compare low season with peak",
        })
    return found, tier


def evaluate_targets(rows, coverage, multiplier, rules):
    """Step "rank": monthly demand and season for every target and for the
    whole selection, minimum data, and the top targets by potential.

    rows: dicts with target, month, room_nights, bookings (one per target ×
    month, years already summed; the engines may also send one per year)."""
    by_target = {}
    for row in rows:
        by_target.setdefault(row["target"], []).append(row)

    selection_demand = monthly_demand(rows, coverage, multiplier)
    selection = {
        "demand": selection_demand,
        "season": season(selection_demand, rules["low_season_threshold"]),
        "valid_bookings": sum(r["bookings"] for r in rows) / multiplier,
    }
    if selection["season"]:
        selection["texts"] = season_texts(selection_demand, selection["season"])

    candidates = []
    for target, target_rows in by_target.items():
        if target[1] == UNKNOWN_COUNTRY:
            continue  # S5: Unknown counts in the selection, never as a target
        bookings = sum(r["bookings"] for r in target_rows) / multiplier
        if bookings < rules["min_bookings"]:
            continue
        demand = monthly_demand(target_rows, coverage, multiplier)
        info = season(demand, rules["low_season_threshold"])
        if not info or not info["low_months"]:
            continue
        candidates.append({"target": target, "valid_bookings": bookings, "demand": demand, "season": info})

    candidates.sort(key=lambda c: (-round(c["season"]["potential"], 6), -c["valid_bookings"],
                                   tuple(x or "" for x in c["target"])))
    return {
        "selection": selection,
        "targets_found": len(by_target),
        "targets_qualified": len(candidates),
        "top": candidates[: int(rules["top_n"])],
    }


def profile_figures(row, multiplier=1):
    """Season profile from an engine's profile row: bookings, sums and the
    median lead time."""
    bookings = row["bookings"]
    if not bookings:
        return {"bookings": 0, "median_lead": None, "avg_stay": None, "family_share": None, "avg_adr": None}
    return {
        "bookings": bookings / multiplier,
        "median_lead": row["median_lead"],
        "avg_stay": row["nights"] / bookings,
        "family_share": row["family_bookings"] / bookings,
        "avg_adr": row["adr_sum"] / row["adr_count"] if row["adr_count"] else None,
    }


def top_channels(segment_counts, limit=2):
    """segment_counts: {market_segment: bookings} in low season."""
    total = sum(segment_counts.values())
    if not total:
        return []
    ranked = sorted(segment_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
    return [f"{segment} ({count / total:.0%})" for segment, count in ranked]


def build_card(rank, candidate, low, peak, segments, rules):
    """One recommendation card (spec 3.2)."""
    info = candidate["season"]
    found, tier = offers(low, peak, info, rules)
    return {
        "rank": rank,
        "target": candidate["target"],
        "valid_bookings": candidate["valid_bookings"],
        "low_months": info["low_months"],
        "when_text": "; ".join(season_texts(candidate["demand"], info)),
        "peak_month": info["peak_month"],
        "peak_demand": info["peak_demand"],
        "low_demand": info["low_demand"],
        "demand_ratio": info["demand_ratio"],
        "potential": info["potential"],
        "offers": [f["offer"] for f in found],
        "evidence": [f["evidence"] for f in found],
        "discount": tier,
        "channels": top_channels(segments),
        "low": low,
        "peak": peak,
        "demand": candidate["demand"],
    }


def rounded(value, digits=2):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(value, digits)
