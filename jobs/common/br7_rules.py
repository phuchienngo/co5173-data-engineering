"""BR7 contract: taxi drop-offs in tourist zones as a proxy for arrivals.

The streaming job calls forecast_rows() for every closed hour, and the tests and
scripts/verify_br7.py call the same functions, so there is one forecast rule.
prepare.py mirrors hour_of_week() with Spark expressions for the baseline.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

LOCAL_TZ = "America/New_York"

# TLC taxi zone id -> landmark (Vietnamese) shown on the dashboard. Zone names
# come from taxi_zone_lookup.csv; the landmark is only a reading aid.
TOURIST_ZONES = {
    12: "Bến phà tượng Nữ thần Tự do",
    43: "Công viên Central Park",
    45: "Khu phố Tàu (Chinatown)",
    88: "Phố Wall",
    144: "Khu Little Italy",
    161: "Rockefeller Center, bảo tàng MoMA",
    164: "Tòa nhà Empire State",
    230: "Quảng trường Thời Đại, Broadway",
    234: "Quảng trường Union",
    261: "Đài tưởng niệm 11/9",
}

MIN_TRAINING_HOURS = 8   # baseline cell needs this many historical hours
RATIO_CLAMP = (0.5, 3.0)  # limit how far one hour can move the forecast
MIN_WARNING_ARRIVALS = 50  # ignore "unusual" hours that are still small


def event_id(pickup_time, dropoff_time, pickup_zone, dropoff_zone, distance, total):
    raw = f"{pickup_time}|{dropoff_time}|{pickup_zone}|{dropoff_zone}|{distance}|{total}"
    return hashlib.sha256(raw.encode()).hexdigest()


def hour_of_week(moment):
    """Monday 00:00 local = 0 ... Sunday 23:00 local = 167."""
    local = moment.astimezone(ZoneInfo(LOCAL_TZ))
    return local.weekday() * 24 + local.hour


def predict(observed, mean_now, mean_next, p90_next):
    """Scale the usual next-hour level by how unusual the current hour is."""
    if mean_now and mean_now > 0:
        ratio = min(max(observed / mean_now, RATIO_CLAMP[0]), RATIO_CLAMP[1])
    else:
        ratio = 1.0
    predicted = round(mean_next * ratio, 2)
    warn = predicted > p90_next and predicted >= MIN_WARNING_ARRIVALS
    return predicted, "warning" if warn else "normal"


def normalize_event(row):
    """Validate a prepared drop-off record before it is sent to Kafka."""
    zone = int(row["dropoff_zone_id"])
    if zone not in TOURIST_ZONES:
        raise ValueError(f"zone {zone} is not a BR7 tourist zone")
    dropoff = datetime.fromisoformat(row["dropoff_time"].replace("Z", "+00:00"))
    if dropoff.tzinfo is None:
        raise ValueError("dropoff_time must include a timezone")
    passengers = int(row["passenger_count"] or 0)
    if passengers < 0:
        raise ValueError("passenger_count must be nonnegative")
    if not row.get("event_id") or len(row["event_id"]) != 64:
        raise ValueError("event_id must be a sha256 hex digest")
    return {
        "event_id": row["event_id"],
        "dropoff_zone_id": zone,
        "pickup_zone_id": int(row["pickup_zone_id"]),
        "dropoff_time": dropoff.astimezone(timezone.utc).isoformat(),
        "passenger_count": passengers,
    }


def forecast_rows(rows, baseline):
    """Turn closed windows (zone, UTC hour, arrivals, passengers) into fact rows.

    baseline maps (zone_id, hour_of_week) -> (mean_arrivals, p90_arrivals).
    """
    facts = []
    for zone, observed_hour, arrivals, passengers in rows:
        target_hour = observed_hour + timedelta(hours=1)
        now = baseline.get((zone, hour_of_week(observed_hour)))
        nxt = baseline.get((zone, hour_of_week(target_hour)))
        if nxt is None:
            predicted, threshold, level = None, None, "no_baseline"
        else:
            predicted, level = predict(arrivals, now[0] if now else None, nxt[0], nxt[1])
            threshold = nxt[1]
        facts.append([zone, observed_hour, target_hour, arrivals, passengers,
                      now[0] if now else None, predicted, threshold, level])
    return facts
