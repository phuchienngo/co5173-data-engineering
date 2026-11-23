"""BR8 contract: named attractions, destination check-ins, distinct UTC days."""

import hashlib
import re
from datetime import datetime, timezone

ATTRACTION_PATTERN = (
    r"(?i)(museum|aquarium|zoo|amusement.?park|theme.?park|tourist.?attraction|"
    r"historic.{0,25}site|historical|monument|castle|palace|botanical.?garden|"
    r"scenic.?lookout|national.?park|\blandmark\b)"
)


def visit_id(user_id, place_id, visit_date):
    return hashlib.sha256(f"{user_id}|{place_id}|{visit_date}".encode()).hexdigest()


def is_attraction(schema, categories):
    return bool(re.search(ATTRACTION_PATTERN, f"{schema or ''} {' '.join(categories or [])}"))


def normalize_event(row):
    """Validate a prepared CSV record before it crosses the Kafka boundary."""
    user_id = int(row["user_id"])
    raw_checkins = int(row["raw_checkins"])
    timestamp = datetime.fromisoformat(row["visit_time"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("visit_time must include a timezone")
    timestamp = timestamp.astimezone(timezone.utc)
    day = timestamp.date().isoformat()
    place_id = row["place_id"].strip()
    if user_id <= 0 or raw_checkins <= 0 or not place_id:
        raise ValueError("user, place and positive check-in count are required")
    expected = visit_id(user_id, place_id, day)
    if row["visit_date"] != day or row["event_id"] != expected:
        raise ValueError("visit date/event ID does not match the UTC daily visit")
    return {
        "event_id": expected,
        "user_id": user_id,
        "place_id": place_id,
        "visit_date": day,
        "visit_time": timestamp.isoformat(),
        "raw_checkins": raw_checkins,
    }
