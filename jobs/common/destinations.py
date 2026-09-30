"""Map monitored airports to Inside Airbnb destinations."""

from __future__ import annotations

DESTINATIONS: list[dict] = [
    {
        "city_id": "las-vegas",
        "city_name": "Las Vegas",
        "state": "NV",
        "timezone": "America/Los_Angeles",
        "airbnb_path": "united-states/nv/clark-county-nv",
        "airbnb_snapshots": ["2026-03-28", "2026-06-27"],
        "airports": ["LAS"],
    },
    {
        "city_id": "hawaii",
        "city_name": "Hawaii",
        "state": "HI",
        "timezone": "Pacific/Honolulu",
        "airbnb_path": "united-states/hi/hawaii",
        "airbnb_snapshots": ["2026-03-25", "2026-06-21"],

        "airports": ["HNL", "OGG", "KOA", "LIH", "ITO"],
    },
    {
        "city_id": "new-orleans",
        "city_name": "New Orleans",
        "state": "LA",
        "timezone": "America/Chicago",
        "airbnb_path": "united-states/la/new-orleans",
        "airbnb_snapshots": ["2026-03-19", "2026-06-16"],
        "airports": ["MSY"],
    },
    {
        "city_id": "nashville",
        "city_name": "Nashville",
        "state": "TN",
        "timezone": "America/Chicago",
        "airbnb_path": "united-states/tn/nashville",
        "airbnb_snapshots": ["2026-03-28", "2026-06-26"],
        "airports": ["BNA"],
    },
    {
        "city_id": "new-york-city",
        "city_name": "New York City",
        "state": "NY",
        "timezone": "America/New_York",
        "airbnb_path": "united-states/ny/new-york-city",
        "airbnb_snapshots": ["2026-03-16", "2026-06-14"],

        "airports": ["JFK", "LGA", "EWR"],
    },
    {
        "city_id": "san-francisco",
        "city_name": "San Francisco",
        "state": "CA",
        "timezone": "America/Los_Angeles",
        "airbnb_path": "united-states/ca/san-francisco",
        "airbnb_snapshots": ["2026-03-16", "2026-06-14"],
        "airports": ["SFO"],
    },
]


def monitored_airports() -> dict[str, str]:
    """Monitored airports."""
    return {ap: d["city_id"] for d in DESTINATIONS for ap in d["airports"]}


def latest_snapshot(dest: dict) -> str:
    return max(dest["airbnb_snapshots"])


def by_city(city_id: str) -> dict:
    for d in DESTINATIONS:
        if d["city_id"] == city_id:
            return d
    raise KeyError(f"Unknown destination '{city_id}'. Available: {[d['city_id'] for d in DESTINATIONS]}")
