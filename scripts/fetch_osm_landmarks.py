import argparse
import csv
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
USER_AGENT = "co5173-data-engineering/landmarks-fetch (student project)"
CITIES = {
    "hcmc": "10.65,106.55,10.90,106.85",
    "hanoi": "20.95,105.75,21.10,105.90",
    "danang": "15.95,108.10,16.15,108.30",
}
TICKET_DEFAULT_VND = {
    "museum": 40000,
    "gallery": 30000,
    "zoo": 60000,
    "theme_park": 150000,
    "aquarium": 100000,
    "attraction": 20000,
    "viewpoint": 0,
    "artwork": 0,
    "historic": 30000,
    "place_of_worship": 0,
}
COLUMNS = ["osm_id", "name", "category", "city", "lat", "lon", "ticket_vnd"]


def build_query(bbox):
    return (
        "[out:json][timeout:120];\n"
        "(\n"
        f'  nwr["tourism"~"^(attraction|museum|gallery|zoo|theme_park|aquarium|viewpoint|artwork)$"]({bbox});\n'
        f'  nwr["historic"]({bbox});\n'
        f'  nwr["amenity"="place_of_worship"]["name"]({bbox});\n'
        ");\n"
        "out center tags;"
    )


def post(endpoint, query):
    body = urllib.parse.urlencode({"data": query}).encode()
    request = urllib.request.Request(endpoint, data=body, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.loads(response.read().decode("utf-8"))["elements"]


def fetch(query):
    for endpoint in ENDPOINTS:
        for attempt in range(2):
            try:
                return post(endpoint, query)
            except (OSError, ValueError) as error:
                print(f"  {endpoint} attempt {attempt + 1} failed: {error}")
                if attempt == 0:
                    time.sleep(10)
    raise SystemExit("Overpass unreachable on all endpoints")


def clean_name(raw):
    name = raw.replace(",", " ").replace('"', "")
    return " ".join(name.split())


def category_of(tags):
    if "tourism" in tags:
        return tags["tourism"]
    if "historic" in tags:
        return "historic"
    return "place_of_worship"


def ticket_vnd(tags, category):
    # ponytail: ticket price = OSM fee/charge tag else per-category flat estimate; swap for a real price table when one exists
    if tags.get("fee") == "no":
        return 0
    match = re.match(r"^\D*(\d{1,3}(?:[.,]\d{3})+|\d+)\D*$", tags.get("charge", ""))
    if match:
        return int(re.sub(r"[.,]", "", match.group(1)))
    return TICKET_DEFAULT_VND.get(category, 0)


def coords(element):
    if element["type"] == "node":
        return element.get("lat"), element.get("lon")
    center = element.get("center") or {}
    return center.get("lat"), center.get("lon")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/br1/landmarks.csv")
    args = parser.parse_args()

    out_path = REPO_ROOT / args.out
    seen = set()
    rows = []
    fetched = 0
    dropped = {"duplicate": 0, "no_name": 0, "no_coords": 0}
    for city in CITIES:
        elements = fetch(build_query(CITIES[city]))
        kept = 0
        fetched += len(elements)
        for element in elements:
            key = (element["type"], element["id"])
            tags = element.get("tags", {})
            name = clean_name(tags.get("name", ""))
            lat, lon = coords(element)
            if key in seen:
                dropped["duplicate"] += 1
                continue
            if not name:
                dropped["no_name"] += 1
                continue
            if lat is None or lon is None:
                dropped["no_coords"] += 1
                continue
            seen.add(key)
            category = category_of(tags)
            rows.append({
                "osm_id": f"{element['type'][0]}{element['id']}",
                "name": name,
                "category": category,
                "city": city,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "ticket_vnd": ticket_vnd(tags, category),
            })
            kept += 1
        print(f"{city}: fetched {len(elements)}, kept {kept}")

    rows.sort(key=lambda row: (row["city"], row["osm_id"]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        for row in rows:
            writer.writerow([row[column] for column in COLUMNS])
    print(f"fetched {fetched}, dropped {dropped}, written {len(rows)}")
    print(f"total: {len(rows)} rows -> {out_path}")


if __name__ == "__main__":
    main()
