"""Fetch a pinned TripWorld snapshot and prepare BR8 using Spark in Docker."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs" / "common"))
from br8_rules import ATTRACTION_PATTERN
DATA = Path(os.environ.get("BR8_DATA_DIR", str(ROOT / "data" / "br8")))
REVISION = "421bc1d"
BASE_URL = f"https://huggingface.co/datasets/CRUISEResearchGroup/TripWorld/resolve/{REVISION}"
SOURCE_FILES = (
    "travel_behaviors.parquet",
    "metadata/metadata_all.parquet",
    "region_labels.parquet",
)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(relative, destination, expected=None):
    if destination.exists() and (expected is None or sha256(destination) == expected):
        print(f"Using cached {relative}", flush=True)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(f"{BASE_URL}/{relative}", headers={"User-Agent": "co5173-br8/1.0"})
    temp_path = None
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as target:
                temp_path = Path(target.name)
                while chunk := response.read(1024 * 1024):
                    target.write(chunk)
        if expected and sha256(temp_path) != expected:
            raise ValueError(f"Checksum mismatch: {relative}")
        os.replace(temp_path, destination)
        print(f"Downloaded and verified {relative}", flush=True)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


def build(args):
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.appName("br8_prepare_tripworld").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw_dir, out_dir = DATA / "raw", DATA / "prepared"
    travel = spark.read.parquet(str(raw_dir / SOURCE_FILES[0]))
    metadata = spark.read.parquet(str(raw_dir / SOURCE_FILES[1]))
    regions = spark.read.parquet(str(raw_dir / SOURCE_FILES[2]))
    # All IDs come from the release; no fuzzy spatial or name matching.
    places = metadata.select(
        F.col("fsq_place_id").alias("place_id"),
        F.coalesce(F.expr("nullif(trim(fsq_name), '')"), F.expr("nullif(trim(google_name), '')")).alias("place_name"),
        F.coalesce(F.col("fsq_category_labels"), F.array().cast("array<string>")).alias("categories"),
        F.col("fsq_latitude").cast("double").alias("latitude"),
        F.col("fsq_longitude").cast("double").alias("longitude"),
    ).filter(F.col("place_name").isNotNull()).dropDuplicates(["place_id"])
    destinations = travel.filter(F.col("r_h") != F.col("r_o"))
    if args.region != "ALL":
        destinations = destinations.filter(F.col("r_o") == args.region)
    checkins = destinations.select("user_id", F.col("r_o").alias("region_id"), F.explode("c_o").alias("ci")).select(
        F.col("user_id").cast("long").alias("user_id"),
        "region_id",
        F.col("ci.venue_id").alias("place_id"),
        F.col("ci.ts").cast("timestamp").alias("visit_time"),
        F.col("ci.venue_schema").alias("venue_schema"),
    ).filter((F.col("user_id") > 0) & F.col("place_id").isNotNull() & F.col("visit_time").isNotNull())
    # A trail can be repeated across travel records. Collapse exact check-ins first.
    checkins = checkins.dropDuplicates(["user_id", "place_id", "visit_time"])
    named = checkins.join(places, "place_id", "inner").filter(
        F.concat_ws(" ", F.col("venue_schema"), F.array_join("categories", " ")).rlike(ATTRACTION_PATTERN)
    ).cache()
    counts = named.groupBy("place_id").agg(F.countDistinct("user_id").alias("users"))
    if args.max_places:
        counts = counts.orderBy(F.desc("users"), "place_id").limit(args.max_places)
    selected = named.join(counts.select("place_id"), "place_id", "inner").cache()
    visits = selected.withColumn("visit_date", F.to_date("visit_time")).groupBy(
        "user_id", "place_id", "visit_date"
    ).agg(F.min("visit_time").alias("visit_time"), F.count("*").alias("raw_checkins"))
    visits = visits.withColumn("event_id", F.sha2(F.concat_ws("|", "user_id", "place_id", "visit_date"), 256)).cache()
    visit_count = visits.count()
    if visit_count == 0:
        raise ValueError(f"No named attractions with destination visits for region {args.region}; try --region ALL")
    labels = regions.select(F.col("region_id"), "city_name", "country_name")
    dimensions = selected.groupBy("place_id", "place_name", "latitude", "longitude").agg(
        F.min("region_id").alias("region_id"),
        F.min(F.array_join("categories", " | ")).alias("category"),
    ).join(labels, "region_id", "left").select(
        "place_id", "place_name", "category", "region_id",
        F.coalesce("city_name", "region_id").alias("city_name"),
        F.coalesce("country_name", F.lit("Unknown")).alias("country_name"),
        "latitude", "longitude",
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    # One sorted file keeps historical replay ordered. Full snapshot is ~108k visits; region/max_places shrink it for a quick demo.
    visits.select(
        "event_id", "user_id", "place_id", "visit_date",
        F.date_format("visit_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("visit_time"), "raw_checkins",
    ).coalesce(1).sortWithinPartitions("visit_time", "user_id", "place_id").write.mode("overwrite").option("header", True).csv(str(out_dir / "visits"))
    dimensions.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir / "places"))
    user_places = visits.groupBy("user_id", "place_id").count()
    bounds = visits.agg(F.min("visit_date"), F.max("visit_date")).first()
    summary = {
        "source": "CRUISEResearchGroup/TripWorld", "revision": REVISION,
        "region": args.region, "max_places": args.max_places,
        "visit_definition": "one user/place/UTC calendar day; destination c_o only",
        "attraction_pattern": ATTRACTION_PATTERN,
        "places": dimensions.count(), "visits": visit_count,
        "visitor_place_pairs": user_places.count(),
        "returning_visitor_place_pairs": user_places.filter("count >= 2").count(),
        "first_visit_date": str(bounds[0]), "last_visit_date": str(bounds[1]),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2), flush=True)
    spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default=os.environ.get("BR8_REGION", "ALL"), help="Destination Wikidata ID; ALL=all destinations, Q1490=Tokyo")
    parser.add_argument("--max-places", type=int, default=int(os.environ.get("BR8_MAX_PLACES", "0")), help="Top named attractions by distinct visitors; 0=all (default)")
    parser.add_argument("--refresh", action="store_true", help="Rebuild prepared visits from the pinned source")
    parser.add_argument("--build", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--download-only", action="store_true", help="Only download and verify source files")
    args = parser.parse_args()
    if args.max_places < 0:
        parser.error("--max-places must be nonnegative")
    if args.build:
        build(args)
        return
    summary_path = DATA / "prepared" / "summary.json"
    if summary_path.exists() and not args.refresh:
        summary = json.loads(summary_path.read_text())
        if summary.get("region") == args.region and summary.get("max_places") == args.max_places and summary.get("revision") == REVISION and summary.get("attraction_pattern") == ATTRACTION_PATTERN:
            if list((DATA / "prepared" / "visits").glob("part-*.csv")) and list((DATA / "prepared" / "places").glob("part-*.csv")):
                print(f"Using prepared BR8 data: {summary_path}\n{json.dumps(summary, indent=2)}")
                return
    manifest_path = DATA / "raw" / "MANIFEST.json"
    download("MANIFEST.json", manifest_path)
    manifest = {item["path"]: item for item in json.loads(manifest_path.read_text())["files"]}
    for relative in SOURCE_FILES:
        download(relative, DATA / "raw" / relative, manifest[relative]["sha256"])
    if args.download_only:
        return
    subprocess.run([
        "docker", "exec", "-e", "PYTHONPATH=/opt/jobs/common", "spark-local",
        "spark-submit", "--master", "local[2]", "--driver-memory", "2g",
        "--conf", "spark.sql.shuffle.partitions=8", "/opt/scripts/prepare_br8.py",
        "--build", "--region", args.region, "--max-places", str(args.max_places),
    ], check=True)


if __name__ == "__main__":
    main()
