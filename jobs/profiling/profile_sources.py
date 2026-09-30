#!/usr/bin/env python3
"""Profile source coverage and quality."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import Window

from common.config import settings
from common.destinations import DESTINATIONS, monitored_airports
from common.flights import add_utc_times
from common.pipeline_log import job_run
from common.sources import bts_csv_files, read_airports, read_bts_csv, read_calendar, read_listings, read_reviews
from common.spark_session import build_spark

REPORT: dict = {"generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z", "checks": []}
MD: list[str] = []


def check(name: str, status: str, detail: str) -> None:
    icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[status]
    REPORT["checks"].append({"name": name, "status": status, "detail": detail})
    print(f"  {icon} {status:4s} · {name} — {detail}")


def md_table(rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    MD.append("| " + " | ".join(cols) + " |")
    MD.append("|" + "|".join("---" for _ in cols) + "|")
    for r in rows:
        MD.append("| " + " | ".join(_fmt(r[c]) for c in cols) + " |")
    MD.append("")


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v)


def section(title: str) -> None:
    print("\n" + "═" * 72 + f"\n  {title}\n" + "═" * 72)
    MD.append(f"\n## {title}\n")


# ══════════════════════════════════════════════════════════════════════
#  BTS
# ══════════════════════════════════════════════════════════════════════

def profile_bts(spark, months: int | None) -> None:
    section("BTS On-Time Performance")
    files = bts_csv_files()
    if months:
        files = files[-months:]
    if not files:
        check("BTS · files present", "FAIL", "no CSV files — run ./scripts/run_job.sh bts")
        return

    airports = read_airports(spark).cache()
    flights = read_bts_csv(spark, files).cache()
    mon = monitored_airports()

    per_month = (flights.groupBy(F.date_format("FlightDate", "yyyy-MM").alias("month"))
                 .agg(F.count("*").alias("flights"),
                      F.round(100 * F.avg("Cancelled"), 2).alias("cancelled_pct"),
                      F.round(100 * F.avg("ArrDel15"), 2).alias("arr_delayed_pct"))
                 .orderBy("month").collect())
    rows = [r.asDict() for r in per_month]
    total = sum(r["flights"] for r in rows)
    REPORT["bts_months"] = rows
    MD.append(f"{len(files)} months, **{total:,} flights**.\n")
    md_table(rows)
    print(f"  {len(files)} months · {total:,} flights")
    check("BTS · historical months", "PASS" if len(rows) >= 6 else "WARN",
          f"{len(rows)} months ({rows[0]['month']} → {rows[-1]['month']}); at least 6 are recommended for a stable p95 baseline")


    arr = (flights.filter(F.col("Dest").isin(list(mon)))
           .groupBy("Dest")
           .agg(F.count("*").alias("arrivals"),
                F.countDistinct("FlightDate").alias("days"),
                F.first("DestCityMarketID").alias("city_market_id"),
                F.first("DestCityName").alias("city_name"))
           .withColumn("arrivals_per_day", F.round(F.col("arrivals") / F.col("days"), 1))
           .orderBy(F.desc("arrivals")).collect())
    rows = [dict(r.asDict(), city_id=mon[r["Dest"]]) for r in arr]
    REPORT["bts_monitored_airports"] = rows
    MD.append("\n### Monitored airports\n")
    md_table(rows)
    missing = sorted(set(mon) - {r["Dest"] for r in rows})
    check("BTS · all 12 monitored airports", "PASS" if not missing else "FAIL",
          "complete" if not missing else f"missing {missing}")


    ev = (flights.filter(F.col("Dest").isin(list(mon)) & (F.col("Cancelled") == 0) & (F.col("Diverted") == 0))
          .withColumn("slot", (F.col("CRSArrTime") / 100).cast("int") * 4 + ((F.col("CRSArrTime") % 100) / 15).cast("int")))
    dens = (ev.groupBy("Dest", "FlightDate", "slot").count()
            .groupBy("Dest")
            .agg(F.round(F.avg("count"), 2).alias("avg_per_active_15min"),
                 F.max("count").alias("max_per_15min"),
                 F.count("*").alias("active_windows"))
            .orderBy(F.desc("avg_per_active_15min")).collect())
    rows = [r.asDict() for r in dens]
    REPORT["bts_density"] = rows
    MD.append("\n### Arrival density by 15-minute window\n")
    md_table(rows)
    thin = [r["Dest"] for r in rows if r["avg_per_active_15min"] < 1.5]
    check("BTS · sufficient 15-minute density", "PASS" if not thin else "WARN",
          "all airports average at least 1.5 flights per active window" if not thin
          else f"sparse: {thin} — interpret alerts using destination-level aggregation")


    all_ap = (flights.select(F.col("Origin").alias("iata")).union(flights.select(F.col("Dest").alias("iata")))
              .distinct())
    no_tz = all_ap.join(airports, "iata", "left_anti").collect()
    n_ap = all_ap.count()
    no_tz_list = sorted(r["iata"] for r in no_tz)
    affected = flights.filter(F.col("Origin").isin(no_tz_list) & F.col("Dest").isin(list(mon))).count() if no_tz_list else 0
    REPORT["bts_airports_without_tz"] = no_tz_list
    check("Time zones · airport coverage in OpenFlights", "PASS" if affected == 0 else "WARN",
          f"{n_ap - len(no_tz_list)}/{n_ap} airports have time zones; missing {no_tz_list or 'none'}; "
          f"{affected:,} flights to monitored airports affected")


    utc = add_utc_times(flights.filter(F.col("Dest").isin(list(mon))), airports)
    ok = (utc.filter(F.col("arr_utc").isNotNull() & F.col("ArrTime").isNotNull())
          .withColumn("arr_local", F.from_utc_timestamp("arr_utc", F.col("dest_tz")))
          .withColumn("hhmm", F.hour("arr_local") * 100 + F.minute("arr_local"))
          .withColumn("arr_time_norm", F.col("ArrTime") % 2400)
          .agg(F.count("*").alias("n"),
               F.sum((F.col("hhmm") == F.col("arr_time_norm")).cast("int")).alias("exact"),
               F.sum((F.abs(F.col("hhmm") - F.col("arr_time_norm")) <= 1).cast("int")).alias("within_1"),
               F.sum((F.to_date("arr_local") != F.col("FlightDate")).cast("int")).alias("next_day"))
          .collect()[0].asDict())
    pct = 100 * ok["exact"] / max(ok["n"], 1)
    REPORT["tz_validation"] = ok
    MD.append("\n### UTC conversion validation\n")
    MD.append(f"Derived arrival from scheduled departure, duration, and delay versus `ArrTime`: "
              f"**{ok['exact']:,}/{ok['n']:,} exact minute matches ({pct:.2f} %)**. "
              f"{ok['next_day']:,} arrivals on a different local date from `FlightDate`.\n")
    check("Time zones · derived arrivals match ArrTime", "PASS" if pct >= 99 else ("WARN" if pct >= 95 else "FAIL"),
          f"{pct:.2f} % exact minute matches; {ok['next_day']:,} arrivals on a different date from FlightDate")


    odd = flights.agg(
        F.sum((F.col("CRSArrTime") == 2400).cast("int")).alias("crs_arr_2400"),
        F.sum((F.col("ArrTime") == 2400).cast("int")).alias("arr_2400"),
        F.sum(F.col("CRSElapsedTime").isNull().cast("int")).alias("crs_elapsed_null"),
        F.sum(((F.col("Cancelled") == 0) & (F.col("Diverted") == 0) & F.col("ArrDelay").isNull()).cast("int")).alias("arr_delay_null_not_cancelled"),
    ).collect()[0].asDict()
    REPORT["bts_odd_values"] = odd
    MD.append(f"Unusual values: {odd}\n")
    check("BTS · unusual values", "PASS" if odd["crs_elapsed_null"] == 0 else "WARN", json.dumps(odd))

    flights.unpersist()


# ══════════════════════════════════════════════════════════════════════
#  Inside Airbnb
# ══════════════════════════════════════════════════════════════════════

def profile_airbnb(spark) -> None:
    section("Inside Airbnb")
    listing_rows, occ_rows, review_rows = [], [], []
    reviewer_frames = []

    for d in DESTINATIONS:
        cid = d["city_id"]
        for snap in sorted(d["airbnb_snapshots"]):
            try:
                L = read_listings(spark, cid, snap).cache()
                C = read_calendar(spark, cid, snap)
            except FileNotFoundError as exc:
                check(f"Airbnb · {cid} {snap}", "FAIL", str(exc))
                continue

            active = L.filter((F.col("availability_365") > 0) & (F.col("number_of_reviews_ltm") > 0))
            lr = L.agg(F.count("*").alias("listings"),
                       F.sum(F.col("accommodates").isNull().cast("int")).alias("accommodates_null"),
                       F.sum(F.col("latitude").isNull().cast("int")).alias("latlon_null"),
                       F.countDistinct("neighbourhood").alias("neighbourhoods")).collect()[0].asDict()
            ar = active.agg(F.count("*").alias("active"), F.sum("accommodates").alias("active_guests")).collect()[0]
            listing_rows.append({"city": cid, "snapshot": snap, **lr,
                                 "active": ar["active"], "active_capacity_guests": ar["active_guests"]})

            cal = (C.join(active.select("listing_id", "accommodates"), "listing_id")
                   .withColumn("ahead", F.datediff("night_date", "snapshot_date")))
            cr = cal.agg(
                F.count("*").alias("calendar_rows_active"),
                F.min("night_date").alias("first_night"),
                F.max("night_date").alias("last_night"),
                F.round(100 * F.sum(F.when(F.col("ahead") < 30, F.col("is_unavailable") * F.col("accommodates")))
                        / F.sum(F.when(F.col("ahead") < 30, F.col("accommodates"))), 1).alias("occ_pct_0_29"),
                F.round(100 * F.sum(F.when(F.col("ahead").between(30, 89), F.col("is_unavailable") * F.col("accommodates")))
                        / F.sum(F.when(F.col("ahead").between(30, 89), F.col("accommodates"))), 1).alias("occ_pct_30_89"),
                F.round(100 * F.sum(F.when(F.col("ahead") >= 270, F.col("is_unavailable") * F.col("accommodates")))
                        / F.sum(F.when(F.col("ahead") >= 270, F.col("accommodates"))), 1).alias("occ_pct_270_plus"),
            ).collect()[0].asDict()
            occ_rows.append({"city": cid, "snapshot": snap, **{k: str(v) if hasattr(v, "isoformat") else v for k, v in cr.items()}})
            print(f"  {cid:14s} {snap}  listings {lr['listings']:>6,}  active {ar['active']:>6,}  "
                  f"calendar {cr['calendar_rows_active']:>10,}  occupancy 0–29 nights {cr['occ_pct_0_29']}%")
            L.unpersist()


        snap = max(d["airbnb_snapshots"])
        try:
            R = read_reviews(spark, cid, snap).cache()
        except FileNotFoundError as exc:
            check(f"Airbnb · review {cid}", "FAIL", str(exc))
            continue
        w = Window.partitionBy("reviewer_id").orderBy("review_date")
        trips = (R.withColumn("gap", F.datediff("review_date", F.lag("review_date").over(w)))
                  .withColumn("new_trip", F.when(F.col("gap").isNull() | (F.col("gap") > settings.same_trip_days), 1).otherwise(0)))
        per = trips.groupBy("reviewer_id").agg(F.sum("new_trip").alias("trips"))
        rr = R.agg(F.count("*").alias("reviews"),
                   F.countDistinct("reviewer_id").alias("reviewers"),
                   F.min("review_date").alias("first"),
                   F.max("review_date").alias("last"),
                   F.sum((F.col("review_date") >= "2022-01-01").cast("int")).alias("reviews_since_2022")).collect()[0].asDict()
        rep = per.agg(F.sum((F.col("trips") >= 2).cast("int")).alias("repeat_guests")).collect()[0]["repeat_guests"]
        review_rows.append({"city": cid, "snapshot": snap, "reviews": rr["reviews"], "reviewers": rr["reviewers"],
                            "first": str(rr["first"]), "last": str(rr["last"]),
                            "reviews_since_2022": rr["reviews_since_2022"],
                            "repeat_guests_same_city": rep,
                            "repeat_pct": round(100 * rep / max(rr["reviewers"], 1), 2)})
        reviewer_frames.append(R.select("reviewer_id", "city_id").distinct())
        R.unpersist()

    MD.append("\n### Listings\n")
    md_table(listing_rows)
    MD.append("\n### Calendar — unavailable share by nights ahead\n")
    md_table(occ_rows)
    MD.append("\n### Review\n")
    md_table(review_rows)
    REPORT.update(airbnb_listings=listing_rows, airbnb_calendar=occ_rows, airbnb_reviews=review_rows)

    if listing_rows:
        bad = [r for r in listing_rows if r["accommodates_null"] > 0.01 * r["listings"]]
        check("Airbnb · complete accommodates values", "PASS" if not bad else "WARN",
              "less than 1% missing in every snapshot" if not bad else f"many missing values: {[(r['city'], r['snapshot']) for r in bad]}")
        n_cities = len({r["city"] for r in listing_rows})
        check("Airbnb · six destinations and two snapshots each", "PASS" if len(listing_rows) >= 12 else "FAIL",
              f"{len(listing_rows)} snapshot / {n_cities} destinations")
        far = [r for r in occ_rows if r["occ_pct_270_plus"] and r["occ_pct_30_89"]
               and r["occ_pct_270_plus"] > r["occ_pct_30_89"]]
        check("Airbnb · distant calendar blocking", "WARN" if far else "PASS",
              f"{len(far)}/{len(occ_rows)} snapshots have higher unavailable share at 270+ nights than at 30–89 nights "
              f"→ likely blocked calendars; prefer the {settings.occ_horizon_nights} night horizon")


    bts_months = {r["month"] for r in REPORT.get("bts_months", [])}
    overlap = []
    for r in occ_rows:
        first, last = r["first_night"], r["last_night"]
        hit = sorted(m for m in bts_months if first[:7] <= m <= last[:7])
        overlap.append({"city": r["city"], "snapshot": r["snapshot"], "bts_months_covered": ", ".join(hit) or "—"})
    MD.append("\n### Overlap: Airbnb calendar nights and downloaded BTS months\n")
    md_table(overlap)
    if bts_months:
        n_hit = sum(1 for o in overlap if o["bts_months_covered"] != "—")
        check("BTS ↔ Airbnb · time overlap", "PASS" if n_hit >= 6 else "FAIL",
              f"{n_hit}/{len(overlap)} snapshots have nights overlapping downloaded BTS months")


    if reviewer_frames:
        allr = reviewer_frames[0]
        for f in reviewer_frames[1:]:
            allr = allr.unionByName(f)
        multi = allr.groupBy("reviewer_id").agg(F.countDistinct("city_id").alias("n")).groupBy("n").count().orderBy("n").collect()
        rows = [{"destination_count": r["n"], "guests": r["count"]} for r in multi]
        MD.append("\n### Reviewers across multiple destinations\n")
        md_table(rows)
        REPORT["reviewers_by_city_count"] = rows
        n_multi = sum(r["guests"] for r in rows if r["destination_count"] >= 2)
        check("BR10 · reviewers across at least two destinations", "PASS" if n_multi >= 1000 else "WARN",
              f"{n_multi:,} guests reviewed listings in at least two of six destinations")
        rep_total = sum(r["repeat_guests_same_city"] for r in review_rows)
        check("BR10 · repeat guests in the same destination", "PASS" if rep_total >= 5000 else "WARN",
              f"{rep_total:,} guests made at least two trips (separated by more than {settings.same_trip_days} days) to the same destination")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["bts", "airbnb"])
    ap.add_argument("--bts-months", type=int, default=None, help="only profile the latest N months")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    with job_run("profile_sources", br="COMMON", step="profile source data") as run:
        spark = build_spark("profile_sources", local=args.local, shuffle_partitions=12)
        MD.append("# Source data profile — Data Pipeline\n")
        MD.append(f"Generated by `profiling.profile_sources` at {REPORT['generated_at']}.\n")
        if args.only in (None, "bts"):
            profile_bts(spark, args.bts_months)
        if args.only in (None, "airbnb"):
            profile_airbnb(spark)

        section("Check summary")
        md_table([{"check": c["name"], "result": c["status"], "details": c["detail"]} for c in REPORT["checks"]])

        out = Path(settings.report_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "profile_sources.json").write_text(json.dumps(REPORT, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        (out / "profile_sources.md").write_text("\n".join(MD), encoding="utf-8")
        print(f"\n  Report: {out}/profile_sources.md")
        n_fail = sum(c["status"] == "FAIL" for c in REPORT["checks"])
        n_warn = sum(c["status"] == "WARN" for c in REPORT["checks"])
        print(f"  {len(REPORT['checks'])} check · {n_fail} FAIL · {n_warn} WARN")
        run.rows = len(REPORT["checks"])
        spark.stop()


if __name__ == "__main__":
    main()
