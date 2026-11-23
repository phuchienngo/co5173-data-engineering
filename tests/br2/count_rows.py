"""BR2 row accounting: raw TLC rows -> lake fact_trips -> baseline hourly input, with rows dropped per filter.

Mirrors the filters of jobs/br2_dynamic_pricing/pricing.py `read_trips` (in the same order, so the drops are
sequential) and job.py `run_baseline` (baseline months, 16 landmark zones). The lake count is cross-checked
against `read_trips` itself, so a change to the real filters shows up as a MISMATCH.
Read-only: reads data/br2/raw, writes nothing (no lake, no ClickHouse).

Usage: python3 tests/br2/count_rows.py [--service yellow] [--baseline-from 2025-07] [--baseline-to 2026-06]
Needs the spark-local container up. The script is copied to /tmp in that container and run with spark-submit.
"""
import argparse
import glob
import re
import subprocess
import sys

CONTAINER_SCRIPT = "/tmp/count_rows.py"


def launch(argv):
    subprocess.run(["docker", "cp", __file__, f"spark-local:{CONTAINER_SCRIPT}"], check=True)
    cmd = ["docker", "exec", "-i", "spark-local", "spark-submit", "--master", "spark://spark-master:7077",
           "--driver-memory", "512m", "--executor-memory", "512m", "--conf", "spark.jars.ivy=/tmp/.ivy2",
           "--conf", "spark.cores.max=1", "--conf", "spark.sql.shuffle.partitions=4",
           "--py-files", "/opt/jobs/common/spark_session.py,/opt/jobs/br2_dynamic_pricing/pricing.py",
           CONTAINER_SCRIPT, "--spark", *argv]
    sys.exit(subprocess.run(cmd).returncode)


def count(args):
    from pyspark.sql import DataFrame, functions as F
    from functools import reduce
    from pricing import LANDMARKS, RAW_DIR, SERVICE_COLUMNS, read_trips
    from spark_session import get_spark_session

    spark = get_spark_session("br2-count-rows")
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # as job.py: TLC timestamps are wall-clock
    spark.sparkContext.setLogLevel("ERROR")
    files = sorted(glob.glob(f"{RAW_DIR}/{args.service}/*.parquet"))
    pickup_col, fare_col = SERVICE_COLUMNS[args.service]

    frames = []
    for path in files:
        month = re.search(r"(\d{4}-\d{2})\.parquet$", path).group(1)
        t = spark.read.parquet(path).select(
            F.col(pickup_col).cast("timestamp").alias("pickup"), F.col(fare_col).cast("double").alias("fare"),
            F.col("PULocationID").cast("int").alias("pu"), F.col("DOLocationID").cast("int").alias("do"))
        # first failing condition, in the order read_trips applies them
        reason = (F.when(F.col("pickup").isNull(), "1 pickup_datetime null")
                  .when(F.date_format("pickup", "yyyy-MM") != month, "2 pickup date outside the file's month")
                  .when(F.col("fare").isNull(), "3 fare_amount null")
                  .when(F.col("fare") <= 0, "4 fare_amount <= 0")
                  .when(F.col("fare") >= 500, "5 fare_amount >= 500")
                  .when(F.col("pu").isNull() | ~F.col("pu").between(1, 263), "6 pu_location_id null or not in 1..263")
                  .when(F.col("do").isNull() | ~F.col("do").between(1, 263), "7 do_location_id null or not in 1..263")
                  .when(~F.lit(month).between(args.baseline_from, args.baseline_to), "8 month outside baseline range")
                  .when(~F.col("do").isin(list(LANDMARKS)), "9 do_location_id not in the 16 landmark zones")
                  .otherwise("10 used for hourly"))
        frames.append(t.select(reason.alias("reason")))
    counts = {r["reason"]: r["count"] for r in reduce(DataFrame.unionByName, frames).groupBy("reason").count().collect()}
    n = lambda prefix: sum(v for k, v in counts.items() if int(k.split()[0]) in prefix)

    raw, lake_dropped = sum(counts.values()), n(range(1, 8))
    lake, used = raw - lake_dropped, n({10})
    lake_check = read_trips(spark, args.service, files).count()

    print(f"{'step':<48}{'rows':>14}")
    print(f"{'raw (' + str(len(files)) + ' files)':<48}{raw:>14,}")
    for k in sorted(counts, key=lambda k: int(k.split()[0])):
        if int(k.split()[0]) <= 7:
            print(f"{'  dropped: ' + k.split(' ', 1)[1]:<48}{counts[k]:>14,}")
    print(f"{'lake fact_trips (after normalization)':<48}{lake:>14,}")
    print(f"{'  cross-check read_trips().count()':<48}{lake_check:>14,}  {'OK' if lake_check == lake else 'MISMATCH'}")
    for k in sorted(counts, key=lambda k: int(k.split()[0])):
        if int(k.split()[0]) in (8, 9):
            print(f"{'  dropped: ' + k.split(' ', 1)[1]:<48}{counts[k]:>14,}")
    print(f"{'used for hourly (bookings)':<48}{used:>14,}")
    print(f"{'total dropped (raw - used)':<48}{raw - used:>14,}")
    print(f"{'  (a) data quality (steps 1-7)':<48}{lake_dropped:>14,}")
    print(f"{'  (b) scope filtering (steps 8-9)':<48}{n({8, 9}):>14,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", default="yellow")
    parser.add_argument("--baseline-from", default="2025-07")
    parser.add_argument("--baseline-to", default="2026-06")
    parser.add_argument("--spark", action="store_true", help="internal: run inside spark-local")
    args = parser.parse_args()
    if args.spark:
        count(args)
    else:
        launch(sys.argv[1:])
