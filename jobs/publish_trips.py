"""BR1 replay: publish one month of fact_trips to Kafka in pickup order (simulated live feed)."""
import argparse
from pyspark.sql import SparkSession, functions as F
from common import FACT_PATH, KAFKA_BOOTSTRAP, TOPIC


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2026-07")
    parser.add_argument("--service", default="yellow")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("br1-publish-trips").getOrCreate()

    trips = spark.read.parquet(FACT_PATH).where(
        (F.col("service") == args.service) & (F.col("pickup_month") == args.month)
    )

    n = trips.count()
    if n == 0:
        raise SystemExit(
            f"no fact rows for {args.service} {args.month} - run batch_fact_capacity.py first"
        )

    # one sorted partition keeps Kafka in pickup-time order, so the stream's watermark
    # never drops replayed trips.
    (
        trips.repartition(1)
        .sortWithinPartitions("pickup_datetime")
        .select(
            F.to_json(
                F.struct(
                    "service", "pickup_datetime", "pu_location_id", "do_location_id", "fare_amount"
                )
            ).alias("value")
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC)
        .save()
    )

    print(f"published {n} {args.service} trips for {args.month} to {TOPIC}")
    spark.stop()


if __name__ == "__main__":
    main()
