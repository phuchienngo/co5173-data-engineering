"""prepare.py load_month()'s quality rule outside Spark: the limits (repeated,
since prepare.py needs pyspark; tests/br7/test_bench.py keeps them equal), the
labels in the rule's order, and the rule on one trip for the Python UDF
(br7_bench_spark.py). Standard library only."""

MAX_TRIP_SECONDS = 6 * 3600  # prepare.py
MAX_TRIP_MILES = 100  # prepare.py
LABELS = ["missing_field", "dropoff_outside_file_month", "dropoff_not_after_pickup", "trip_longer_than_6h",
          "invalid_distance", "nonpositive_amount"]


def quality_label(start_us, end_us, pickup_us, dropoff_us, pickup_zone, dropoff_zone, distance, amount):
    """The rule on one trip. Times are epoch microseconds (start_us/end_us: the
    file month's bounds); seconds are floored, as Spark's timestamp-to-long cast.
    A null distance or amount passes its test, as a null comparison does in Spark."""
    if pickup_us is None or dropoff_us is None or pickup_zone is None or dropoff_zone is None:
        return "missing_field"
    if dropoff_us < start_us or dropoff_us >= end_us:
        return "dropoff_outside_file_month"
    seconds = dropoff_us // 1_000_000 - pickup_us // 1_000_000
    if seconds <= 0:
        return "dropoff_not_after_pickup"
    if seconds > MAX_TRIP_SECONDS:
        return "trip_longer_than_6h"
    if distance is not None and (distance <= 0 or distance > MAX_TRIP_MILES):
        return "invalid_distance"
    if amount is not None and amount <= 0:
        return "nonpositive_amount"
    return "ok"
