"""Contract tests for BR7: local hour-of-week, forecast rule, replay events, runner."""

import csv
import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs" / "common"))
from br7_rules import (MIN_WARNING_ARRIVALS, TOURIST_ZONES, event_id, forecast_rows,
                       hour_of_week, normalize_event, predict)


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def utc(*parts):
    return datetime(*parts, tzinfo=timezone.utc)


class HourOfWeekTests(unittest.TestCase):
    def test_monday_midnight_new_york_is_zero(self):
        # 2024-11-04 is a Monday; 05:00Z = 00:00 EST.
        self.assertEqual(hour_of_week(utc(2024, 11, 4, 5)), 0)

    def test_sunday_last_hour_is_167(self):
        self.assertEqual(hour_of_week(utc(2024, 11, 11, 4)), 167)

    def test_daylight_saving_shift_uses_local_clock(self):
        # 18:00 local on a Tuesday is 22:00Z in EDT and 23:00Z in EST.
        self.assertEqual(hour_of_week(utc(2024, 10, 29, 22)), 24 + 18)
        self.assertEqual(hour_of_week(utc(2024, 11, 5, 23)), 24 + 18)


class ForecastTests(unittest.TestCase):
    def test_usual_hour_forecasts_usual_next_hour(self):
        self.assertEqual(predict(100, 100, 200, 250), (200.0, "normal"))

    def test_busy_hour_scales_next_hour_and_warns(self):
        predicted, level = predict(150, 100, 200, 250)
        self.assertEqual((predicted, level), (300.0, "warning"))

    def test_ratio_is_clamped(self):
        self.assertEqual(predict(1000, 100, 200, 10_000)[0], 600.0)
        self.assertEqual(predict(0, 100, 200, 10_000)[0], 100.0)

    def test_small_counts_never_warn(self):
        predicted, level = predict(12, 10, 20, 15)  # 24 > p90 15, but below the minimum
        self.assertEqual(predicted, 24.0)
        self.assertLess(predicted, MIN_WARNING_ARRIVALS)
        self.assertEqual(level, "normal")

    def test_missing_current_baseline_uses_usual_level(self):
        self.assertEqual(predict(500, None, 200, 250), (200.0, "normal"))

    def test_forecast_rows_targets_next_hour_and_flags_missing_baseline(self):
        hour = utc(2024, 11, 5, 22)  # Tuesday 17:00 EST
        baseline = {(230, hour_of_week(hour)): (100.0, 130.0),
                    (230, hour_of_week(utc(2024, 11, 5, 23))): (200.0, 250.0)}
        rows = forecast_rows([(230, hour, 150, 170), (43, hour, 10, 12)], baseline)
        self.assertEqual(rows[0], [230, hour, utc(2024, 11, 5, 23), 150, 170, 100.0, 300.0, 250.0, "warning"])
        self.assertEqual(rows[1][-1], "no_baseline")
        self.assertIsNone(rows[1][6])


class EventTests(unittest.TestCase):
    def row(self, **updates):
        row = {
            "event_id": event_id("2024-11-01T04:00:00Z", "2024-11-01T04:12:00Z", 161, 230, 1.2, 15.5),
            "dropoff_zone_id": "230", "pickup_zone_id": "161",
            "dropoff_time": "2024-11-01T04:12:00Z", "passenger_count": "2",
        }
        row.update(updates)
        return row

    def test_valid_event_is_normalized_to_utc(self):
        event = normalize_event(self.row())
        self.assertEqual(event["dropoff_zone_id"], 230)
        self.assertEqual(event["dropoff_time"], "2024-11-01T04:12:00+00:00")

    def test_rejects_non_tourist_zone_naive_time_and_bad_id(self):
        for update in ({"dropoff_zone_id": "1"}, {"dropoff_time": "2024-11-01T04:12:00"},
                       {"event_id": "fake"}, {"passenger_count": "-1"}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                normalize_event(self.row(**update))

    def test_missing_passenger_count_becomes_zero(self):
        self.assertEqual(normalize_event(self.row(passenger_count=""))["passenger_count"], 0)

    def test_producer_reads_prepared_csv(self):
        producer = load("br7_producer_test", "ingestion/producers/produce_br7.py")
        with tempfile.TemporaryDirectory() as directory:
            with (Path(directory) / "part-00000.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(self.row()))
                writer.writeheader()
                writer.writerow(self.row())
            self.assertEqual([e["dropoff_zone_id"] for e in producer.events(Path(directory))], [230])


class RunnerTests(unittest.TestCase):
    def test_br7_is_discovered_with_its_fact_table(self):
        runner = load("br7_runner_test", "scripts/start.py")
        self.assertIn("7", runner.discover_brs())
        self.assertEqual(runner.fact_tables_in(ROOT / "sql/clickhouse/ddl_br7.sql"), ["fact_br7_zone_hourly"])

    def test_resetting_br7_only_touches_its_topic(self):
        topics = load("br7_topics_test", "scripts/topics.py")
        self.assertEqual(topics.discover_topics(["7"]), ["br7_taxi_dropoffs"])
        self.assertNotIn("br7_taxi_dropoffs", topics.discover_topics(["3", "4"]))

    def test_tourist_zone_list_is_stable(self):
        self.assertEqual(len(TOURIST_ZONES), 10)
        self.assertIn(230, TOURIST_ZONES)


if __name__ == "__main__":
    unittest.main()
