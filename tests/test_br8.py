"""Contract tests for the BR8 identities and UTC daily-visit definition."""

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs" / "common"))
from br8_rules import is_attraction, normalize_event, visit_id


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class VisitContractTests(unittest.TestCase):
    def row(self, **updates):
        row = {
            "user_id": "101", "place_id": "foursquare:site-a", "visit_date": "2018-05-01",
            "visit_time": "2018-05-01T10:00:00Z", "raw_checkins": "3",
            "event_id": visit_id(101, "foursquare:site-a", "2018-05-01"),
        }
        row.update(updates)
        return row

    def test_replay_uses_same_visit_identity(self):
        a = normalize_event(self.row())
        b = normalize_event(self.row(visit_time="2018-05-01T17:00:00Z"))
        self.assertEqual(a["event_id"], b["event_id"])
        self.assertEqual(a["raw_checkins"], 3)

    def test_distinct_day_or_place_has_different_identity(self):
        first = visit_id(101, "site-a", "2018-05-01")
        self.assertNotEqual(first, visit_id(101, "site-a", "2018-05-02"))
        self.assertNotEqual(first, visit_id(101, "site-b", "2018-05-01"))

    def test_timezone_conversion_uses_utc_day(self):
        event = normalize_event(self.row(visit_time="2018-05-02T01:00:00+09:00"))
        self.assertEqual(event["visit_date"], "2018-05-01")
        self.assertEqual(event["visit_time"], "2018-05-01T16:00:00+00:00")

    def test_naive_timestamp_rejected(self):
        with self.assertRaises(ValueError):
            normalize_event(self.row(visit_time="2018-05-01T10:00:00"))

    def test_corrupt_date_or_id_rejected(self):
        for update in ({"visit_date": "2018-05-02"}, {"event_id": "fake"}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                normalize_event(self.row(**update))

    def test_invalid_user_or_count_rejected(self):
        for update in ({"user_id": "0"}, {"raw_checkins": "0"}, {"raw_checkins": "-1"}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                normalize_event(self.row(**update))

    def test_attraction_filter_excludes_regular_businesses(self):
        self.assertTrue(is_attraction("schema:Museum", []))
        self.assertTrue(is_attraction(None, ["Arts and Entertainment", "Theme Park"]))
        self.assertFalse(is_attraction("schema:CafeOrCoffeeShop", ["Coffee Shop"]))
        self.assertFalse(is_attraction("schema:TrainStation", ["Rail Station"]))
        self.assertFalse(is_attraction(None, ["Landmarks and Outdoors > Structure"]))
        self.assertFalse(is_attraction(None, ["Landmarks and Outdoors > States and Municipalities > Neighborhood"]))
        self.assertTrue(is_attraction(None, ["Landmarks and Outdoors > Historic and Protected Site"]))

    def test_producer_validates_prepared_records(self):
        producer = load("br8_producer_test", "ingestion/producers/produce_br8.py")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with (path / "part-00000.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(self.row()))
                writer.writeheader()
                writer.writerow(self.row())
            self.assertEqual(list(producer.events(path))[0]["user_id"], 101)

    def test_topic_selection_will_not_reset_another_br(self):
        topics = load("br8_topics_test", "scripts/topics.py")
        self.assertEqual(topics.discover_topics(["8"]), ["br8_tourist_visits"])
        self.assertEqual(topics.discover_topics(["7"]), [])

    def test_runner_discovers_br8(self):
        runner = load("br8_runner_test", "scripts/start.py")
        self.assertEqual(runner.discover_brs(), ["8"])
        self.assertEqual(runner.fact_tables_in(ROOT / "sql/clickhouse/ddl_br8.sql"), ["fact_br8_visits"])


if __name__ == "__main__":
    unittest.main()
