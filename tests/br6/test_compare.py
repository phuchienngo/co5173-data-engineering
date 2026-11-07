"""Unit tests for the Run History comparison (route_compare.settings_diff,
plan-monitoring.md H5). No Spark or ClickHouse needed."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "br6_route"))
sys.path.insert(0, str(ROOT / "jobs" / "common"))

import route_compare  # noqa: E402


class SettingsDiff(unittest.TestCase):
    def test_same_settings(self):
        snap = {"pipeline.matching": "grid", "effective.spark.sql.adaptive.enabled": "true"}
        self.assertEqual(route_compare.settings_diff(snap, dict(snap)), [])

    def test_differences_and_missing_keys(self):
        a = {"pipeline.matching": "grid", "pipeline.distance": "native", "launch.settings_from": "current settings"}
        b = {"pipeline.matching": "all", "pipeline.distance": "native", "launch.settings_from": "preset baseline",
             "rules.top_n": "10"}
        self.assertEqual(route_compare.settings_diff(a, b), [
            ("pipeline.matching", "grid", "all"),
            ("rules.top_n", None, "10"),
            ("launch.settings_from", "current settings", "preset baseline"),
        ])

    def test_order(self):
        a = {"launch.settings_from": "x", "grouping.sha1": "1", "cluster.cores": "2",
             "effective.spark.sql.shuffle.partitions": "200", "spark.sql.shuffle.partitions": "200"}
        b = {k: v + "!" for k, v in a.items()}
        self.assertEqual([k for k, _, _ in route_compare.settings_diff(a, b)], [
            "spark.sql.shuffle.partitions", "effective.spark.sql.shuffle.partitions", "cluster.cores",
            "grouping.sha1", "launch.settings_from"])


if __name__ == "__main__":
    unittest.main()
