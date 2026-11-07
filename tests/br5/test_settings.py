"""Unit tests for jobs/br5_promotion/run_settings.py."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs" / "br5_promotion"))

import rules  # noqa: E402
import run_settings as rs  # noqa: E402

CLUSTER = {"worker_memory_mb": 2048, "worker_cores": 2}


class Defaults(unittest.TestCase):
    def test_defaults_are_valid(self):
        errors, warnings = rs.validate(rs.defaults(), CLUSTER)
        self.assertEqual(errors, {})
        self.assertEqual(warnings, {})

    def test_builtin_presets_are_valid(self):
        for name in rs.BUILTIN_PRESETS:
            errors, _ = rs.validate(rs.resolve(preset=name), CLUSTER)
            self.assertEqual(errors, {}, name)

    def test_rule_defaults_match_rules_module(self):
        self.assertEqual(rs.options(rs.defaults())["rules"] | {}, {
            **{k: v for k, v in rules.DEFAULT_RULES.items()}, "duplicates": "keep"})

    def test_every_setting_has_a_known_group(self):
        self.assertTrue(all(s.group in rs.GROUPS for s in rs.SETTINGS))


class Parse(unittest.TestCase):
    def test_types(self):
        self.assertEqual(rs.parse("spark.executor.memory", "1g"), 1024 * rs.MB)
        self.assertEqual(rs.parse("spark.executor.memory", "768m"), 768 * rs.MB)
        self.assertEqual(rs.parse("spark.sql.shuffle.partitions", "8"), 8)
        self.assertIs(rs.parse("spark.sql.adaptive.enabled", "False"), False)
        self.assertEqual(rs.parse("spark.sql.autoBroadcastJoinThreshold", "-1"), -1)
        self.assertIsNone(rs.parse("spark.executor.cores", ""))

    def test_rejections(self):
        for key, value in [
            ("spark.sql.shuffle.partitions", "abc"),
            ("spark.sql.shuffle.partitions", "0"),
            ("spark.sql.shuffle.partitions", "2.5"),
            ("spark.executor.memory", "lots"),
            ("spark.executor.memory", "100m"),        # below the minimum
            ("spark.sql.adaptive.enabled", "yes"),
            ("pipeline.engine", "duckdb"),
            ("spark.cores.max", ""),                  # not optional
            ("rules.tier1_label", ""),
        ]:
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    rs.parse(key, value)

    def test_validate_reports_the_key(self):
        errors, _ = rs.validate({**rs.defaults(), "rules.low_season_threshold": "120"})
        self.assertIn("rules.low_season_threshold", errors)


class Cluster(unittest.TestCase):
    def test_executor_larger_than_worker_is_an_error(self):
        errors, _ = rs.validate({**rs.defaults(), "spark.executor.memory": "4g"}, CLUSTER)
        self.assertIn("could ever start", errors["spark.executor.memory"])

    def test_overhead_over_worker_is_a_warning(self):
        errors, warnings = rs.validate({**rs.defaults(), "spark.executor.memory": "1800m"}, CLUSTER)
        self.assertEqual(errors, {})
        self.assertIn("spark.executor.memory", warnings)

    def test_executor_cores_above_worker_or_total(self):
        errors, _ = rs.validate({**rs.defaults(), "spark.executor.cores": "4", "spark.cores.max": "4"}, CLUSTER)
        self.assertIn("worker has", errors["spark.executor.cores"])
        errors, _ = rs.validate({**rs.defaults(), "spark.executor.cores": "2", "spark.cores.max": "1"}, CLUSTER)
        self.assertIn("total cores", errors["spark.executor.cores"])

    def test_not_all_executors_fit(self):
        values = {**rs.defaults(), "spark.executor.cores": "1", "spark.executor.memory": "1200m"}
        errors, warnings = rs.validate(values, CLUSTER)
        self.assertEqual(errors, {})
        self.assertIn("only 1 of the 2", warnings["spark.executor.memory"])

    def test_no_cluster_skips_cluster_checks(self):
        errors, _ = rs.validate({**rs.defaults(), "spark.executor.memory": "8g"}, None)
        self.assertEqual(errors, {})

    def test_cross_field_rules(self):
        errors, _ = rs.validate({**rs.defaults(), "spark.memory.offHeap.enabled": "true"})
        self.assertIn("spark.memory.offHeap.size", errors)
        errors, _ = rs.validate({**rs.defaults(), "pipeline.storage_level": "OFF_HEAP"})
        self.assertIn("pipeline.storage_level", errors)
        errors, _ = rs.validate({**rs.defaults(), "rules.last_minute_days": "30"})
        self.assertIn("rules.last_minute_days", errors)
        errors, _ = rs.validate({**rs.defaults(), "rules.tier2_below": "40"})
        self.assertIn("rules.tier2_below", errors)


class Resolve(unittest.TestCase):
    def test_merge_order_and_unknown_keys(self):
        values = rs.resolve({"spark.sql.shuffle.partitions": "8", "old.setting": "x"}, preset="baseline")
        self.assertEqual(values["spark.sql.shuffle.partitions"], "8")
        self.assertEqual(values["pipeline.reader"], "driver")
        self.assertNotIn("old.setting", values)
        self.assertEqual(len(values), len(rs.SETTINGS))

    def test_spark_conf(self):
        conf = rs.spark_conf(rs.resolve(preset="baseline"))
        self.assertEqual(conf["spark.sql.autoBroadcastJoinThreshold"], "-1")
        self.assertEqual(conf["spark.serializer"], rs.SERIALIZERS["java"])
        self.assertNotIn("spark.executor.cores", conf)      # unset
        self.assertNotIn("spark.driver.memory", conf)       # passed to spark-submit instead
        self.assertNotIn("pipeline.reader", conf)

    def test_options_and_changed(self):
        values = {**rs.defaults(), "rules.low_season_threshold": "70", "pipeline.read_partitions": "4"}
        opts = rs.options(values)
        self.assertAlmostEqual(opts["rules"]["low_season_threshold"], 0.7)
        self.assertEqual(opts["pipeline"]["read_partitions"], 4)
        self.assertEqual(sorted(rs.changed(values)), ["pipeline.read_partitions", "rules.low_season_threshold"])
        self.assertEqual(rs.clickhouse_query_settings(rs.defaults()), {})
        off = {**rs.defaults(), "clickhouse.limit_threads_by_memory": "false", "clickhouse.max_threads": "4"}
        self.assertEqual(rs.clickhouse_query_settings(off), {"max_threads": 4, "max_threads_min_free_memory_per_thread": 0})


if __name__ == "__main__":
    unittest.main()
