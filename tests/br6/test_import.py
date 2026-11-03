"""Unit tests for the BR6 import helpers that need no Spark session or
ClickHouse: settings catalog, reference files and small dimensions."""

import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "br6_route"))
sys.path.insert(0, str(ROOT / "jobs" / "common"))

import checkin_source as source  # noqa: E402
import route_settings  # noqa: E402
import route_star as star  # noqa: E402

CLUSTER = {"worker_memory_mb": 2048, "worker_cores": 2}
DATA = ROOT / "data" / "br6"


class Settings(unittest.TestCase):
    def test_defaults_and_presets_are_valid(self):
        catalog = route_settings.CATALOG
        for name in catalog.presets:
            errors, warnings = catalog.validate(catalog.resolve(preset=name), CLUSTER)
            self.assertEqual(errors, {}, name)
            self.assertEqual(warnings, {}, name)

    def test_spark_conf_reads_timestamps_as_utc(self):
        conf = route_settings.spark_conf(route_settings.CATALOG.defaults())
        self.assertEqual(conf["spark.sql.session.timeZone"], "UTC")
        self.assertEqual(conf["spark.eventLog.dir"], "file:/opt/logs/spark-events")

    def test_executor_larger_than_worker_blocks(self):
        catalog = route_settings.CATALOG
        values = catalog.resolve({"spark.executor.memory": "4g"})
        errors, _ = catalog.validate(values, CLUSTER)
        self.assertIn("spark.executor.memory", errors)

    def test_baseline_options(self):
        options = route_settings.CATALOG.options(route_settings.CATALOG.resolve(preset="baseline"))
        pipeline = options["pipeline"]
        self.assertEqual((pipeline["writer"], pipeline["write_partitions"]), ("pandas", 2))
        self.assertEqual((pipeline["matching"], pipeline["distance"], pipeline["place_stats"], pipeline["reader"]),
                         ("all", "python_udf", "run", "driver"))
        self.assertFalse(pipeline["cache"])

    def test_rules_are_fractions_and_numbers(self):
        rules = route_settings.CATALOG.options(route_settings.CATALOG.defaults())["rules"]
        self.assertEqual((rules["weight_popularity"], rules["weight_fit"], rules["weight_closeness"]), (0.5, 0.3, 0.2))
        self.assertEqual((rules["general_weight_popularity"], rules["general_weight_closeness"]), (0.7, 0.3))
        self.assertEqual((rules["max_detour_km"], rules["top_n"], rules["min_place_visitors"]), (1.0, 10, 3))

    def test_weights_must_add_up(self):
        catalog = route_settings.CATALOG
        errors, _ = catalog.validate(catalog.resolve({"rules.weight_closeness": "30"}), CLUSTER)
        self.assertIn("rules.weight_popularity", errors)
        errors, _ = catalog.validate(catalog.resolve({"rules.general_weight_closeness": "40"}), CLUSTER)
        self.assertIn("rules.general_weight_popularity", errors)
        errors, _ = catalog.validate(catalog.resolve({"pipeline.storage_level": "OFF_HEAP"}), CLUSTER)
        self.assertIn("pipeline.storage_level", errors)

    def test_clickhouse_thread_limit(self):
        catalog = route_settings.CATALOG
        self.assertEqual(route_settings.clickhouse_query_settings(catalog.defaults()), {})
        off = catalog.resolve({"clickhouse.limit_threads_by_memory": "false", "clickhouse.max_threads": "4"})
        self.assertEqual(route_settings.clickhouse_query_settings(off),
                         {"max_threads": 4, "max_threads_min_free_memory_per_thread": 0})


class ReferenceFiles(unittest.TestCase):
    def test_every_city_file_has_a_city_row(self):
        cities = star.load_cities(DATA / "cities.csv")
        self.assertEqual(set(source.available_cities(str(DATA))), set(cities))

    def test_grouping_uses_known_place_types(self):
        grouping = star.load_grouping(DATA / "category_place_type.csv")
        self.assertEqual(len(grouping), 764)
        self.assertEqual(set(grouping.values()), set(star.PLACE_TYPES))


class SmallDimensions(unittest.TestCase):
    def test_city_rows_are_sorted_and_checked(self):
        reference = {"tokyo": {"city_name": "Tokyo", "country_code": "JP", "country_name": "Japan",
                               "timezone": "Asia/Tokyo"},
                     "beijing": {"city_name": "Beijing", "country_code": "CN", "country_name": "China",
                                 "timezone": "Asia/Shanghai"}}
        rows = star.city_rows([("tokyo", "JP", 35.6, 139.3), ("beijing", "CN", 39.9, 116.4)], reference)
        self.assertEqual([(r["city_key"], r["city_code"]) for r in rows], [(1, "beijing"), (2, "tokyo")])
        with self.assertRaises(star.ReferenceDataError):
            star.city_rows([("tokyo", "CN", 0, 0)], reference)

    def test_place_types_and_missing_categories(self):
        categories = [{"category_key": 1, "category_id": "a", "category_name": "Museum"},
                      {"category_key": 2, "category_id": "b", "category_name": "Brand new"}]
        types, mapping, missing = star.place_type_rows({"a": "Sights & culture"}, categories)
        self.assertEqual(len(types), 6)
        self.assertEqual([t["is_offered"] for t in types], [1, 1, 1, 1, 1, 0])
        self.assertEqual(mapping, [{"category_key": 1, "place_type_key": 1},
                                   {"category_key": 2, "place_type_key": 6}])
        self.assertEqual(missing, ["Brand new"])

    def test_date_rows_cover_every_day(self):
        rows = star.date_rows(date(2016, 2, 27), date(2016, 3, 1))
        self.assertEqual([r["date_key"] for r in rows], [20160227, 20160228, 20160229, 20160301])
        self.assertEqual(rows[0]["day_of_week"], 6)
        self.assertEqual(rows[0]["is_weekend"], 1)


class SourceProfile(unittest.TestCase):
    def test_file_profile_parses_quoted_values(self):
        rows, first, last = source.file_profile(DATA / "beijing_checkins.csv")
        self.assertEqual(rows, 1470)
        self.assertEqual(len(first), len(source.COLUMN_NAMES))
        self.assertEqual(first[0], "2013_113678")
        self.assertIn("Dashanzi Art District, Jiuxianqiao Lu", first[6])
        self.assertEqual(last[-1], "2018-06-02 06:03:00")


if __name__ == "__main__":
    unittest.main()
