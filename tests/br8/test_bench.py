"""Unit tests for the BR8 benchmark (docs/plan/br7/plan-benchmark.md): presets,
settings sent to Spark and ClickHouse, the matrix, the run checks and the SQL
engine's queries. Standard library only, no Docker: `make br8-test`.

The engines' answers themselves were checked on the real TripWorld data
(ClickHouse engine against an independent pandas version of prepare_br8.py);
the expected fingerprints are in tests/br8/benchmark.py EXPECTED.
"""

import importlib.util
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "br8_tourist_retention"))
sys.path.insert(0, str(ROOT / "jobs" / "common"))

import br8_bench_runs as runs  # noqa: E402
import br8_bench_settings as settings  # noqa: E402
from br8_bench_sql import SqlEngine  # noqa: E402
from br8_rules import ATTRACTION_PATTERN, is_attraction  # noqa: E402

_spec = importlib.util.spec_from_file_location("br8_benchmark", ROOT / "tests" / "br8" / "benchmark.py")
bench = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench)
CATALOG = settings.CATALOG
CLUSTER = {"worker_memory_mb": 2048, "worker_cores": 2}


def diff(a, b):
    return {k for k in a if a[k] != b[k]}


class Presets(unittest.TestCase):
    def setUp(self):
        self.values = {live: bench.preset_values(live)[1] for live in bench.LIVE}

    def test_every_preset_is_valid(self):
        for live, values in self.values.items():
            errors, _ = CATALOG.validate(values, CLUSTER)
            self.assertEqual(errors, {}, live)

    def test_preset_names_are_unique_and_prefixed(self):
        names = [bench.LIVE[live][0] for live in bench.LIVE]
        self.assertEqual(len(names), len(set(names)))
        self.assertTrue(all(n.startswith("bench-") for n in names))

    def test_baseline_turns_every_optimization_off(self):
        self.assertEqual(diff(self.values["L1"], self.values["L2"]), {
            "pipeline.reader", "pipeline.udf", "pipeline.cache", "pipeline.writer",
            "spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.enabled", "spark.driver.memory"})
        self.assertEqual(self.values["L1"]["pipeline.reader"], "driver")
        self.assertEqual(self.values["L1"]["pipeline.udf"], "python")

    def test_third_setting_and_block_v_change_one_thing(self):
        self.assertEqual(diff(self.values["L3"], self.values["L2"]), {"pipeline.read_partitions"})
        for live in ("V1", "V2", "V3", "V4", "V5", "V6"):
            self.assertEqual(len(diff(self.values[live], self.values["L2"])), 1, live)

    def test_every_spark_preset_takes_all_cores(self):
        for live in ("L1", "L2", "L3", "V1", "V2", "V3", "V4", "V5", "V6"):
            self.assertEqual(self.values[live]["spark.cores.max"], "4", live)
            self.assertEqual(self.values[live]["pipeline.engine"], "spark", live)

    def test_clickhouse_presets(self):
        self.assertEqual(self.values["HL1"]["pipeline.engine"], "clickhouse")
        self.assertEqual(diff(self.values["HL2"], self.values["HL1"]), {"clickhouse.limit_threads_by_memory"})

    def test_analysis_rules_never_change(self):
        for live, values in self.values.items():
            self.assertEqual(values["rules.max_places"], "0", live)


class SettingsSent(unittest.TestCase):
    def test_spark_conf_of_the_baseline(self):
        conf = settings.spark_conf(bench.preset_values("L1")[1])
        self.assertEqual(conf["spark.sql.adaptive.enabled"], "false")
        self.assertEqual(conf["spark.sql.autoBroadcastJoinThreshold"], "-1")
        self.assertEqual(conf["spark.sql.session.timeZone"], "UTC")
        self.assertEqual(conf["spark.eventLog.dir"], "file:/opt/logs/spark-events")
        self.assertNotIn("pipeline.reader", conf)

    def test_clickhouse_thread_limit(self):
        self.assertEqual(settings.clickhouse_query_settings(bench.preset_values("HL1")[1]), {})
        self.assertEqual(settings.clickhouse_query_settings(bench.preset_values("HL2")[1]),
                         {"max_threads_min_free_memory_per_thread": 0})

    def test_options_reach_the_engines(self):
        opts = CATALOG.options(bench.preset_values("V2")[1])
        self.assertEqual(opts["pipeline"]["udf"], "pandas")
        self.assertIs(opts["pipeline"]["cache"], True)
        self.assertEqual(opts["rules"]["max_places"], 0)


class Inputs(unittest.TestCase):
    def test_inputs_match_the_launcher(self):
        self.assertEqual(bench.INPUTS, runs.INPUTS)

    def test_region_filter(self):
        self.assertEqual(runs.region_where("ALL"), "1")
        self.assertEqual(runs.region_where("Q1490"), "region_id = 'Q1490'")
        for bad in ("Q1'; DROP TABLE x", "Q1 OR 1=1", ""):
            with self.assertRaises(ValueError):
                runs.region_where(bad)


class Matrix(unittest.TestCase):
    def test_cells_per_block(self):
        self.assertEqual(len(bench.block_cells("S2")), 9)
        self.assertEqual(len(bench.block_cells("V")), 14)
        self.assertEqual(len(bench.block_cells("H3")), 6)
        self.assertEqual(bench.block_cells("S1", ["I3-L3"]), ["I3-L3"])

    def test_unknown_cell_is_refused(self):
        with self.assertRaises(SystemExit):
            bench.block_cells("H1", ["I1-L2"])

    def test_block_sizes_and_repeats(self):
        self.assertEqual(bench.BLOCKS, {"S1": 1, "S2": 2, "S3": 4, "V": 2})
        self.assertEqual(bench.CLICKHOUSE_BLOCKS, {"H1": 1, "H2": 2, "H3": 4})
        self.assertEqual(bench.BLOCK_REPEATS.get("S2", 2), 2)
        self.assertEqual(bench.BLOCK_REPEATS["V"], 3)
        self.assertEqual(bench.BLOCK_REPEATS["H2"], 5)

    def test_block_env_keeps_the_sizes(self):
        self.assertEqual(bench.block_env("S3")["BR5_WORKER_CORES"], "4")
        self.assertEqual(bench.block_env("H2")["BR5_CLICKHOUSE_CPUS"], "2")
        self.assertEqual(bench.block_env("H2")["BR5_WORKER_CORES"], "2")


class Checks(unittest.TestCase):
    def row(self, **changes):
        read, fingerprint = bench.EXPECTED["I1"]
        row = {"block": "S2", "cell": "I1-L2", "input": "I1", "live": "L2", "preset": "bench-default",
               "repeat": "1", "run_id": "r1", "status": "succeeded", "rows_read": str(read),
               "fingerprint": fingerprint}
        row.update(changes)
        return row

    SNAP = {"settings_from": "preset bench-default", "worker_cores": "2", "ch_threads": "auto(1)", "engine": "spark"}

    def check(self, row, snap=None, block="S2", threads=None):
        original = bench.read_manifest
        bench.read_manifest = lambda: []
        try:
            return bench.check_row(row, snap or self.SNAP, block, threads or {})
        finally:
            bench.read_manifest = original

    def test_a_good_run(self):
        self.assertEqual(self.check(self.row()), [])

    def test_a_different_answer_fails(self):
        problems = self.check(self.row(fingerprint="visits 1:2 · places 3:4"))
        self.assertTrue(any("different result" in p for p in problems))

    def test_wrong_rows_read_fails(self):
        self.assertTrue(self.check(self.row(rows_read="5")))

    def test_wrong_preset_or_size_fails(self):
        self.assertTrue(self.check(self.row(), {**self.SNAP, "settings_from": "current settings"}))
        self.assertTrue(self.check(self.row(), {**self.SNAP, "worker_cores": "4"}))
        self.assertTrue(self.check(self.row(block="S1"), block="S1"))

    def test_clickhouse_block_checks(self):
        snap = {**self.SNAP, "engine": "clickhouse", "ch_threads": "auto(2)", "settings_from": "preset bench-clickhouse"}
        row = self.row(block="H2", cell="I1-HL1", live="HL1", preset="bench-clickhouse")
        self.assertEqual(self.check(row, snap, "H2", {"r1": (5, 1, 0, 0)}), [])
        self.assertTrue(self.check(row, {**snap, "ch_threads": "auto(1)"}, "H2"))
        hl2 = self.row(block="H2", cell="I1-HL2", live="HL2", preset="bench-clickhouse-nomemlimit")
        snap2 = {**snap, "settings_from": "preset bench-clickhouse-nomemlimit"}
        self.assertEqual(self.check(hl2, snap2, "H2", {"r1": (5, 2, 5, 0)}), [])
        self.assertTrue(self.check(hl2, snap2, "H2", {"r1": (5, 1, 0, 0)}))
        self.assertTrue(self.check(row, snap, "H2", {"r1": (5, 1, 0, 1)}))  # query cache used

    def test_failed_run(self):
        self.assertEqual(self.check(self.row(status="failed")), ["status failed"])

    def test_expected_covers_every_input(self):
        self.assertEqual(set(bench.EXPECTED), set(bench.INPUTS))


class AttractionTests(unittest.TestCase):
    TEXTS = [("schema:Museum", []), ("schema:Restaurant", ["Café"]), (None, ["Art Museum"]),
             ("schema:LandmarksOrHistoricalBuildings", []), ("schema:Place", ["Theme Park"]),
             ("schema:Place", ["Landmark"]), ("schema:Place", ["Landmarks"]), ("", ["National Park"]),
             ("schema:Zoo", None), ("schema:Bar", ["Scenic Lookout"])]

    def test_python_rule_and_pattern_agree(self):
        # The native (rlike) and SQL (match) tests use the text "schema categories"; is_attraction too.
        for schema, cats in self.TEXTS:
            text = f"{schema or ''} {' '.join(cats or [])}"
            self.assertEqual(is_attraction(schema, cats), bool(re.search(ATTRACTION_PATTERN, text)), text)
        self.assertTrue(is_attraction("schema:Museum", []))
        self.assertFalse(is_attraction("schema:Restaurant", ["Café"]))
        self.assertFalse(is_attraction("schema:Place", ["Landmarks"]))  # \blandmark\b


class FakeClient:
    def __init__(self):
        self.sql = []

    def query(self, sql, parameters=None, settings=None):
        self.sql.append((sql, parameters))

        class Result:
            def named_results(self):
                return iter([{"n": 7, "valid": 5, "total": 7, "used": 4, "is_attraction": 1}])
        return Result()

    def command(self, sql, parameters=None, settings=None):
        self.sql.append((sql, parameters))


class Ctx:
    def __init__(self, region="ALL", max_places=0):
        self.client = FakeClient()
        self.where = runs.region_where(region)
        self.opts = {"rules": {"max_places": max_places}, "pipeline": {}}
        self.query_settings = {"log_comment": "r1"}


class SqlQueries(unittest.TestCase):
    def run_engine(self, region="Q1490", max_places=0):
        ctx = Ctx(region, max_places)
        engine = SqlEngine(ctx)
        self.assertEqual(engine.read(), 7)
        self.assertEqual(engine.clean(), ({"invalid": 2, "duplicate": 1}, 4))
        engine.match()
        engine.aggregate("00000000-0000-0000-0000-000000000001")
        engine.write("00000000-0000-0000-0000-000000000001")
        return ctx.client.sql

    def test_every_parameter_is_bound(self):
        for sql, params in self.run_engine():
            for name in re.findall(r"\{(\w+):\w+\}", sql):
                self.assertIn(name, params or {}, sql[:200])

    def test_region_filter_reaches_every_read(self):
        for sql, _ in self.run_engine("Q1490"):
            if "raw_checkin" in sql:
                self.assertIn("region_id = 'Q1490'", sql)

    def test_no_aggregate_alias_shadows_a_filtered_column(self):
        # ClickHouse lets an alias replace a column of the same name in WHERE: an
        # aggregate aliased as a source column name breaks the query.
        for sql, _ in self.run_engine():
            for func, column, alias in re.findall(r"\b(any|min|max|sum|count)\((\w+)\) AS (\w+)", sql):
                self.assertNotEqual(column, alias, f"{func}({column}) AS {alias} in: {sql[:300]}")

    def test_top_places(self):
        sql = " ".join(s for s, _ in self.run_engine("ALL", 20))
        self.assertIn("LIMIT 20", sql)
        self.assertIn("ORDER BY uniqExact(user_id) DESC, place_id", sql)

    def test_writes_go_to_the_result_tables(self):
        writes = [s for s, _ in self.run_engine() if s.strip().startswith("INSERT")]
        self.assertEqual(len(writes), 2)
        self.assertIn("br8.run_visit", writes[0])
        self.assertIn("br8.run_place", writes[1])


class Ddl(unittest.TestCase):
    def test_benchmark_tables_are_never_truncated_by_start_py(self):
        ddl = (ROOT / "sql" / "clickhouse" / "ddl_br8.sql").read_text()
        names = re.findall(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_.]+)", ddl, re.IGNORECASE)
        bench_tables = [n for n in names if n.startswith("br8.")]
        self.assertEqual(sorted(bench_tables), sorted(
            f"br8.{t}" for t in ["region", "place", "raw_checkin", "pipeline_runs", "settings", "run_visit",
                                 "run_place"]))
        self.assertFalse([n for n in bench_tables if n.split(".")[-1].startswith("fact_")])

    def test_grafana_view_expects_the_driver_fingerprints(self):
        # br8.run_history marks a run "✓ correct" with the same answers check uses.
        ddl = (ROOT / "sql" / "clickhouse" / "ddl_br8.sql").read_text()
        for input_id, region in bench.INPUTS.items():
            self.assertIn(f"'{region}'", ddl)
            self.assertIn(f"'{bench.EXPECTED[input_id][1]}'", ddl, input_id)

    def test_dashboards_read_existing_view_columns(self):
        import json
        ddl = (ROOT / "sql" / "clickhouse" / "ddl_br8.sql").read_text()
        view = ddl[ddl.index("CREATE OR REPLACE VIEW br8.run_history"):]
        columns = set(re.findall(r"\bAS (\w+)", view))
        for name in ("what", "input_name", "variant", "hardware", "correct", "label", "fingerprint", "block", "cell",
                     "repeat", "duration_s", "outside_steps_s"):
            self.assertIn(name, columns)
        for board in ("br8-benchmark", "br8-benchmark-run"):
            json.loads((ROOT / "grafana" / "dashboards" / f"{board}.json").read_text(encoding="utf-8"))

    def test_run_columns_match_run_store(self):
        import run_store
        ddl = (ROOT / "sql" / "clickhouse" / "ddl_br8.sql").read_text()
        block = ddl[ddl.index("CREATE TABLE IF NOT EXISTS br8.pipeline_runs"):]
        block = block[:block.index("ENGINE")]
        columns = re.findall(r"^\s{4}(\w+)\s", block, re.MULTILINE)
        self.assertEqual(columns, run_store.RUN_COLUMNS)


if __name__ == "__main__":
    unittest.main()
