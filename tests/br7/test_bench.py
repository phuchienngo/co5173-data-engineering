"""Unit tests for the BR7 benchmark (docs/plan/br7/plan-benchmark.md): presets,
settings sent to Spark and ClickHouse, inputs, the matrix, the run checks, the
quality rule and the SQL engine's queries. Standard library only, no Docker:
`make br7-test`.

The engines' answers themselves were checked on the real TLC data (I1 against
prepare.py's summary.json); the expected fingerprints are in tests/br7/benchmark.py EXPECTED.
"""

import ast
import importlib.util
import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOB = ROOT / "jobs" / "br7_tourist_arrival_alerts"
sys.path.insert(0, str(JOB))
sys.path.insert(0, str(ROOT / "jobs" / "common"))

import br7_bench_quality as quality  # noqa: E402
import br7_bench_runs as runs  # noqa: E402
import br7_bench_settings as settings  # noqa: E402
import br7_bench_sql as sql  # noqa: E402
from br7_rules import TOURIST_ZONES  # noqa: E402

_spec = importlib.util.spec_from_file_location("br7_benchmark", ROOT / "tests" / "br7" / "benchmark.py")
bench = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench)
CATALOG = settings.CATALOG
CLUSTER = {"worker_memory_mb": 2048, "worker_cores": 2}
DDL = (ROOT / "sql" / "clickhouse" / "ddl_br7.sql").read_text(encoding="utf-8")


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
        self.assertEqual(self.values["L1"]["pipeline.reader"], "single")
        self.assertEqual(self.values["L1"]["pipeline.udf"], "python")
        self.assertEqual(self.values["L1"]["spark.driver.memory"], "2g")

    def test_no_driver_reader(self):
        # Plan R1: 50.7 M trips through the driver would take the WSL host down.
        self.assertNotIn("driver", CATALOG.by_key["pipeline.reader"].choices)

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


class SettingsSent(unittest.TestCase):
    def test_spark_conf_of_the_baseline(self):
        conf = settings.spark_conf(bench.preset_values("L1")[1])
        self.assertEqual(conf["spark.sql.adaptive.enabled"], "false")
        self.assertEqual(conf["spark.sql.autoBroadcastJoinThreshold"], "-1")
        self.assertEqual(conf["spark.sql.session.timeZone"], "UTC")
        self.assertNotIn("pipeline.reader", conf)

    def test_clickhouse_thread_limit(self):
        self.assertEqual(settings.clickhouse_query_settings(bench.preset_values("HL1")[1]), {})
        self.assertEqual(settings.clickhouse_query_settings(bench.preset_values("HL2")[1]),
                         {"max_threads_min_free_memory_per_thread": 0})

    def test_options_reach_the_engines(self):
        opts = CATALOG.options(bench.preset_values("V2")[1])
        self.assertEqual(opts["pipeline"]["udf"], "pandas")
        self.assertIs(opts["pipeline"]["cache"], True)


class Inputs(unittest.TestCase):
    def test_inputs_match_the_launcher(self):
        self.assertEqual(bench.INPUTS, runs.INPUTS)
        launcher = ast.parse((ROOT / "scripts" / "br7.py").read_text(encoding="utf-8"))
        found = next(ast.literal_eval(n.value) for n in launcher.body
                     if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "INPUTS")
        self.assertEqual(found, runs.INPUTS)

    def test_months_are_prepare_br7s(self):
        tree = ast.parse((ROOT / "scripts" / "prepare_br7.py").read_text(encoding="utf-8"))
        values = {n.targets[0].id: n.value for n in tree.body if isinstance(n, ast.Assign)
                  and isinstance(n.targets[0], ast.Name)}
        self.assertEqual(ast.literal_eval(values["REPLAY_MONTH"]), runs.REPLAY_MONTH)
        trip_sha = ast.literal_eval(values["TRIP_SHA256"])
        self.assertEqual(sorted(trip_sha), runs.ALL_MONTHS)

    def test_training_months(self):
        self.assertEqual(len(runs.train_months("2025-09")), 12)
        self.assertEqual(runs.train_months("2026-03"), [f"2026-{m:02d}" for m in range(3, 9)])
        self.assertEqual(runs.train_months("2026-07"), ["2026-07", "2026-08"])
        self.assertEqual(runs.month_where("2026-07"), "source_month IN ('2026-07', '2026-08', '2026-09')")

    def test_bad_month_is_refused(self):
        for bad in ("2026-09", "2024-01", "2025-9", "x' OR 1=1 --", ""):
            with self.assertRaises(ValueError):
                runs.train_months(bad)


class Matrix(unittest.TestCase):
    def test_cells_per_block(self):
        self.assertEqual(len(bench.block_cells("S2")), 9)
        self.assertEqual(len(bench.block_cells("V")), 14)
        self.assertEqual(len(bench.block_cells("H3")), 6)

    def test_block_sizes_and_repeats(self):
        self.assertEqual(bench.BLOCKS, {"S1": 1, "S2": 2, "S3": 4, "V": 2})
        self.assertEqual(bench.CLICKHOUSE_BLOCKS, {"H1": 1, "H2": 2, "H3": 4})
        self.assertEqual(bench.BLOCK_REPEATS["V"], 3)
        self.assertEqual(bench.BLOCK_REPEATS["H2"], 5)

    def test_block_env_keeps_the_sizes(self):
        self.assertEqual(bench.block_env("S3")["BR5_WORKER_CORES"], "4")
        self.assertEqual(bench.block_env("H2")["BR5_CLICKHOUSE_CPUS"], "2")


class Checks(unittest.TestCase):
    SNAP = {"settings_from": "preset bench-default", "worker_cores": "2", "ch_threads": "auto(1)", "engine": "spark"}

    def row(self, **changes):
        read, fingerprint = bench.EXPECTED["I1"]
        row = {"block": "S2", "cell": "I1-L2", "input": "I1", "live": "L2", "preset": "bench-default",
               "repeat": "1", "run_id": "r1", "status": "succeeded", "rows_read": str(read),
               "fingerprint": fingerprint}
        row.update(changes)
        return row

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
        self.assertTrue(any("different result" in p for p in self.check(self.row(fingerprint="quality 1:2"))))

    def test_wrong_rows_read_or_size_fails(self):
        self.assertTrue(self.check(self.row(rows_read="5")))
        self.assertTrue(self.check(self.row(), {**self.SNAP, "worker_cores": "4"}))

    def test_expected_covers_every_input(self):
        self.assertEqual(set(bench.EXPECTED), set(bench.INPUTS))
        for read, fingerprint in bench.EXPECTED.values():
            self.assertGreater(read, 0)
            self.assertEqual([p.split(" ")[0] for p in fingerprint.split(" · ")], list(runs.FINGERPRINT_PARTS))


class QualityRule(unittest.TestCase):
    """br7_bench_quality.quality_label (Python UDF) is prepare.py's rule."""
    S, E = 1_000_000_000_000_000, 1_000_000_000_000_000 + 30 * 86400 * 10**6
    T = S + 86400 * 10**6

    def label(self, pickup=T, dropoff=T + 600 * 10**6, pu=1, do=161, dist=2.5, amount=20.0):
        return quality.quality_label(self.S, self.E, pickup, dropoff, pu, do, dist, amount)

    def test_limits_are_prepare_pys(self):
        tree = ast.parse((JOB / "prepare.py").read_text(encoding="utf-8"))
        consts = {n.targets[0].id: n.value for n in tree.body if isinstance(n, ast.Assign)
                  and isinstance(n.targets[0], ast.Name)}
        self.assertEqual(eval(compile(ast.Expression(consts["MAX_TRIP_SECONDS"]), "", "eval")),
                         quality.MAX_TRIP_SECONDS)
        self.assertEqual(ast.literal_eval(consts["MAX_TRIP_MILES"]), quality.MAX_TRIP_MILES)
        source = (JOB / "prepare.py").read_text(encoding="utf-8")
        self.assertEqual(re.findall(r'"(\w+)"\)\n', source[source.index("reason = ("):source.index(".otherwise")]),
                         quality.LABELS)

    def test_labels_in_order(self):
        self.assertEqual(self.label(), "ok")
        self.assertEqual(self.label(pu=None), "missing_field")
        self.assertEqual(self.label(dropoff=None, dist=-1), "missing_field")
        self.assertEqual(self.label(dropoff=self.E), "dropoff_outside_file_month")
        self.assertEqual(self.label(dropoff=self.S - 1, pickup=self.S - 10**9), "dropoff_outside_file_month")
        self.assertEqual(self.label(dropoff=self.T), "dropoff_not_after_pickup")
        self.assertEqual(self.label(dropoff=self.T + 6 * 3600 * 10**6), "ok")
        self.assertEqual(self.label(dropoff=self.T + (6 * 3600 + 1) * 10**6), "trip_longer_than_6h")
        self.assertEqual(self.label(dist=0), "invalid_distance")
        self.assertEqual(self.label(dist=100.01), "invalid_distance")
        self.assertEqual(self.label(dist=100), "ok")
        self.assertEqual(self.label(amount=0), "nonpositive_amount")

    def test_nulls_pass_like_spark(self):
        self.assertEqual(self.label(dist=None, amount=None), "ok")

    def test_seconds_are_floored(self):
        # 0.9 s apart but in two different seconds: one second, as cast(long) gives.
        self.assertEqual(self.label(pickup=self.T + 999_999, dropoff=self.T + 1_100_000), "ok")
        self.assertEqual(self.label(pickup=self.T + 100_000, dropoff=self.T + 900_000), "dropoff_not_after_pickup")


class FakeClient:
    def __init__(self):
        self.sql = []
        self.inserts = []

    def query(self, text, parameters=None, settings=None):
        self.sql.append((text, parameters))

        class Result:
            def named_results(self):
                return iter([{"n": 3, "z": 161, "zone_id": z, "zone_name": f"zone {z}", "borough": "Manhattan",
                              "source_month": "2026-09", "quality": "ok", "ok_trips": 5, "trips": 4}
                             for z in TOURIST_ZONES])
        return Result()

    def command(self, text, parameters=None, settings=None):
        self.sql.append((text, parameters))

    def insert(self, table, rows, column_names=None):
        self.inserts.append((table, rows, column_names))


class Ctx:
    def __init__(self, first_month="2026-07"):
        self.client = FakeClient()
        self.train_months = runs.train_months(first_month)
        self.replay_month = runs.REPLAY_MONTH
        self.where = runs.month_where(first_month)
        self.opts = {"pipeline": {}}
        self.query_settings = {"log_comment": "r1"}


RUN_ID = "00000000-0000-0000-0000-000000000001"


class SqlQueries(unittest.TestCase):
    def run_engine(self, first_month="2026-07"):
        ctx = Ctx(first_month)
        engine = sql.SqlEngine(ctx)
        q = engine.quality()
        m = engine.clean()
        engine.baseline(RUN_ID)
        engine.replay(RUN_ID)
        engine.write(RUN_ID, q, m)
        return ctx.client

    def test_every_parameter_is_bound(self):
        for text, params in self.run_engine().sql:
            for name in re.findall(r"\{(\w+):\w+\}", text):
                self.assertIn(name, params or {}, text[:200])

    def test_month_filter_reaches_every_raw_read(self):
        for text, _ in self.run_engine("2026-03").sql:
            if "raw_trip" in text:
                self.assertIn("source_month IN ('2026-03'", text)

    def test_baseline_reads_training_months_only_and_replay_the_replay_month(self):
        texts = [t for t, _ in self.run_engine().sql]
        baseline = next(t for t in texts if "run_baseline" in t and "INSERT" in t)
        replay = next(t for t in texts if "run_replay" in t and "INSERT" in t)
        self.assertIn("source_month IN ('2026-07', '2026-08')", baseline)
        self.assertIn("HAVING n >= 8", baseline)
        self.assertIn("source_month = '2026-09'", replay)

    def test_training_hours(self):
        # 2026-07 00:00 New York (EDT) = 04:00 UTC; July + August = 62 days.
        start, hours = sql.training_hours(["2026-07", "2026-08"])
        self.assertEqual(start.strftime("%Y-%m-%d %H:%M"), "2026-07-01 04:00")
        self.assertEqual(hours, 62 * 24)
        # 12 months over both DST changes: 365 days.
        self.assertEqual(sql.training_hours(runs.train_months("2025-09"))[1], 365 * 24)

    def test_small_tables_are_written(self):
        client = self.run_engine()
        tables = [t for t, _, _ in client.inserts]
        self.assertEqual(tables, ["br7.run_quality", "br7.run_month", "br7.run_zone"])
        zones = client.inserts[2][1]
        self.assertEqual([r[1] for r in zones], sorted(TOURIST_ZONES))
        self.assertEqual(zones[0][4], TOURIST_ZONES[sorted(TOURIST_ZONES)[0]])

    def test_event_id_follows_spark(self):
        self.assertIn("'.0'", sql.EVENT_ID)  # 12.0 -> "12.0"
        self.assertIn("arrayFilter(x -> isNotNull(x)", sql.EVENT_ID)  # concat_ws skips nulls
        self.assertIn("%Y-%m-%dT%H:%i:%SZ", sql.EVENT_ID)


class Ddl(unittest.TestCase):
    def test_benchmark_tables_are_never_truncated_by_start_py(self):
        names = re.findall(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_.]+)", DDL, re.IGNORECASE)
        bench_tables = [n for n in names if n.startswith("br7.")]
        self.assertEqual(sorted(bench_tables), sorted(
            f"br7.{t}" for t in ["zone", "raw_trip", "pipeline_runs", "settings", "run_quality", "run_month",
                                 "run_baseline", "run_replay", "run_zone"]))
        self.assertFalse([n for n in bench_tables if n.split(".")[-1].startswith("fact_")])

    def test_grafana_view_expects_the_driver_fingerprints(self):
        for input_id, month in bench.INPUTS.items():
            self.assertIn(f"'{month}'", DDL)
            self.assertIn(f"'{bench.EXPECTED[input_id][1]}'", DDL, input_id)

    def test_dashboards_read_existing_view_columns(self):
        view = DDL[DDL.index("CREATE OR REPLACE VIEW br7.run_history"):]
        columns = set(re.findall(r"\bAS (\w+)", view))
        for name in ("what", "input_name", "variant", "hardware", "correct", "label", "fingerprint", "block", "cell",
                     "repeat", "duration_s", "outside_steps_s", "baseline_cells", "replay_events"):
            self.assertIn(name, columns)
        for board in ("br7-benchmark", "br7-benchmark-run"):
            text = (ROOT / "grafana" / "dashboards" / f"{board}.json").read_text(encoding="utf-8")
            json.loads(text)
            for used in set(re.findall(r"br7\.run_history\b", text)):
                self.assertTrue(used)

    def test_run_columns_match_run_store(self):
        import run_store
        block = DDL[DDL.index("CREATE TABLE IF NOT EXISTS br7.pipeline_runs"):]
        block = block[:block.index("ENGINE")]
        self.assertEqual(re.findall(r"^\s{4}(\w+)\s", block, re.MULTILINE), run_store.RUN_COLUMNS)

    def test_fingerprint_reads_the_result_tables(self):
        for table in ("run_quality", "run_month", "run_baseline", "run_replay", "run_zone"):
            self.assertIn(f"br7.{table}", runs.FINGERPRINT_SQL)
        self.assertNotIn("passenger_count", runs.FINGERPRINT_SQL)


if __name__ == "__main__":
    unittest.main()
