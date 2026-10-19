"""Unit tests for the monitoring helpers: run_metrics.summarize_spark and
compare_runs.settings_diff (docs/plan/br5/plan-monitoring.md, section 8)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs" / "br5_promotion"))

import compare_runs  # noqa: E402
import run_metrics  # noqa: E402

# Shapes as returned by Spark's REST API (/allexecutors, /stages, /jobs), trimmed.
EXECUTORS = [
    {"id": "driver", "peakMemoryMetrics": {"JVMHeapMemory": 300}},
    {"id": "0", "peakMemoryMetrics": {"JVMHeapMemory": 200}},
    {"id": "1", "peakMemoryMetrics": {"JVMHeapMemory": 250}},
]
JOBS = [
    {"jobId": 0, "jobGroup": "read", "stageIds": [0]},
    {"jobId": 1, "jobGroup": "aggregate", "stageIds": [0, 1, 2]},  # stage 0 reused (skipped)
    {"jobId": 2, "stageIds": [3]},  # no group: before the first step
]


def stage(stage_id, cpu_ns=0, run_ms=0, shuffle_write=0, tasks=0, **extra):
    return {"stageId": stage_id, "attemptId": 0, "executorCpuTime": cpu_ns, "executorRunTime": run_ms,
            "shuffleWriteBytes": shuffle_write, "numCompleteTasks": tasks, **extra}


STAGES = [
    stage(0, cpu_ns=2_000_000, run_ms=10, tasks=2),
    stage(1, cpu_ns=3_000_000, run_ms=20, shuffle_write=500, tasks=4),
    stage(2, cpu_ns=1_000_000, run_ms=5, tasks=1, diskBytesSpilled=64),
    stage(3, cpu_ns=4_000_000, run_ms=7, tasks=1),
]


class SparkFigures(unittest.TestCase):
    def setUp(self):
        self.f = run_metrics.summarize_spark(EXECUTORS, STAGES, JOBS)

    def test_totals(self):
        self.assertEqual(self.f["spark.cpu_ms"], 10.0)
        self.assertEqual(self.f["spark.run_ms"], 42.0)
        self.assertEqual(self.f["spark.shuffle_write_bytes"], 500.0)
        self.assertEqual(self.f["spark.spill_disk_bytes"], 64.0)
        self.assertEqual(self.f["spark.tasks"], 8.0)
        self.assertEqual((self.f["spark.jobs"], self.f["spark.stages"]), (3.0, 4.0))

    def test_per_step_goes_to_the_first_job_of_a_stage(self):
        self.assertEqual(self.f["spark.cpu_ms@read"], 2.0)
        self.assertEqual(self.f["spark.cpu_ms@aggregate"], 4.0)
        self.assertEqual(self.f["spark.cpu_ms@setup"], 4.0)

    def test_steps_add_up_to_the_total(self):
        for name, _, _ in run_metrics.STAGE_FIGURES:
            steps = sum(v for k, v in self.f.items() if k.startswith(f"spark.{name}@"))
            self.assertAlmostEqual(steps, self.f[f"spark.{name}"], msg=name)

    def test_peak_heap_executors_and_driver(self):
        self.assertEqual(self.f["spark.executors"], 2.0)
        self.assertEqual(self.f["spark.executor_peak_heap_bytes"], 250.0)
        self.assertEqual(self.f["spark.driver_peak_heap_bytes"], 300.0)

    def test_missing_fields_and_empty_lists(self):
        f = run_metrics.summarize_spark([{"id": "0"}], [{"stageId": 0}], [])
        self.assertEqual(f["spark.cpu_ms"], 0.0)
        self.assertEqual(f["spark.cpu_ms@setup"], 0.0)
        self.assertNotIn("spark.executor_peak_heap_bytes", f)
        self.assertEqual(run_metrics.summarize_spark([], [], [])["spark.stages"], 0.0)

    def test_event_log_directory_created_only_when_on(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "events"
            run_metrics.prepare_event_log({"spark.eventLog.enabled": "false", "spark.eventLog.dir": f"file:{target}"})
            self.assertFalse(target.exists())
            run_metrics.prepare_event_log({"spark.eventLog.enabled": "true", "spark.eventLog.dir": f"file:{target}"})
            self.assertTrue(target.is_dir())


class SettingsDiff(unittest.TestCase):
    def test_identical_snapshots_have_no_difference(self):
        snap = {"spark.sql.adaptive.enabled": "true", "cluster.worker_cores": "2"}
        self.assertEqual(compare_runs.settings_diff(snap, dict(snap)), [])

    def test_differences_and_missing_keys_in_order(self):
        a = {"launch.settings_from": "current settings", "effective.spark.sql.adaptive.enabled": "true",
             "spark.sql.adaptive.enabled": "true", "pipeline.join": "late", "cluster.worker_cores": "2"}
        b = {"launch.settings_from": "preset baseline", "effective.spark.sql.adaptive.enabled": "false",
             "spark.sql.adaptive.enabled": "false", "pipeline.join": "late", "pipeline.cache": "false"}
        self.assertEqual(compare_runs.settings_diff(a, b), [
            ("pipeline.cache", None, "false"),
            ("spark.sql.adaptive.enabled", "true", "false"),
            ("effective.spark.sql.adaptive.enabled", "true", "false"),
            ("cluster.worker_cores", "2", None),
            ("launch.settings_from", "current settings", "preset baseline"),
        ])


if __name__ == "__main__":
    unittest.main()
