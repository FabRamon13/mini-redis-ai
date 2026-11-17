import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))

from benchmark_mixed_workload import aggregate, summarize_jobs


class MixedWorkloadCalculationsTests(unittest.TestCase):
    def test_summarize_jobs_reports_queue_and_completion_percentiles(self):
        summary = summarize_jobs([
            {"status": "completed", "queue_wait_ms": 10, "completion_ms": 1010},
            {"status": "completed", "queue_wait_ms": 30, "completion_ms": 1030},
            {"status": "failed", "queue_wait_ms": 5, "completion_ms": 20},
        ])

        self.assertEqual(summary["completed"], 2)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["queue_wait_ms"]["p50"], 20)
        self.assertEqual(summary["completion_ms"]["p50"], 1020)

    def test_aggregate_compares_unrelated_completion_latency(self):
        def result(scenario, latency):
            return {
                "scenario": scenario,
                "unrelated": {
                    "queue_wait_ms": {"p50": latency - 100},
                    "completion_ms": {"p50": latency},
                },
                "metric_deltas": {"provider_call_count": 4},
            }

        summary = aggregate([
            result("control", 1000),
            result("control", 1100),
            result("mixed", 2000),
            result("mixed", 2200),
        ])

        self.assertEqual(summary["unrelated_completion_median_delta_ms"], 1050)
        self.assertEqual(summary["unrelated_completion_median_ratio"], 2.0)


if __name__ == "__main__":
    unittest.main()
