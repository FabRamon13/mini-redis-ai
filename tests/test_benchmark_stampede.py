import unittest

from benchmarks.benchmark_stampede import metric_delta
from benchmarks.benchmark_stampede import percentile
from benchmarks.benchmark_stampede import poll_one
from benchmarks.benchmark_stampede import summarize_records


class SequenceClient:
    def __init__(self, responses=None, error=None):
        self.responses = list(responses or [])
        self.error = error

    def get_job(self, job_id):
        if self.error is not None:
            raise self.error
        return self.responses.pop(0)


class StampedeBenchmarkTests(unittest.TestCase):
    def test_percentile_interpolates_and_handles_empty_input(self):
        self.assertIsNone(percentile([], 0.50))
        self.assertEqual(percentile([10, 20, 30], 0.50), 20)
        self.assertEqual(percentile([10, 20], 0.95), 19.5)

    def test_percentile_rejects_invalid_quantile(self):
        with self.assertRaises(ValueError):
            percentile([1], 1.1)

    def test_metric_delta_uses_backend_counters(self):
        delta = metric_delta(
            {"provider_call_count": 4, "semantic_cache_hits": 2},
            {
                "provider_call_count": 8,
                "semantic_cache_hits": 98,
                "semantic_cache_misses": 4,
            },
        )

        self.assertEqual(delta["provider_call_count"], 4)
        self.assertEqual(delta["semantic_cache_hits"], 96)
        self.assertEqual(delta["semantic_cache_misses"], 4)

    def test_summary_counts_failures_and_latency_percentiles(self):
        records = [
            {"status": "completed", "latency_ms": 10.0, "cache": "miss"},
            {"status": "completed", "latency_ms": 20.0, "cache": "hit"},
            {"status": "failed", "latency_ms": 30.0, "cache": None},
            {"status": "timed_out", "latency_ms": 40.0, "cache": None},
            {"status": "poll_error", "latency_ms": 5.0, "cache": None},
            {"status": "submission_error", "latency_ms": None, "cache": None},
        ]

        summary = summarize_records(records)

        self.assertEqual(summary["submitted_jobs"], 5)
        self.assertEqual(summary["completed_jobs"], 2)
        self.assertEqual(summary["failed_jobs"], 1)
        self.assertEqual(summary["timed_out_jobs"], 1)
        self.assertEqual(summary["poll_error_jobs"], 1)
        self.assertEqual(summary["submission_errors"], 1)
        self.assertEqual(summary["cache_hits"], 1)
        self.assertEqual(summary["cache_misses"], 1)
        self.assertEqual(summary["latency_ms"]["p50"], 15.0)

    def test_poll_one_returns_terminal_failure(self):
        client = SequenceClient(responses=[{"status": "failed", "error": "boom"}])
        times = iter([1.0, 1.0, 1.1])

        record = poll_one(
            client,
            {"job_id": "job-1", "submitted_monotonic": 1.0},
            timeout_seconds=5,
            poll_seconds=0.1,
            clock=lambda: next(times),
            sleeper=lambda _: None,
        )

        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["error"], "boom")

    def test_poll_one_records_polling_error(self):
        client = SequenceClient(error=RuntimeError("connection lost"))
        times = iter([1.0, 1.0, 1.1])

        record = poll_one(
            client,
            {"job_id": "job-1", "submitted_monotonic": 1.0},
            timeout_seconds=5,
            poll_seconds=0.1,
            clock=lambda: next(times),
            sleeper=lambda _: None,
        )

        self.assertEqual(record["status"], "poll_error")
        self.assertIn("connection lost", record["error"])

    def test_poll_one_records_timeout(self):
        client = SequenceClient(responses=[{"status": "queued"}])
        times = iter([1.0, 1.0, 6.0, 6.0])

        record = poll_one(
            client,
            {"job_id": "job-1", "submitted_monotonic": 1.0},
            timeout_seconds=5,
            poll_seconds=0.1,
            clock=lambda: next(times),
            sleeper=lambda _: None,
        )

        self.assertEqual(record["status"], "timed_out")


if __name__ == "__main__":
    unittest.main()
