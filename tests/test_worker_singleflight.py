import tempfile
import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from redis_clone.client import FlightAcquireResult, FlightState
from redis_clone.server import Server
from worker.singleflight import FlightOwnershipLost, FlightWaitTimeout, derive_flight_key
from worker.worker import process_inference


def _bytes(value):
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


class InProcessClient:
    """Exercise real datastore commands without opening a TCP listener."""

    def __init__(self, server):
        self.server = server

    def get(self, key):
        return self.server.get(_bytes(key))

    def lrange(self, key, start, stop):
        return self.server.lrange(_bytes(key), _bytes(start), _bytes(stop))

    def incr(self, key):
        return self.server.incr(_bytes(key))

    def incrby(self, key, amount):
        return self.server.incrby(_bytes(key), _bytes(amount))

    def flight_acquire(self, key, owner_id, lease_ms):
        result = self.server.flight_acquire(_bytes(key), _bytes(owner_id), _bytes(lease_ms))
        return FlightAcquireResult(
            role=result["role"],
            state=result["state"],
            generation=result["generation"],
            expires_at_ms=result.get("expires_at_ms", 0),
            retry_at_ms=result.get("retry_at_ms", 0),
            takeover=bool(result.get("takeover", 0)),
        )

    def flight_renew(self, key, owner_id, generation, lease_ms):
        result = self.server.flight_renew(
            _bytes(key), _bytes(owner_id), _bytes(generation), _bytes(lease_ms)
        )
        if result == 0:
            return None
        return FlightState(
            state=result["state"],
            generation=result["generation"],
            expires_at_ms=result["expires_at_ms"],
        )

    def flight_status(self, key):
        result = self.server.flight_status(_bytes(key))
        if result is None:
            return None
        return FlightState(
            state=result["state"],
            generation=result["generation"],
            expires_at_ms=result.get("expires_at_ms", 0),
            retry_at_ms=result.get("retry_at_ms", 0),
            resolved_at_ms=result.get("resolved_at_ms", 0),
            error_code=result.get("error_code", ""),
            result_key=result.get("result_key", ""),
        )

    def flight_release(self, key, owner_id, generation):
        return self.server.flight_release(_bytes(key), _bytes(owner_id), _bytes(generation))

    def flight_fail(self, key, owner_id, generation, cooldown_ms, error_code):
        return self.server.flight_fail(
            _bytes(key),
            _bytes(owner_id),
            _bytes(generation),
            _bytes(cooldown_ms),
            _bytes(error_code),
        )

    def flight_commit(
        self,
        flight_key,
        owner_id,
        generation,
        cache_key,
        cache_payload,
        index_key,
        max_entries,
    ):
        return self.server.flight_commit(
            _bytes(flight_key),
            _bytes(owner_id),
            _bytes(generation),
            _bytes(cache_key),
            _bytes(cache_payload),
            _bytes(index_key),
            _bytes(max_entries),
        )


class NeverResolvingClient:
    def __init__(self):
        self.metrics = {}

    def incr(self, key):
        self.metrics[key] = self.metrics.get(key, 0) + 1
        return self.metrics[key]

    def incrby(self, key, amount):
        self.metrics[key] = self.metrics.get(key, 0) + int(amount)
        return self.metrics[key]

    def flight_acquire(self, key, owner_id, lease_ms):
        return FlightAcquireResult(
            role="FOLLOWER",
            state="ACTIVE",
            generation=1,
            expires_at_ms=9999999999999,
        )

    def flight_status(self, key):
        return FlightState("ACTIVE", 1, expires_at_ms=9999999999999)


class RenewalRejectingClient(InProcessClient):
    def flight_renew(self, key, owner_id, generation, lease_ms):
        return None


class CommitFailingClient(InProcessClient):
    def flight_commit(self, *args, **kwargs):
        raise ConnectionError("datastore unavailable during publication")


class CountingRenewClient(InProcessClient):
    def __init__(self, server):
        super().__init__(server)
        self.renewals = 0

    def flight_renew(self, key, owner_id, generation, lease_ms):
        self.renewals += 1
        return super().flight_renew(key, owner_id, generation, lease_ms)


class RecheckInsertingClient(InProcessClient):
    def __init__(self, server, entry_payload, cache_key):
        super().__init__(server)
        self.entry_payload = entry_payload
        self.cache_key = cache_key
        self.inserted = False

    def flight_acquire(self, key, owner_id, lease_ms):
        result = super().flight_acquire(key, owner_id, lease_ms)
        if result.role == "LEADER" and not self.inserted:
            self.server.set(_bytes(self.cache_key), _bytes(self.entry_payload))
            self.server.lpush(b"semantic_cache:index", _bytes(self.cache_key))
            self.inserted = True
        return result


class WorkerSingleFlightTests(unittest.TestCase):
    def make_server(self, tmpdir):
        return Server(port=0, aof_file=f"{tmpdir}/appendonly.aof")

    def test_healthy_concurrent_misses_execute_provider_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            start = threading.Barrier(8)
            provider_calls = 0
            provider_lock = threading.Lock()

            def provider(prompt, provider="fake"):
                nonlocal provider_calls
                with provider_lock:
                    provider_calls += 1
                time.sleep(0.08)
                return {"provider": provider, "answer": prompt}

            def run(index):
                start.wait()
                return process_inference(
                    {"id": f"job-{index}", "prompt": "same cold prompt", "provider": "fake"},
                    InProcessClient(server),
                )

            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", side_effect=provider),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
                patch("worker.worker.FLIGHT_WAIT_TIMEOUT_SECONDS", 3.0),
            ):
                with ThreadPoolExecutor(max_workers=8) as executor:
                    results = list(executor.map(run, range(8)))

            self.assertEqual(provider_calls, 1)
            self.assertEqual(server.get(b"metrics:provider_call_count"), b"1")
            self.assertEqual(server.get(b"metrics:flight_publications"), b"1")
            self.assertTrue(all(result["response"]["answer"] == "same cold prompt" for result in results))
            self.assertEqual(sum(result["cache"] == "miss" for result in results), 1)
            self.assertEqual(sum(result["cache"] == "hit" for result in results), 7)
            key = derive_flight_key(
                prompt="same cold prompt",
                provider="fake",
                model_id="hash-embedding-v1",
                model_revision="v1",
                generation_config={},
            )
            self.assertEqual(server.flight_status(_bytes(key))["state"], "COMPLETED")

    def test_provider_exception_marks_failed_flight_with_cooldown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            client = InProcessClient(server)
            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", side_effect=RuntimeError("provider failed")),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
            ):
                with self.assertRaisesRegex(RuntimeError, "provider failed"):
                    process_inference({"prompt": "failure", "provider": "fake"}, client)

            key = derive_flight_key(
                prompt="failure",
                provider="fake",
                model_id="hash-embedding-v1",
                model_revision="v1",
                generation_config={},
            )
            self.assertEqual(server.flight_status(_bytes(key))["state"], "FAILED")
            self.assertEqual(server.get(b"metrics:provider_call_count"), b"1")
            self.assertEqual(server.get(b"metrics:flight_failed"), b"1")

    def test_leader_rechecks_cache_after_acquisition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            entry = {
                "entry_id": "raced-entry",
                "prompt": "race",
                "provider": "fake",
                "model_id": "hash-embedding-v1",
                "model_revision": "v1",
                "embedding": [1.0, 0.0],
                "response": {"answer": "already published"},
                "embedding_dimensions": 2,
                "created_at": "test",
            }
            client = RecheckInsertingClient(
                server,
                json.dumps(entry),
                "semantic_cache:raced-entry",
            )
            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response") as provider,
            ):
                result = process_inference({"prompt": "race", "provider": "fake"}, client)

            self.assertEqual(result["cache"], "hit")
            self.assertEqual(result["response"]["answer"], "already published")
            provider.assert_not_called()
            self.assertEqual(server.get(b"metrics:flight_cache_recheck_hits"), b"1")

    def test_healthy_slow_provider_renews_lease(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            client = CountingRenewClient(server)

            def slow_provider(prompt, provider="fake"):
                time.sleep(0.08)
                return {"answer": "renewed"}

            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", side_effect=slow_provider),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
                patch("worker.worker.FLIGHT_LEASE_MS", 30),
            ):
                result = process_inference({"prompt": "renew", "provider": "fake"}, client)

            self.assertEqual(result["response"]["answer"], "renewed")
            self.assertGreaterEqual(client.renewals, 2)

    def test_orphaned_pre_provider_leader_expires_and_is_taken_over(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            key = derive_flight_key(
                prompt="orphan",
                provider="fake",
                model_id="hash-embedding-v1",
                model_revision="v1",
                generation_config={},
            )
            server.flight_acquire(_bytes(key), b"crashed-owner", b"25")
            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", return_value={"answer": "recovered"}),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
                patch("worker.worker.FLIGHT_LEASE_MS", 50),
                patch("worker.worker.FLIGHT_POLL_MIN_SECONDS", 0.005),
                patch("worker.worker.FLIGHT_POLL_MAX_SECONDS", 0.01),
                patch("worker.worker.FLIGHT_WAIT_TIMEOUT_SECONDS", 1.0),
            ):
                result = process_inference(
                    {"prompt": "orphan", "provider": "fake"},
                    InProcessClient(server),
                )

            self.assertEqual(result["response"]["answer"], "recovered")
            self.assertEqual(server.get(b"metrics:flight_takeovers"), b"1")
            self.assertEqual(server.flight_status(_bytes(key))["state"], "COMPLETED")

    def test_renewal_rejection_prevents_publication_after_provider_returns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)

            def slow_provider(prompt, provider="fake"):
                time.sleep(0.04)
                return {"answer": "late"}

            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", side_effect=slow_provider),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
                patch("worker.worker.FLIGHT_LEASE_MS", 20),
            ):
                with self.assertRaises(FlightOwnershipLost):
                    process_inference(
                        {"prompt": "slow", "provider": "fake"},
                        RenewalRejectingClient(server),
                    )

            self.assertEqual(server.lrange(b"semantic_cache:index", b"0", b"-1"), [])

    def test_publication_connection_failure_does_not_write_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", return_value={"answer": "response"}),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
            ):
                with self.assertRaisesRegex(ConnectionError, "publication"):
                    process_inference(
                        {"prompt": "commit failure", "provider": "fake"},
                        CommitFailingClient(server),
                    )
            self.assertEqual(server.lrange(b"semantic_cache:index", b"0", b"-1"), [])

    def test_follower_deadline_is_bounded(self):
        client = NeverResolvingClient()
        with (
            patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
            patch("worker.worker.get_semantic_cache_entries", return_value=[]),
            patch("worker.worker.FLIGHT_WAIT_TIMEOUT_SECONDS", 0.025),
            patch("worker.worker.FLIGHT_POLL_MIN_SECONDS", 0.005),
            patch("worker.worker.FLIGHT_POLL_MAX_SECONDS", 0.01),
        ):
            with self.assertRaises(FlightWaitTimeout):
                process_inference({"prompt": "wait", "provider": "fake"}, client)
        self.assertEqual(client.metrics["metrics:flight_timeouts"], 1)

    def test_committed_cache_survives_job_level_retry_without_provider_call(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            client = InProcessClient(server)
            provider_calls = 0

            def provider(prompt, provider="fake"):
                nonlocal provider_calls
                provider_calls += 1
                return {"answer": "durable"}

            with (
                patch("worker.worker.get_embedding", return_value=[1.0, 0.0]),
                patch("worker.worker.generate_response", side_effect=provider),
                patch("worker.worker.DEMO_INFERENCE_DELAY_SECONDS", 0),
            ):
                first = process_inference({"prompt": "retry", "provider": "fake"}, client)
                second = process_inference({"prompt": "retry", "provider": "fake"}, client)

            self.assertEqual(first["cache"], "miss")
            self.assertEqual(second["cache"], "hit")
            self.assertEqual(provider_calls, 1)


if __name__ == "__main__":
    unittest.main()
