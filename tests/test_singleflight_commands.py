import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from redis_clone.client import Client
from redis_clone.exceptions import CommandError
from redis_clone.server import Server


class FakeClock:
    def __init__(self, seconds=1_700_000_000.0):
        self.seconds = seconds

    def __call__(self):
        return self.seconds

    def advance_ms(self, milliseconds):
        self.seconds += milliseconds / 1000


class SingleFlightCommandTests(unittest.TestCase):
    def make_server(self, tmpdir, clock=None):
        return Server(
            port=0,
            aof_file=f"{tmpdir}/appendonly.aof",
            clock=clock,
        )

    def test_one_concurrent_leader_and_remaining_followers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)

            def acquire(index):
                return server.flight_acquire(
                    b"inference_flight:key",
                    f"owner-{index}".encode(),
                    b"5000",
                )

            with ThreadPoolExecutor(max_workers=16) as executor:
                results = list(executor.map(acquire, range(32)))

            leaders = [result for result in results if result["role"] == "LEADER"]
            followers = [result for result in results if result["role"] == "FOLLOWER"]
            self.assertEqual(len(leaders), 1)
            self.assertEqual(len(followers), 31)
            self.assertEqual({result["generation"] for result in results}, {1})

    def test_lease_uses_datastore_time_and_expires_at_boundary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clock = FakeClock()
            server = self.make_server(tmpdir, clock)
            result = server.flight_acquire(b"flight", b"owner-a", b"1000")

            self.assertEqual(result["expires_at_ms"], 1_700_000_001_000)
            clock.advance_ms(999)
            self.assertEqual(server.flight_status(b"flight")["state"], "ACTIVE")
            clock.advance_ms(1)
            self.assertEqual(server.flight_status(b"flight")["state"], "EXPIRED")

            takeover = server.flight_acquire(b"flight", b"owner-b", b"1000")
            self.assertEqual(takeover["role"], "LEADER")
            self.assertEqual(takeover["generation"], 2)

    def test_renewal_uses_current_datastore_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clock = FakeClock()
            server = self.make_server(tmpdir, clock)
            acquired = server.flight_acquire(b"flight", b"owner-a", b"1000")
            clock.advance_ms(400)

            renewed = server.flight_renew(
                b"flight", b"owner-a", str(acquired["generation"]).encode(), b"2000"
            )

            self.assertEqual(renewed["expires_at_ms"], 1_700_000_002_400)

    def test_stale_owner_cannot_renew_release_or_fail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clock = FakeClock()
            server = self.make_server(tmpdir, clock)
            first = server.flight_acquire(b"flight", b"owner-a", b"100")
            clock.advance_ms(100)
            second = server.flight_acquire(b"flight", b"owner-b", b"1000")

            self.assertEqual(server.flight_renew(b"flight", b"owner-a", b"1", b"1000"), 0)
            self.assertEqual(server.flight_release(b"flight", b"owner-a", b"1"), 0)
            self.assertEqual(
                server.flight_fail(b"flight", b"owner-a", b"1", b"100", b"timeout"),
                0,
            )
            status = server.flight_status(b"flight")
            self.assertEqual(status["state"], "ACTIVE")
            self.assertEqual(status["generation"], second["generation"])
            self.assertGreater(second["generation"], first["generation"])

    def test_release_is_terminal_and_immediately_reacquirable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            acquired = server.flight_acquire(b"flight", b"owner-a", b"1000")

            self.assertEqual(server.flight_release(b"flight", b"owner-a", b"1"), 1)
            self.assertEqual(server.flight_status(b"flight")["state"], "RELEASED")
            replacement = server.flight_acquire(b"flight", b"owner-b", b"1000")
            self.assertEqual(replacement["role"], "LEADER")
            self.assertGreater(replacement["generation"], acquired["generation"])

    def test_failure_cooldown_blocks_then_allows_one_retry_leader(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clock = FakeClock()
            server = self.make_server(tmpdir, clock)
            server.flight_acquire(b"flight", b"owner-a", b"1000")
            self.assertEqual(
                server.flight_fail(b"flight", b"owner-a", b"1", b"500", b"provider_timeout"),
                1,
            )

            status = server.flight_status(b"flight")
            self.assertEqual(status["state"], "FAILED")
            self.assertEqual(status["error_code"], "provider_timeout")
            blocked = server.flight_acquire(b"flight", b"owner-b", b"1000")
            self.assertEqual(blocked["role"], "FOLLOWER")
            self.assertEqual(blocked["state"], "FAILED")

            clock.advance_ms(500)
            self.assertEqual(server.flight_status(b"flight")["state"], "RETRYABLE")
            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(
                    executor.map(
                        lambda index: server.flight_acquire(
                            b"flight", f"retry-{index}".encode(), b"1000"
                        ),
                        range(16),
                    )
                )
            self.assertEqual(sum(result["role"] == "LEADER" for result in results), 1)
            self.assertEqual({result["generation"] for result in results}, {2})

    def test_durations_are_positive_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            for value in (b"0", b"-1", b"invalid", b"300001"):
                with self.subTest(value=value):
                    with self.assertRaises(CommandError):
                        server.flight_acquire(b"flight", b"owner", value)

            server.flight_acquire(b"flight", b"owner", b"1000")
            for value in (b"0", b"60001"):
                with self.subTest(cooldown=value):
                    with self.assertRaises(CommandError):
                        server.flight_fail(b"flight", b"owner", b"1", value, b"error")

    def test_append_failure_does_not_grant_or_renew_ownership(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clock = FakeClock()
            server = self.make_server(tmpdir, clock)
            with patch.object(server, "_append_to_aof", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    server.flight_acquire(b"flight", b"owner-a", b"1000")

            self.assertIsNone(server.flight_status(b"flight"))
            self.assertEqual(server._flight_generation, 0)

            acquired = server.flight_acquire(b"flight", b"owner-a", b"1000")
            old_expiry = acquired["expires_at_ms"]
            clock.advance_ms(100)
            with patch.object(server, "_append_to_aof", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    server.flight_renew(b"flight", b"owner-a", b"1", b"2000")
            self.assertEqual(server.flight_status(b"flight")["expires_at_ms"], old_expiry)

    def test_detected_partial_write_is_rolled_back_before_later_appends(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            original_write = server._protocol.write_response

            def partial_write(handle, _parts):
                handle.write(b"*3\r\n$11\r\nFLIGHTSTATE\r\n")
                handle.flush()
                raise OSError("simulated write failure")

            with patch.object(server._protocol, "write_response", side_effect=partial_write):
                with self.assertRaises(OSError):
                    server.flight_acquire(b"failed", b"owner-a", b"1000")

            self.assertEqual(os.path.getsize(f"{tmpdir}/appendonly.aof"), 0)
            server._protocol.write_response = original_write
            server.flight_acquire(b"valid", b"owner-b", b"1000")
            reloaded = self.make_server(tmpdir)
            self.assertEqual(reloaded.flight_status(b"valid")["generation"], 1)

    def test_flight_records_request_fsync(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            with patch("redis_clone.server.os.fsync") as fsync:
                server.flight_acquire(b"flight", b"owner-a", b"1000")
            fsync.assert_called_once()

    def test_partial_append_failure_leaves_no_live_or_replayed_flight(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            aof_file = f"{tmpdir}/appendonly.aof"

            def partial_append(_parts, fsync=False):
                with open(aof_file, "ab") as handle:
                    handle.write(b"*3\r\n$11\r\nFLIGHTSTATE\r\n$6\r\nflight\r\n$50\r\n")
                    handle.flush()
                raise OSError("simulated interrupted append")

            with patch.object(server, "_append_to_aof", side_effect=partial_append):
                with self.assertRaises(OSError):
                    server.flight_acquire(b"flight", b"owner-a", b"1000")

            self.assertIsNone(server.flight_status(b"flight"))
            reloaded = self.make_server(tmpdir)
            self.assertIsNone(reloaded.flight_status(b"flight"))
            acquired = reloaded.flight_acquire(b"recovered", b"owner-b", b"1000")
            self.assertEqual(acquired["generation"], 1)
            replayed_again = self.make_server(tmpdir)
            self.assertEqual(
                replayed_again.flight_status(b"recovered")["generation"],
                1,
            )

    def test_replay_preserves_state_and_generation_across_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clock = FakeClock()
            server = self.make_server(tmpdir, clock)
            first = server.flight_acquire(b"flight-a", b"owner-a", b"100")
            clock.advance_ms(100)
            second = server.flight_acquire(b"flight-a", b"owner-b", b"1000")
            server.flight_acquire(b"flight-b", b"owner-c", b"1000")
            server.flight_fail(b"flight-b", b"owner-c", b"3", b"500", b"provider_error")

            reloaded = self.make_server(tmpdir, clock)
            self.assertEqual(reloaded.flight_status(b"flight-a")["generation"], 2)
            self.assertEqual(reloaded.flight_status(b"flight-b")["state"], "FAILED")
            reloaded.flight_release(b"flight-a", b"owner-b", b"2")
            next_flight = reloaded.flight_acquire(b"flight-c", b"owner-d", b"1000")
            self.assertEqual(next_flight["generation"], 4)
            self.assertGreater(second["generation"], first["generation"])

    def test_truncated_final_flight_record_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            server.flight_acquire(b"good", b"owner-a", b"1000")
            aof_file = f"{tmpdir}/appendonly.aof"
            with open(aof_file, "ab") as handle:
                handle.write(b"*3\r\n$11\r\nFLIGHTSTATE\r\n$3\r\nbad\r\n$10\r\n{\"state\"")

            reloaded = self.make_server(tmpdir)
            self.assertIsNotNone(reloaded.flight_status(b"good"))
            self.assertIsNone(reloaded.flight_status(b"bad"))

    def test_internal_replay_command_is_rejected_from_live_dispatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            with self.assertRaises(CommandError):
                server.get_response([
                    b"FLIGHTSTATE",
                    b"flight",
                    json.dumps({"state": "ACTIVE", "generation": 99}).encode(),
                ])

    def test_flush_clears_flights_but_does_not_reuse_generation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self.make_server(tmpdir)
            server.flight_acquire(b"flight", b"owner-a", b"1000")
            server.flush()
            self.assertIsNone(server.flight_status(b"flight"))
            self.assertEqual(
                server.flight_acquire(b"other", b"owner-b", b"1000")["generation"],
                2,
            )


class SingleFlightClientWrapperTests(unittest.TestCase):
    def make_client_without_socket(self):
        return object.__new__(Client)

    def test_acquire_and_status_are_typed(self):
        client = self.make_client_without_socket()
        response = {
            b"role": b"LEADER",
            b"state": b"ACTIVE",
            b"generation": 7,
            b"expires_at_ms": 12345,
            b"retry_at_ms": 0,
            b"resolved_at_ms": 0,
            b"error_code": b"",
        }
        with patch.object(client, "execute", return_value=response) as execute:
            acquired = client.flight_acquire("flight", "owner", 1000)
        self.assertEqual(acquired.role, "LEADER")
        self.assertEqual(acquired.generation, 7)
        execute.assert_called_once_with("FLIGHTACQUIRE", "flight", "owner", "1000")

        with patch.object(client, "execute", return_value=None):
            self.assertIsNone(client.flight_status("missing"))

    def test_mutating_wrappers_preserve_tokens_and_durations(self):
        client = self.make_client_without_socket()
        with patch.object(client, "execute", return_value=1) as execute:
            self.assertEqual(client.flight_release("flight", "owner", 4), 1)
            execute.assert_called_once_with("FLIGHTRELEASE", "flight", "owner", "4")

        with patch.object(client, "execute", return_value=1) as execute:
            self.assertEqual(client.flight_fail("flight", "owner", 4, 500, "timeout"), 1)
            execute.assert_called_once_with(
                "FLIGHTFAIL", "flight", "owner", "4", "500", "timeout"
            )


if __name__ == "__main__":
    unittest.main()
