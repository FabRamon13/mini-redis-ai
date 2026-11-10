import socket
import threading
import time
import unittest
from unittest.mock import patch

from redis_clone.client import Client, ConnectionState
from redis_clone.exceptions import (
    AmbiguousCommandResult,
    ClientClosedError,
    CommandError,
    CommandNotSent,
)
from redis_clone.protocol import ProtocolHandler


class ScriptedServer:
    def __init__(self, handlers):
        self.handlers = list(handlers)
        self.errors = []
        self.accepted = 0
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen()
        self.host, self.port = self._socket.getsockname()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        try:
            for handler in self.handlers:
                connection, _address = self._socket.accept()
                self.accepted += 1
                with connection:
                    handler(connection)
        except OSError:
            pass
        except Exception as exc:
            self.errors.append(exc)

    def close(self):
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._socket.close()
        self._thread.join(timeout=1)
        if self.errors:
            raise self.errors[0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


def read_command(connection):
    return ProtocolHandler().handle_request(connection.makefile("rb"))


def reply(payload):
    def handler(connection):
        read_command(connection)
        connection.sendall(payload)

    return handler


def drop_after_request(connection):
    read_command(connection)


class FakeWriteTimeoutSocket:
    def __init__(self):
        self.shutdown_called = False
        self.close_called = False
        self.timeout = None

    def settimeout(self, value):
        self.timeout = value

    def sendall(self, _payload):
        raise socket.timeout("write stalled")

    def shutdown(self, _how):
        self.shutdown_called = True

    def close(self):
        self.close_called = True


class SafeClientTests(unittest.TestCase):
    def make_client(self, server, **overrides):
        settings = {
            "connect_timeout_seconds": 0.2,
            "write_timeout_seconds": 0.1,
            "read_timeout_seconds": 0.1,
            "command_timeout_seconds": 0.3,
        }
        settings.update(overrides)
        return Client(server.host, server.port, **settings)

    def test_rejects_non_positive_or_non_finite_timeouts(self):
        names = (
            "connect_timeout_seconds",
            "write_timeout_seconds",
            "read_timeout_seconds",
            "command_timeout_seconds",
        )
        for name in names:
            for value in (0, -1, float("inf"), float("nan"), "invalid"):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(ValueError):
                        Client(**{name: value})

    def test_connection_timeout_is_known_not_sent(self):
        with patch(
            "redis_clone.client.socket.create_connection",
            side_effect=socket.timeout("connect stalled"),
        ):
            with self.assertRaises(CommandNotSent) as raised:
                Client(connect_timeout_seconds=0.01)

        self.assertFalse(raised.exception.may_have_committed)
        self.assertEqual(raised.exception.phase, "connect")

    def test_write_timeout_is_ambiguous_and_aborts_socket(self):
        sock = FakeWriteTimeoutSocket()
        with patch("redis_clone.client.socket.create_connection", return_value=sock):
            client = Client()
            with self.assertRaises(AmbiguousCommandResult) as raised:
                client.set("key", "value")

        self.assertTrue(raised.exception.may_have_committed)
        self.assertEqual(raised.exception.command, "SET")
        self.assertEqual(raised.exception.phase, "write")
        self.assertEqual(client.connection_state, ConnectionState.BROKEN)
        self.assertTrue(sock.shutdown_called)
        self.assertTrue(sock.close_called)

    def test_read_stall_is_bounded_and_ambiguous(self):
        def stall(connection):
            read_command(connection)
            time.sleep(0.2)

        with ScriptedServer([stall]) as server:
            client = self.make_client(
                server,
                read_timeout_seconds=0.04,
                command_timeout_seconds=0.12,
            )
            started = time.monotonic()
            with self.assertRaises(AmbiguousCommandResult) as raised:
                client.set("key", "value")
            elapsed = time.monotonic() - started
            client.close()

        self.assertEqual(raised.exception.phase, "read")
        self.assertLess(elapsed, 0.18)

    def test_slow_trickle_cannot_extend_overall_deadline(self):
        def trickle(connection):
            read_command(connection)
            try:
                for byte in b"$2\r\nok\r\n":
                    connection.sendall(bytes([byte]))
                    time.sleep(0.025)
            except OSError:
                pass

        with ScriptedServer([trickle]) as server:
            client = self.make_client(
                server,
                read_timeout_seconds=0.08,
                command_timeout_seconds=0.1,
            )
            started = time.monotonic()
            with self.assertRaises(AmbiguousCommandResult):
                client.execute("MUTATE")
            elapsed = time.monotonic() - started
            client.close()

        self.assertLess(elapsed, 0.16)

    def test_partial_and_malformed_responses_break_connection(self):
        for payload in (b"$5\r\nab", b"?invalid\r\n"):
            with self.subTest(payload=payload):
                with ScriptedServer([reply(payload)]) as server:
                    client = self.make_client(server)
                    with self.assertRaises(AmbiguousCommandResult):
                        client.execute("MUTATE")
                    self.assertEqual(
                        client.connection_state,
                        ConnectionState.BROKEN,
                    )
                    client.close()

    def test_commit_then_drop_is_ambiguous_and_mutation_is_not_retried(self):
        commits = []

        def commit_then_drop(connection):
            commits.append(read_command(connection))

        with ScriptedServer([
            commit_then_drop,
            reply(b"+PONG\r\n"),
        ]) as server:
            client = self.make_client(server)
            with self.assertRaises(AmbiguousCommandResult) as raised:
                client.set("key", "value")
            self.assertEqual(len(commits), 1)
            self.assertEqual(server.accepted, 1)
            self.assertEqual(client.ping(), b"PONG")
            self.assertEqual(server.accepted, 2)
            client.close()

        self.assertTrue(raised.exception.may_have_committed)

    def test_valid_server_error_keeps_connection_usable(self):
        commands = []

        def two_responses(connection):
            reader = connection.makefile("rb")
            protocol = ProtocolHandler()
            commands.append(protocol.handle_request(reader))
            connection.sendall(b"-queue is full\r\n")
            commands.append(protocol.handle_request(reader))
            connection.sendall(b"+PONG\r\n")

        with ScriptedServer([two_responses]) as server:
            client = self.make_client(server)
            with self.assertRaises(CommandError):
                client.enqueue("jobs", "id", "job:id", "{}", 1)
            self.assertEqual(client.connection_state, ConnectionState.CONNECTED)
            self.assertEqual(client.ping(), b"PONG")
            self.assertEqual(server.accepted, 1)
            client.close()

        self.assertEqual(commands[0][0], b"ENQUEUE")
        self.assertEqual(commands[1][0], b"PING")

    def test_read_only_command_retries_once_on_new_connection(self):
        with ScriptedServer([
            drop_after_request,
            reply(b"$5\r\nvalue\r\n"),
        ]) as server:
            client = self.make_client(server)
            self.assertEqual(client.get("key"), b"value")
            self.assertEqual(server.accepted, 2)
            client.close()

    def test_read_only_retry_is_bounded_to_one(self):
        with ScriptedServer([
            drop_after_request,
            drop_after_request,
        ]) as server:
            client = self.make_client(server)
            with self.assertRaises(AmbiguousCommandResult):
                client.get("key")
            self.assertEqual(server.accepted, 2)
            client.close()

    def test_existing_wrappers_preserve_commands_and_decoding(self):
        commands = []

        def responses(connection):
            reader = connection.makefile("rb")
            protocol = ProtocolHandler()
            commands.append(protocol.handle_request(reader))
            connection.sendall(b"$5\r\nvalue\r\n")
            commands.append(protocol.handle_request(reader))
            connection.sendall(b":1\r\n")

        with ScriptedServer([responses]) as server:
            client = self.make_client(server)
            self.assertEqual(client.get("key"), b"value")
            self.assertEqual(client.set("key", "new"), 1)
            client.close()

        self.assertEqual(commands, [
            [b"GET", b"key"],
            [b"SET", b"key", b"new"],
        ])

    def test_close_is_idempotent_and_prevents_reuse(self):
        with ScriptedServer([]) as server:
            client = self.make_client(server)
            client.close()
            client.close()
            with self.assertRaises(ClientClosedError):
                client.ping()
            self.assertEqual(client.connection_state, ConnectionState.CLOSED)


if __name__ == "__main__":
    unittest.main()
