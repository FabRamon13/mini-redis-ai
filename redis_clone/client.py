import math
import socket
import threading
import time
from dataclasses import dataclass
from enum import Enum

from redis_clone.exceptions import (
    AmbiguousCommandResult,
    ClientClosedError,
    CommandError,
    CommandNotSent,
    Disconnect,
    ProtocolError,
)
from redis_clone.protocol import Error, ProtocolHandler


DEFAULT_CONNECT_TIMEOUT_SECONDS = 2.0
DEFAULT_WRITE_TIMEOUT_SECONDS = 5.0
DEFAULT_READ_TIMEOUT_SECONDS = 5.0
DEFAULT_COMMAND_TIMEOUT_SECONDS = 10.0

READ_ONLY_COMMANDS = frozenset({
    "EXISTS",
    "FLIGHTSTATUS",
    "GET",
    "LLEN",
    "LRANGE",
    "MGET",
    "PING",
    "TTL",
})


class ConnectionState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTED = "connected"
    BROKEN = "broken"
    CLOSED = "closed"


@dataclass(frozen=True)
class FlightState:
    state: str
    generation: int
    expires_at_ms: int = 0
    retry_at_ms: int = 0
    resolved_at_ms: int = 0
    error_code: str = ""
    result_key: str = ""


@dataclass(frozen=True)
class FlightAcquireResult(FlightState):
    role: str = "FOLLOWER"
    takeover: bool = False


def _positive_finite_timeout(name, value):
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive finite number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return parsed


def _decode_flight_map(response):
    if response is None:
        return None

    decoded = {}
    for key, value in response.items():
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else key
        decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
        decoded[decoded_key] = decoded_value
    return decoded


class _DeadlineSocketReader:
    """Small file-like RESP reader with inactivity and absolute deadlines."""

    def __init__(self, sock):
        self._socket = sock
        self._buffer = bytearray()
        self._deadline = None
        self._read_timeout = None

    def configure(self, deadline, read_timeout):
        self._deadline = deadline
        self._read_timeout = read_timeout

    def _recv(self):
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("command deadline exceeded")
        self._socket.settimeout(min(self._read_timeout, remaining))
        return self._socket.recv(4096)

    def read(self, size=-1):
        if size == 0:
            return b""
        if size < 0:
            raise ValueError("unbounded reads are not supported")
        while len(self._buffer) < size:
            chunk = self._recv()
            if not chunk:
                break
            self._buffer.extend(chunk)
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def readline(self, size=-1):
        while True:
            newline = self._buffer.find(b"\n")
            if newline >= 0:
                end = newline + 1
                if size >= 0:
                    end = min(end, size)
                result = bytes(self._buffer[:end])
                del self._buffer[:end]
                return result
            if size >= 0 and len(self._buffer) >= size:
                result = bytes(self._buffer[:size])
                del self._buffer[:size]
                return result
            chunk = self._recv()
            if not chunk:
                result = bytes(self._buffer)
                self._buffer.clear()
                return result
            self._buffer.extend(chunk)


class Client:
    def __init__(
        self,
        host="127.0.0.1",
        port=31337,
        *,
        connect_timeout_seconds=DEFAULT_CONNECT_TIMEOUT_SECONDS,
        write_timeout_seconds=DEFAULT_WRITE_TIMEOUT_SECONDS,
        read_timeout_seconds=DEFAULT_READ_TIMEOUT_SECONDS,
        command_timeout_seconds=DEFAULT_COMMAND_TIMEOUT_SECONDS,
    ):
        self.host = host
        self.port = port
        self.connect_timeout_seconds = _positive_finite_timeout(
            "connect_timeout_seconds", connect_timeout_seconds
        )
        self.write_timeout_seconds = _positive_finite_timeout(
            "write_timeout_seconds", write_timeout_seconds
        )
        self.read_timeout_seconds = _positive_finite_timeout(
            "read_timeout_seconds", read_timeout_seconds
        )
        self.command_timeout_seconds = _positive_finite_timeout(
            "command_timeout_seconds", command_timeout_seconds
        )
        self._protocol = ProtocolHandler()
        self._lock = threading.RLock()
        self._socket = None
        self._reader = None
        self._state = ConnectionState.DISCONNECTED
        self._connect()

    @property
    def connection_state(self):
        return self._state

    def _remaining(self, deadline, maximum):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("command deadline exceeded")
        return min(maximum, remaining)

    def _connect(self, deadline=None, command=None):
        if self._state == ConnectionState.CLOSED:
            raise ClientClosedError(
                "client is closed", command=command, phase="connect"
            )
        timeout = self.connect_timeout_seconds
        if deadline is not None:
            try:
                timeout = self._remaining(deadline, timeout)
            except socket.timeout as exc:
                raise CommandNotSent(
                    "command deadline exceeded before connection",
                    command=command,
                    phase="connect",
                    cause=exc,
                ) from exc
        try:
            sock = socket.create_connection((self.host, self.port), timeout=timeout)
        except OSError as exc:
            self._state = ConnectionState.DISCONNECTED
            raise CommandNotSent(
                "datastore connection failed before command transmission",
                command=command,
                phase="connect",
                cause=exc,
            ) from exc
        self._socket = sock
        self._reader = _DeadlineSocketReader(sock)
        self._state = ConnectionState.CONNECTED

    def _abort_connection(self):
        sock = self._socket
        self._socket = None
        self._reader = None
        if self._state != ConnectionState.CLOSED:
            self._state = ConnectionState.BROKEN
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _ensure_connected(self, deadline, command):
        if self._state == ConnectionState.CLOSED:
            raise ClientClosedError(
                "client is closed", command=command, phase="connect"
            )
        if self._state != ConnectionState.CONNECTED:
            self._connect(deadline=deadline, command=command)

    def _execute_once(self, args, command, deadline):
        self._ensure_connected(deadline, command)
        payload = self._protocol.encode(args)
        try:
            self._socket.settimeout(
                self._remaining(deadline, self.write_timeout_seconds)
            )
        except socket.timeout as exc:
            raise CommandNotSent(
                "command deadline exceeded before transmission",
                command=command,
                phase="write",
                cause=exc,
            ) from exc
        except OSError as exc:
            self._abort_connection()
            raise CommandNotSent(
                "connection failed before command transmission",
                command=command,
                phase="write",
                cause=exc,
            ) from exc

        try:
            self._socket.sendall(payload)
        except OSError as exc:
            self._abort_connection()
            raise AmbiguousCommandResult(
                "command write failed after transmission may have begun",
                command=command,
                phase="write",
                cause=exc,
            ) from exc

        try:
            self._reader.configure(deadline, self.read_timeout_seconds)
            response = self._protocol.handle_request(self._reader)
        except (Disconnect, ProtocolError, OSError) as exc:
            self._abort_connection()
            raise AmbiguousCommandResult(
                "command was transmitted but no complete valid response was received",
                command=command,
                phase="read",
                cause=exc,
            ) from exc

        if isinstance(response, Error):
            raise CommandError(response.message)
        return response

    def execute(self, *args):
        command = None
        if args:
            raw_command = args[0]
            if isinstance(raw_command, bytes):
                command = raw_command.decode("utf-8", errors="replace").upper()
            else:
                command = str(raw_command).upper()
        deadline = time.monotonic() + self.command_timeout_seconds
        attempts = 2 if command in READ_ONLY_COMMANDS else 1

        acquired = self._lock.acquire(timeout=self.command_timeout_seconds)
        if not acquired:
            raise CommandNotSent(
                "command deadline exceeded waiting for client lock",
                command=command,
                phase="lock",
            )
        try:
            last_error = None
            for attempt in range(attempts):
                try:
                    return self._execute_once(args, command, deadline)
                except (CommandNotSent, AmbiguousCommandResult) as exc:
                    last_error = exc
                    if attempt + 1 >= attempts or time.monotonic() >= deadline:
                        raise
            raise last_error
        finally:
            self._lock.release()

    def close(self):
        with self._lock:
            if self._state == ConnectionState.CLOSED:
                return
            self._state = ConnectionState.CLOSED
            sock = self._socket
            self._socket = None
            self._reader = None
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass

    def get(self, key):
        return self.execute("GET", key)

    def set(self, key, value, *options):
        return self.execute("SET", key, value, *options)

    def delete(self, key):
        return self.execute("DELETE", key)

    def flush(self):
        return self.execute("FLUSH")

    def mget(self, *keys):
        return self.execute("MGET", *keys)

    def mset(self, *items):
        return self.execute("MSET", *items)

    def ping(self):
        return self.execute("PING")

    def exists(self, key):
        return self.execute("EXISTS", key)

    def ttl(self, key):
        return self.execute("TTL", key)

    def lpush(self, key, *values):
        return self.execute("LPUSH", key, *values)

    def rpop(self, key):
        return self.execute("RPOP", key)

    def llen(self, key):
        return self.execute("LLEN", key)

    def incr(self, key):
        return self.execute("INCR", key)

    def lrange(self, key, start, stop):
        return self.execute("LRANGE", key, start, stop)

    def rpoplpush(self, source, destination):
        return self.execute("RPOPLPUSH", source, destination)

    def lrem(self, key, value):
        return self.execute("LREM", key, value)

    def claim(self, source, destination, worker_id, claim_token, claimed_at, lease_seconds):
        return self.execute(
            "CLAIM", source, destination, worker_id, claim_token,
            claimed_at, str(lease_seconds),
        )

    def requeue(self, source, destination, job_id, job_key, job_payload, claim_token):
        return self.execute(
            "REQUEUE", source, destination, job_id, job_key,
            job_payload, claim_token,
        )

    def ack(self, source, job_id, claim_token):
        return self.execute("ACK", source, job_id, claim_token)

    def finish(self, source, destination, job_id, job_key, job_payload, claim_token):
        return self.execute(
            "FINISH", source, destination, job_id, job_key,
            job_payload, claim_token,
        )

    def update_claim(self, job_id, job_key, job_payload, claim_token):
        return self.execute("UPDATECLAIM", job_id, job_key, job_payload, claim_token)

    def enqueue(self, queue_key, job_id, job_key, job_payload, max_size):
        return self.execute(
            "ENQUEUE", queue_key, job_id, job_key, job_payload, str(max_size)
        )

    def incrby(self, key, amount):
        return self.execute("INCRBY", key, str(amount))

    def flight_acquire(self, key, owner_id, lease_ms):
        result = _decode_flight_map(
            self.execute("FLIGHTACQUIRE", key, owner_id, str(lease_ms))
        )
        return FlightAcquireResult(
            role=result["role"],
            state=result["state"],
            generation=int(result["generation"]),
            expires_at_ms=int(result.get("expires_at_ms", 0)),
            retry_at_ms=int(result.get("retry_at_ms", 0)),
            resolved_at_ms=int(result.get("resolved_at_ms", 0)),
            error_code=result.get("error_code", ""),
            result_key=result.get("result_key", ""),
            takeover=bool(int(result.get("takeover", 0))),
        )

    def flight_renew(self, key, owner_id, generation, lease_ms):
        response = self.execute(
            "FLIGHTRENEW", key, owner_id, str(generation), str(lease_ms)
        )
        if response == 0:
            return None
        result = _decode_flight_map(response)
        return FlightState(
            state=result["state"],
            generation=int(result["generation"]),
            expires_at_ms=int(result.get("expires_at_ms", 0)),
        )

    def flight_status(self, key):
        result = _decode_flight_map(self.execute("FLIGHTSTATUS", key))
        if result is None:
            return None
        return FlightState(
            state=result["state"],
            generation=int(result["generation"]),
            expires_at_ms=int(result.get("expires_at_ms", 0)),
            retry_at_ms=int(result.get("retry_at_ms", 0)),
            resolved_at_ms=int(result.get("resolved_at_ms", 0)),
            error_code=result.get("error_code", ""),
            result_key=result.get("result_key", ""),
        )

    def flight_release(self, key, owner_id, generation):
        return self.execute("FLIGHTRELEASE", key, owner_id, str(generation))

    def flight_fail(self, key, owner_id, generation, cooldown_ms, error_code):
        return self.execute(
            "FLIGHTFAIL", key, owner_id, str(generation),
            str(cooldown_ms), error_code,
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
        return self.execute(
            "FLIGHTCOMMIT", flight_key, owner_id, str(generation),
            cache_key, cache_payload, index_key, str(max_entries),
        )
