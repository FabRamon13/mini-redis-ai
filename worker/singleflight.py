"""Pure helpers for deriving exact-request single-flight identities.

This module intentionally contains no worker orchestration.  The identity is
versioned and hashes the exact request inputs that can affect a provider
response, allowing coordination to be added without exposing prompts in keys.
"""

import hashlib
import json
import math
import threading
import time


DEFAULT_TENANT_ID = "__global__"
DEFAULT_KNOWLEDGE_NAMESPACE = "default"
FLIGHT_IDENTITY_SCHEMA_VERSION = 1


def _normalize_json(value, path="generation_config"):
    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return value

    if isinstance(value, list):
        return [
            _normalize_json(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]

    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings")
            normalized[key] = _normalize_json(item, f"{path}.{key}")
        return normalized

    raise TypeError(f"{path} contains unsupported value type {type(value).__name__}")


def canonical_flight_identity(
    *,
    prompt,
    provider,
    model_id,
    model_revision="",
    generation_config=None,
    tenant_id=DEFAULT_TENANT_ID,
    knowledge_namespace=DEFAULT_KNOWLEDGE_NAMESPACE,
):
    """Return canonical UTF-8 JSON bytes for an exact inference request."""
    required_strings = {
        "prompt": prompt,
        "provider": provider,
        "model_id": model_id,
        "model_revision": model_revision,
        "tenant_id": tenant_id,
        "knowledge_namespace": knowledge_namespace,
    }
    for name, value in required_strings.items():
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")

    if generation_config is None:
        generation_config = {}
    if not isinstance(generation_config, dict):
        raise TypeError("generation_config must be a mapping")

    identity = {
        "generation": _normalize_json(generation_config),
        "knowledge_namespace": knowledge_namespace,
        "model": {
            "id": model_id,
            "revision": model_revision,
        },
        "prompt": prompt,
        "provider": provider,
        "schema_version": FLIGHT_IDENTITY_SCHEMA_VERSION,
        "tenant_id": tenant_id,
    }
    return json.dumps(
        identity,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def derive_flight_key(**identity_fields):
    """Return a stable datastore key without embedding the request prompt."""
    canonical = canonical_flight_identity(**identity_fields)
    return "inference_flight:" + hashlib.sha256(canonical).hexdigest()


class FlightOwnershipLost(RuntimeError):
    """Raised when a worker can no longer prove current flight ownership."""


class FlightWaitTimeout(TimeoutError):
    """Raised when a follower cannot resolve or take over within its deadline."""


class FlightLeaseRenewer:
    """Renew a flight lease and make ownership loss visible to its caller."""

    def __init__(
        self,
        client,
        flight_key,
        owner_id,
        generation,
        lease_ms,
        hard_deadline_seconds,
        interval_seconds=None,
        clock=time.monotonic,
    ):
        self.client = client
        self.flight_key = flight_key
        self.owner_id = owner_id
        self.generation = generation
        self.lease_ms = lease_ms
        self.clock = clock
        self.deadline = clock() + hard_deadline_seconds
        self.interval_seconds = interval_seconds or max(0.01, lease_ms / 3000)
        self.stop_event = threading.Event()
        self.ownership_lost = threading.Event()
        self.error = None
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._renew_loop, daemon=True)
        self.thread.start()
        return self

    def _renew_loop(self):
        while not self.stop_event.wait(self.interval_seconds):
            if self.clock() >= self.deadline:
                self.error = FlightOwnershipLost("flight provider deadline exceeded")
                self.ownership_lost.set()
                return
            try:
                renewed = self.client.flight_renew(
                    self.flight_key,
                    self.owner_id,
                    self.generation,
                    self.lease_ms,
                )
            except Exception as exc:
                self.error = exc
                self.ownership_lost.set()
                return
            if renewed is None:
                self.error = FlightOwnershipLost("flight lease renewal was rejected")
                self.ownership_lost.set()
                return

    def ensure_owned(self):
        if self.ownership_lost.is_set():
            raise FlightOwnershipLost("flight ownership was lost") from self.error

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, self.interval_seconds * 2))
