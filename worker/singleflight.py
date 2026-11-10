"""Pure helpers for deriving exact-request single-flight identities.

This module intentionally contains no worker orchestration.  The identity is
versioned and hashes the exact request inputs that can affect a provider
response, allowing coordination to be added without exposing prompts in keys.
"""

import hashlib
import json
import math


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
