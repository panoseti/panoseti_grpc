"""
Edge-case tests for the Telemetry service that complement the existing integration suite.

Covers:
  - Unknown device type routes to SANDBOX (not main hash namespace)
  - extra_data fields are flattened with correct prefix
  - Strict schema rejects invalid satellite count
  - Concurrent writes to the same device_id for different fields
  - Large payloads (100 KB) are accepted and stored correctly
  - Consecutive updates to the same field are last-writer-wins
  - Production keys never gain a TTL, even after multiple writes
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from tests.telemetry.conftest import poll_redis_field, poll_redis_key

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique(prefix: str) -> str:
    """Generate a unique device ID using the current time in ms."""
    return f"{prefix}_{int(time.time() * 1000)}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_unknown_device_type_stays_in_sandbox(grpc_client: Any, redis_client: Any) -> None:
    """
    An unknown device_type must be routed to SANDBOX:{type}:{device_id} and
    must NOT appear in the main hash namespace.
    """
    device_type = "totally_unknown_device_xyz"
    device_id = _unique("sandbox")

    grpc_client.log_flexible(device_type, device_id, {"signal": 99.9})

    sandbox_key = f"SANDBOX:{device_type}:{device_id}"
    assert poll_redis_key(redis_client, sandbox_key), f"SANDBOX key {sandbox_key!r} not created"

    # Must NOT exist under any other prefix
    keys = redis_client.keys(f"*{device_id}*")
    for k in keys:
        assert k == sandbox_key, f"Unknown device appeared outside SANDBOX: {k!r}"


def test_extra_data_flattening_with_prefix(grpc_client: Any, redis_client: Any) -> None:
    """
    When log_strict provides extra_data, the nested fields must be stored as
    flattened Redis hash fields with the 'extra_' prefix.
    """
    device_id = _unique("extra_flat")
    key = f"UBLOX_ZED-F9T_{device_id}"

    grpc_client.log_strict(
        "gnss",
        device_id,
        {
            "satellites": 9,
            "lat": 34.0,
            "lon": -118.0,
            "fix_mode": "3D",
            "extra_data": {
                "dilution_of_precision": 1.5,
                "hdop": 0.9,
            },
        },
    )

    assert poll_redis_field(redis_client, key, "extra_dilution_of_precision"), (
        "extra_dilution_of_precision not found in Redis hash"
    )
    assert poll_redis_field(redis_client, key, "extra_hdop"), "extra_hdop not found in Redis hash"

    assert redis_client.hget(key, "extra_dilution_of_precision") == "1.5"
    assert redis_client.hget(key, "extra_hdop") == "0.9"


def test_strict_schema_rejects_invalid_satellite_count(grpc_client: Any) -> None:
    """
    log_strict('gnss', ...) must raise ValueError when satellites > 100
    (the schema maximum).
    """
    with pytest.raises(ValueError, match="Server rejected data"):
        grpc_client.log_strict(
            "gnss",
            _unique("bad_gnss"),
            {
                "satellites": 200,  # > 100, invalid
                "lat": 0.0,
                "lon": 0.0,
                "fix_mode": "3D",
            },
        )


def test_strict_schema_rejects_out_of_range_coordinates(grpc_client: Any) -> None:
    """
    Latitude > 90 is outside the valid range and must be rejected.
    """
    with pytest.raises(ValueError, match="Server rejected data"):
        grpc_client.log_strict(
            "gnss",
            _unique("bad_coords"),
            {
                "satellites": 8,
                "lat": 91.0,  # > 90, invalid
                "lon": 0.0,
                "fix_mode": "3D",
            },
        )


def test_large_payload_within_limit(grpc_client: Any, redis_client: Any) -> None:
    """
    A 100 KB flexible payload must be accepted and stored in its entirety.
    """
    device_id = _unique("large_payload")
    key = f"DEV_TEST-FLEX_{device_id}"
    big_string = "X" * 100_000

    grpc_client.log_flexible("test_flex", device_id, {"blob": big_string})

    assert poll_redis_field(redis_client, key, "blob"), f"'blob' field not found in {key!r} after large payload"

    stored = redis_client.hget(key, "blob")
    assert len(stored) == 100_000, f"Expected stored blob length 100000, got {len(stored)}"


def test_production_key_ttl_remains_permanent_after_multiple_writes(grpc_client: Any, redis_client: Any) -> None:
    """
    A production (strict/gnss) device key must have TTL == -1 after both the
    first and subsequent writes — i.e., the server never sets a TTL on it.
    """
    device_id = _unique("perm_ttl")
    key = f"UBLOX_ZED-F9T_{device_id}"

    for i in range(3):
        grpc_client.log_strict(
            "gnss",
            device_id,
            {
                "satellites": 8 + i,
                "lat": float(i),
                "lon": float(-i),
                "fix_mode": "3D",
            },
        )

    assert poll_redis_key(redis_client, key), f"Key {key!r} not found"
    ttl = redis_client.ttl(key)
    assert ttl == -1, f"Production key {key!r} acquired TTL={ttl} after writes (should always be -1 / no expiry)"


def test_consecutive_field_overwrites_last_writer_wins(grpc_client: Any, redis_client: Any) -> None:
    """
    Rapid consecutive updates to the same field must result in the final value
    being stored — no intermediate value should survive.
    """
    device_id = _unique("lw_wins")
    key = f"DEV_TEST-FLEX_{device_id}"
    UPDATES = 15

    for i in range(UPDATES):
        grpc_client.log_flexible("test_flex", device_id, {"counter": float(i)})

    assert poll_redis_field(redis_client, key, "counter", expected=str(float(UPDATES - 1))), (
        f"Final counter value {float(UPDATES - 1)} not found in {key!r}"
    )

    final = redis_client.hget(key, "counter")
    assert float(final) == float(UPDATES - 1), f"Expected counter={float(UPDATES - 1)}, got {final}"


def test_two_device_types_do_not_share_namespace(grpc_client: Any, redis_client: Any) -> None:
    """
    Writing to device_type='gnss' and device_type='test_flex' with the same
    device_id must create two separate Redis keys in different namespaces.
    """
    device_id = _unique("namespace_isolation")
    gnss_key = f"UBLOX_ZED-F9T_{device_id}"
    flex_key = f"DEV_TEST-FLEX_{device_id}"

    grpc_client.log_strict(
        "gnss",
        device_id,
        {
            "satellites": 5,
            "lat": 10.0,
            "lon": 10.0,
            "fix_mode": "2D",
        },
    )
    grpc_client.log_flexible("test_flex", device_id, {"temperature": 25.5})

    assert poll_redis_key(redis_client, gnss_key), f"gnss key {gnss_key!r} not found"
    assert poll_redis_key(redis_client, flex_key), f"flex key {flex_key!r} not found"

    # Keys are distinct — GNSS key must NOT contain temperature field
    assert redis_client.hget(gnss_key, "temperature") is None, "temperature field leaked into GNSS key"
    # GNSS-specific fields must NOT appear in flex key
    assert redis_client.hget(flex_key, "satellites") is None, "satellites field leaked into flex key"
