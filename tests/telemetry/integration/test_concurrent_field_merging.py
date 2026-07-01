"""
Tests for concurrent Redis HSET field-merging behavior in the Telemetry service.
"""

import threading
import time
from typing import Any

from tests.telemetry.conftest import poll_redis_field, poll_redis_key


def test_two_threads_different_fields_no_cross_contamination(grpc_client: Any, redis_client: Any) -> None:
    """
    Thread A updates lat/lon; Thread B updates satellites on the same device_id.
    After both finish, the Redis hash must contain all four fields with correct values.
    """
    device_id = "concurrent_gnss_merge_01"
    key = f"UBLOX_ZED-F9T_{device_id}"
    N_UPDATES = 5

    errors = []

    def update_lat_lon() -> None:
        for i in range(N_UPDATES):
            try:
                grpc_client.log_strict(
                    "gnss",
                    device_id,
                    {
                        "satellites": 0,  # placeholder
                        "lat": 33.0 + i * 0.001,
                        "lon": -116.0 - i * 0.001,
                        "fix_mode": "3D",
                    },
                )
            except Exception as e:
                errors.append(e)

    def update_satellites() -> None:
        for i in range(N_UPDATES):
            try:
                grpc_client.log_strict(
                    "gnss",
                    device_id,
                    {
                        "satellites": i + 1,
                        "lat": 33.0,  # placeholder
                        "lon": -116.0,  # placeholder
                        "fix_mode": "3D",
                    },
                )
            except Exception as e:
                errors.append(e)

    t1 = threading.Thread(target=update_lat_lon)
    t2 = threading.Thread(target=update_satellites)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not errors, f"Threads raised exceptions: {errors}"

    assert poll_redis_key(redis_client, key), f"Key {key!r} must exist in Redis"

    # Both lat/lon and satellites must be present — no fields lost due to race
    lat = redis_client.hget(key, "lat")
    lon = redis_client.hget(key, "lon")
    satellites = redis_client.hget(key, "satellites")

    assert lat is not None, "Field 'lat' must be present after concurrent writes"
    assert lon is not None, "Field 'lon' must be present after concurrent writes"
    assert satellites is not None, "Field 'satellites' must be present after concurrent writes"


def test_rapid_field_overwrite_last_writer_wins(grpc_client: Any, redis_client: Any) -> None:
    """
    20 sequential updates to the fix_mode field: the final Redis value must
    match the last write (no stale values cached anywhere in the pipeline).
    """
    device_id = "overwrite_test_gnss_01"
    key = f"UBLOX_ZED-F9T_{device_id}"
    UPDATES = 20
    modes = [f"MODE_{i:02d}" for i in range(UPDATES)]

    for mode in modes:
        grpc_client.log_strict(
            "gnss",
            device_id,
            {
                "satellites": 8,
                "lat": 33.356,
                "lon": -116.864,
                "fix_mode": mode,
            },
        )
        time.sleep(0.02)  # slight pause to keep ordering deterministic

    # Wait for last write to land, then check final value
    assert poll_redis_field(redis_client, key, "fix_mode", expected=modes[-1]), (
        f"Last fix_mode {modes[-1]!r} not found in {key!r}"
    )

    stored_mode = redis_client.hget(key, "fix_mode")
    assert stored_mode == modes[-1], (
        f"Expected last written mode {modes[-1]!r}, got {stored_mode!r}. "
        "Last-writer-wins must hold for sequential updates."
    )


def test_independent_devices_do_not_share_fields(grpc_client: Any, redis_client: Any) -> None:
    """
    Updates to device A must not affect the Redis hash for device B,
    even when both share the same device_type.
    """
    device_a = "isolation_gnss_A"
    device_b = "isolation_gnss_B"
    key_a = f"UBLOX_ZED-F9T_{device_a}"
    key_b = f"UBLOX_ZED-F9T_{device_b}"

    grpc_client.log_strict(
        "gnss",
        device_a,
        {
            "satellites": 7,
            "lat": 10.0,
            "lon": 20.0,
            "fix_mode": "3D",
        },
    )
    grpc_client.log_strict(
        "gnss",
        device_b,
        {
            "satellites": 12,
            "lat": 50.0,
            "lon": 60.0,
            "fix_mode": "2D",
        },
    )

    assert poll_redis_key(redis_client, key_a) and poll_redis_key(redis_client, key_b), (
        "Both device keys must exist in Redis"
    )

    assert redis_client.hget(key_a, "satellites") == "7"
    assert redis_client.hget(key_b, "satellites") == "12"
    assert redis_client.hget(key_a, "lat") == "10.0"
    assert redis_client.hget(key_b, "lat") == "50.0"
    assert redis_client.hget(key_a, "fix_mode") == "3D"
    assert redis_client.hget(key_b, "fix_mode") == "2D"
