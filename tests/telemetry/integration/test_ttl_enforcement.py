"""
Tests that Redis TTL (time-to-live) is applied correctly for experimental,
production, and unknown (sandboxed) device types.
"""

from tests.telemetry.conftest import poll_redis_key


def test_experimental_key_has_positive_ttl(grpc_client, redis_client):
    """
    After log_flexible for a DEV_ device, the Redis key must have TTL > 0,
    meaning it will expire automatically.
    """
    device_id = "ttl_test_exp_01"
    grpc_client.log_flexible("test_flex", device_id, {"sensor_value": 42.0})

    key = f"DEV_TEST-FLEX_{device_id}"
    assert poll_redis_key(redis_client, key), f"Key {key!r} must exist in Redis"
    ttl = redis_client.ttl(key)
    assert ttl > 0, f"Experimental key {key!r} must have TTL > 0 (got {ttl}). Experimental data should auto-expire."
    assert ttl <= 3600, f"TTL {ttl} exceeds configured 3600 s cap"


def test_production_key_has_no_ttl(grpc_client, redis_client):
    """
    After log_strict for a production device, the Redis key must have TTL == -1
    (no expiry) — production telemetry is retained permanently.
    """
    device_id = "ttl_test_prod_01"
    grpc_client.log_strict(
        "gnss",
        device_id,
        {
            "satellites": 10,
            "lat": 33.356,
            "lon": -116.864,
            "fix_mode": "3D",
        },
    )

    key = f"UBLOX_ZED-F9T_{device_id}"
    assert poll_redis_key(redis_client, key), f"Key {key!r} must exist in Redis"
    ttl = redis_client.ttl(key)
    assert ttl == -1, f"Production key {key!r} must have TTL == -1 (persists forever), got {ttl}"


def test_sandbox_key_has_positive_ttl(grpc_client, redis_client):
    """
    An unknown device_type routes to the SANDBOX namespace and must
    have a positive TTL (it should not pollute Redis indefinitely).
    """
    device_id = "ttl_sandbox_01"
    unknown_type = "mystery_sensor_xyz"
    grpc_client.log_flexible(unknown_type, device_id, {"raw": "data"})

    # The server creates a SANDBOX key; find it by scanning
    sandbox_key = f"SANDBOX:{unknown_type}:{device_id}"
    assert poll_redis_key(redis_client, sandbox_key), f"SANDBOX key {sandbox_key!r} not found in Redis"
    matching_keys = redis_client.keys(f"*{unknown_type}*")

    assert len(matching_keys) > 0, (
        f"Expected a SANDBOX key matching *{unknown_type}* but none found. All keys: {redis_client.keys('*')}"
    )
    for k in matching_keys:
        ttl = redis_client.ttl(k)
        assert ttl > 0, f"SANDBOX key {k!r} must have TTL > 0 (got {ttl}). Unknown-device data should auto-expire."
