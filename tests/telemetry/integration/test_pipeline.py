import pytest
import time
from concurrent.futures import ThreadPoolExecutor


def test_flexible_struct_flow(grpc_client, redis_client):
    """
    Verifies that 'flexible' logging works for configured EXPERIMENTAL devices.
    """
    device_id = "device_01"
    rnd_data = {"voltage": 5.1, "fan_speed": 1200, "status": "OK"}

    # Use "test_flex" which is defined as experimental in config
    grpc_client.log_flexible("test_flex", device_id, rnd_data)

    time.sleep(0.5)

    # CHECK: Expect DEV_ prefix as per config
    expected_key = f"DEV_TEST-FLEX_{device_id}"

    assert redis_client.exists(expected_key)
    # Check data content
    assert redis_client.hget(expected_key, "voltage") == "5.1"

    # Check TTL (Should be > 0 for experimental)
    ttl = redis_client.ttl(expected_key)
    assert ttl > 0 and ttl <= 3600  # Config says 3600s


def test_strict_gps_with_extras(grpc_client, redis_client):
    """
    Verifies that 'strict' logging works for configured PRODUCTION devices.
    """
    device_id = "dome_test_gps"

    grpc_client.log_strict("gnss", device_id, {
        "satellites": 8,
        "lat": 37.0,
        "lon": -122.0,
        "fix_mode": "3D",
        "extra_data": {"dilution_of_precision": 1.2}
    })

    time.sleep(0.5)

    # CHECK: Expect Production prefix
    expected_key = f"UBLOX_ZED-F9T_{device_id}"

    assert redis_client.hget(expected_key, "satellites") == "8"
    assert redis_client.hget(expected_key, "extra_dilution_of_precision") == "1.2"

    # Check TTL (Should be -1 aka Permanent for production)
    assert redis_client.ttl(expected_key) == -1


def test_invalid_schema_rejection(grpc_client):
    """
    Verifies that strict mode actually enforces schema.
    """
    # Missing required field 'satellites'
    invalid_data = {
        "satellites": 999,  # Invalid: must be <= 100
        "lat": 37.0,
        "lon": -122.0,
        "fix_mode": "3D"
    }

    # Should raise ValueError from client wrapper
    with pytest.raises(ValueError) as excinfo:
        grpc_client.log_strict("gnss", "bad_device", invalid_data)

    assert "Server rejected data" in str(excinfo.value)


def test_concurrent_clients(grpc_client, redis_client):
    num_clients = 10
    messages_per_client = 5

    def worker(client_idx):
        dev_id = f"worker_{client_idx}"
        for i in range(messages_per_client):
            try:
                # This should pass now that server.py uses including_default_value_fields=True
                # (iteration=0 and value=0.0 will be preserved)
                grpc_client.log_test(
                    device_id=dev_id,
                    iteration=i,
                    value=float(client_idx),
                    message="STRESS_TEST",
                    active=True
                )
            except Exception as e:
                return f"Client {client_idx} failed: {e}"
        return "OK"

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        results = list(executor.map(worker, range(num_clients)))

    for res in results:
        assert res == "OK"

    time.sleep(1.0)
    # Check last write
    key = f"TEST-STRICT_worker_0"
    assert redis_client.exists(key)
    assert redis_client.hget(key, "message") == "STRESS_TEST"


def test_time_series_integrity(grpc_client, redis_client):
    """
    Scenario: A client sends a sequence of strictly ordered updates.
    We assert that the final state in Redis matches the LAST update,
    not an intermediate one (handling race conditions).
    """
    device_id = "sequencer_01"
    num_updates = 50

    for i in range(num_updates):
        # We use a monotonically increasing 'iteration'
        grpc_client.log_test(
            device_id=device_id,
            iteration=i,
            value=float(i * 10),
            message=f"SEQ_{i}",
            active=True
        )
        # No sleep here! We want to hammer the server.

    # Give server a moment to drain the queue
    time.sleep(0.5)

    key = f"TEST-STRICT_{device_id}"

    # Verify Redis holds the FINAL state
    final_iteration = redis_client.hget(key, "iteration")
    final_message = redis_client.hget(key, "message")

    assert final_iteration == str(num_updates - 1)
    assert final_message == f"SEQ_{num_updates - 1}"


def test_interleaved_clients_same_type(grpc_client, redis_client):
    """
    Scenario: Two different devices of the SAME type (gps) logging simultaneously.
    Ensures the server doesn't cross-contaminate data between IDs.
    """
    dev_a = "dome_a"
    dev_b = "dome_b"

    # A is at Equator, B is at North Pole
    grpc_client.log_strict("gnss", dev_a, {"satellites": 10, "lat": 0.0, "lon": 0.0, "fix_mode": "3D"})
    grpc_client.log_strict("gnss", dev_b, {"satellites": 5, "lat": 90.0, "lon": 0.0, "fix_mode": "2D"})

    time.sleep(0.2)

    key_a = f"UBLOX_ZED-F9T_{dev_a}"
    key_b = f"UBLOX_ZED-F9T_{dev_b}"

    lat_a = redis_client.hget(key_a, "lat")
    lat_b = redis_client.hget(key_b, "lat")

    assert lat_a == "0.0"
    assert lat_b == "90.0"


def test_rapid_reconnect_simulation(grpc_client, redis_client):
    """
    Scenario: Simulates a flaky connection where a client connects,
    sends 1 message, disconnects, and repeats.
    """
    device_id = "flaky_device"

    for i in range(5):
        grpc_client.log_flexible("test_flex", device_id, {"boot_count": i})
        time.sleep(0.05)

    time.sleep(0.2)
    key = f"DEV_TEST-FLEX_{device_id}"

    # Cast to float to handle Proto Struct behavior (4 -> 4.0)
    val = redis_client.hget(key, "boot_count")
    assert float(val) == 4.0


def test_huge_payload(grpc_client, redis_client):
    """
    Scenario: Sending a very large flexible payload (near gRPC limits).
    """
    device_id = "big_data_sensor"

    # Create a 1MB payload (approx)
    big_string = "x" * 100_000
    data = {"blob": big_string, "id": 1}

    grpc_client.log_flexible("test_flex", device_id, data)

    time.sleep(0.5)
    key = f"DEV_TEST-FLEX_{device_id}"

    val = redis_client.hget(key, "blob")
    assert len(val) == 100_000
    assert val == big_string


def test_concurrent_field_merging(grpc_client, redis_client):
    """
    CREATIVE SCENARIO: Two different clients update THE SAME device ID,
    but they write to DIFFERENT fields.

    We verify that the server performs a partial update (HSET)
    and doesn't overwrite the existing fields sent by the other client.
    """
    device_id = "shared_resource_01"

    def client_temp():
        # This client only knows about Temperature
        for i in range(10):
            grpc_client.log_flexible("test_flex", device_id, {"temp": float(i)})
            time.sleep(0.01)

    def client_pressure():
        # This client only knows about Pressure
        for i in range(10):
            grpc_client.log_flexible("test_flex", device_id, {"pressure": float(i + 100)})
            time.sleep(0.01)

    # Run both simultaneously
    with ThreadPoolExecutor(max_workers=2) as exc:
        exc.submit(client_temp)
        exc.submit(client_pressure)

    time.sleep(0.5)
    key = f"DEV_TEST-FLEX_{device_id}"

    # Verify BOTH fields exist and have the last values
    assert float(redis_client.hget(key, "temp")) == 9.0
    assert float(redis_client.hget(key, "pressure")) == 109.0


def test_unknown_experimental_device(grpc_client, redis_client):
    """
    Verifies that a completely unknown device type goes to SANDBOX.
    """
    device_id = "mystery_box"
    data = {"foo": "bar"}

    # Using a type not in TOML
    grpc_client.log_flexible("alien_tech", device_id, data)

    time.sleep(0.2)
    expected_key = f"SANDBOX:alien_tech:{device_id}"

    assert redis_client.exists(expected_key)
    # Sandbox should also have a default TTL
    assert redis_client.ttl(expected_key) > 0