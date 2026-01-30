import pytest
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor


def test_flexible_struct_flow(grpc_client, redis_client):
    device_id = "test_flex_01"
    rnd_data = {"voltage": 5.1, "fan_speed": 1200, "status": "OK"}

    # Send via gRPC
    grpc_client.log_flexible("test", device_id, rnd_data)

    # Wait for async write
    time.sleep(0.2)

    expected_key = f"TEST_INTEGRATION_{device_id}"
    assert redis_client.exists(expected_key)
    assert redis_client.hget(expected_key, "voltage") == "5.1"


def test_strict_gps_with_extras(grpc_client, redis_client):
    device_id = "dome_test_gps"

    # FIX: Use "gnss" instead of "gps" to match the Client logic
    grpc_client.log("gnss", device_id, {
        "satellites": 8,
        "lat": 37.0,
        "lon": -122.0,
        "fix_mode": "3D",
        "extra_data": {"dilution_of_precision": 1.2}
    })

    time.sleep(0.2)
    # FIX: Ensure config maps "gnss" -> "UBLOX_ZED-F9T_"
    expected_key = f"UBLOX_ZED-F9T_{device_id}"

    assert redis_client.hget(expected_key, "lat") == "37.0"
    assert redis_client.hget(expected_key, "extra_dilution_of_precision") == "1.2"


def test_invalid_schema_rejection(grpc_client):
    device_id = "bad_sensor"
    bad_data = {"temp_c": 20.0, "humidity": 150.0}  # Humidity > 100

    with pytest.raises(ValueError) as excinfo:
        grpc_client.log("dew", device_id, bad_data)

    assert "Server rejected data" in str(excinfo.value)


# --- NEW EXTENSION: Multi-Client Load Test ---
def test_concurrent_clients(grpc_client, redis_client):
    """
    Simulates 10 clients sending data simultaneously to verify AsyncIO server stability.
    """
    num_clients = 10
    messages_per_client = 5

    def worker(client_idx):
        # Each worker pretends to be a different device
        dev_id = f"worker_{client_idx}"
        for i in range(messages_per_client):
            try:
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

    # Use ThreadPool to simulate concurrent network requests
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        results = list(executor.map(worker, range(num_clients)))

    # 1. Verify all clients finished successfully
    for res in results:
        assert res == "OK"

    # 2. Verify Data in Redis
    time.sleep(1.0)  # Let the server catch up
    for i in range(num_clients):
        key = f"TEST_INTEGRATION_worker_{i}"
        assert redis_client.exists(key)
        # Check that the LAST message (iteration 4) matches
        assert redis_client.hget(key, "iteration") == str(messages_per_client - 1)