import pytest
import time
from google.protobuf.struct_pb2 import Struct
from panoseti_grpc.telemetry.generated import telemetry_pb2

# Import the user's script logic to simulate snapshotting
# Assuming storeInfluxDB.py is in the python path or root
try:
    import storeInfluxDB
except ImportError:
    # Fallback if running outside docker where path isn't set
    import sys

    sys.path.append(".")
    import storeInfluxDB


def test_telemetry_pipeline(grpc_client, redis_client, influx_client):
    """
    End-to-End Test:
    1. Send Data (Client) -> 2. Verify Redis -> 3. Trigger Influx Write -> 4. Verify InfluxDB
    """

    # --- 1. Validation Logic (Bad Case) ---
    # Sending 'test' type with lower case message (Pydantic validator says must be UPPER)
    with pytest.raises(ValueError) as excinfo:
        grpc_client.log_test("runner_01", iteration=1, value=5.5, message="lower_case_bad", active=True)
    assert "Message must be uppercase" in str(excinfo.value)

    # --- 2. Whitelist/Type Logic (Bad Case) ---
    # Sending unknown type
    with pytest.raises(Exception) as excinfo:
        grpc_client.log_flexible("alien_device", "01", {"val": 1})
    assert "Unknown device type" in str(excinfo.value)

    # --- 3. Success Case (Write Data) ---
    # Send valid Test data
    grpc_client.log_test("runner_01", iteration=42, value=123.456, message="SYSTEM_OK", active=True)

    # Send valid GNSS data (with extra flexible field)
    # Note: Client implementation of log_gnss needs to handle extra_data dict
    # For this test, we construct the request manually or assume client handles it.
    grpc_client.log("gps", "dome_a", {
        "satellites": 8, "lat": 37.0, "lon": -122.0, "fix_mode": "3D",
        "extra_data": {"temp_correction": 0.5}
    })

    # --- 4. Verify Redis State ---
    # Wait for async write
    time.sleep(0.5)

    # Check Test Data in Redis
    test_key = "TEST_INTEGRATION_runner_01"
    assert redis_client.exists(test_key)
    assert redis_client.hget(test_key, "message") == "SYSTEM_OK"
    assert redis_client.hget(test_key, "active") == "1"  # Redis stores as string "1"

    # Check GPS Data in Redis
    gps_key = "UBLOX_ZED-F9T_dome_a"
    assert redis_client.exists(gps_key)
    # Check that extra_data was flattened
    assert redis_client.hget(gps_key, "extra_temp_correction") == "0.5"

    # --- 5. Trigger Snapshot to InfluxDB ---
    # We use the logic from storeInfluxDB.py directly
    # We need to mock the regex list inside storeInfluxDB if TEST type isn't there,
    # OR we add TEST type to storeInfluxDB.py.
    # For this test, let's inject the regex dynamically:
    import re
    storeInfluxDB.DATATYPE_FORMAT['test'] = re.compile("TEST_INTEGRATION_.*")

    # Run the sync function
    redis_keys = [test_key, gps_key]
    key_timestamps = {}
    storeInfluxDB.write_redis_to_influx(influx_client, redis_client, redis_keys, key_timestamps)

    # --- 6. Verify InfluxDB ---
    # Query Test Data
    result_test = list(influx_client.query("SELECT * FROM TEST_INTEGRATION_runner_01").get_points())
    assert len(result_test) >= 1
    last_point = result_test[-1]

    # Type Checks: InfluxDB stores numbers as floats/ints.
    # Ensure they didn't get stored as strings.
    assert isinstance(last_point['value'], float)
    assert last_point['value'] == 123.456
    assert last_point['message'] == "SYSTEM_OK"

    # Query GPS Data
    result_gps = list(influx_client.query("SELECT * FROM UBLOX_ZED-F9T_dome_a").get_points())
    assert len(result_gps) >= 1
    gps_point = result_gps[-1]
    assert gps_point['satellites'] == 8
    # Check flexible field in Influx
    assert gps_point['extra_temp_correction'] == 0.5