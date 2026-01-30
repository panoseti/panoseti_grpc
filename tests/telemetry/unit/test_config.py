import pytest
from pydantic import ValidationError
from panoseti_grpc.telemetry.config import TelemetryConfig, GnssModel, DewModel, TestModel


# 1. Test Key Generation & whitelist logic
def test_redis_key_formatting():
    # Mock a config dictionary directly
    config_data = {
        "devices": {
            "gps": {"type": "gps", "redis_prefix": "UBLOX_ZED-F9T_"},
            "weather": {"type": "weather", "redis_prefix": "WEATHER_MAST_"}
        }
    }
    cfg = TelemetryConfig(**config_data)

    # Assert correct formatting
    assert cfg.get_redis_key("gps", "dome_a") == "UBLOX_ZED-F9T_dome_a"

    # Assert unknown device error
    with pytest.raises(ValueError, match="Unknown device type"):
        cfg.get_redis_key("nuclear_reactor", "01")


# 2. Test Flattening Logic (The R&D Hook)
def test_payload_flattening():
    cfg = TelemetryConfig(devices={})  # Empty config is fine for this method

    raw_data = {
        "satellites": 12,
        "lat": 34.0,
        "lon": -118.0,
        "fix_mode": "3D",
        "extra_data": {"temp_correction": 0.05, "status": "ok"}
    }

    # Validate and Flatten
    flat_data = cfg.validate_and_flatten("gps", raw_data)

    # Check that 'extra_data' dict is gone
    assert "extra_data" not in flat_data
    # Check that fields were hoisted to top level with prefix
    assert flat_data["extra_temp_correction"] == 0.05
    assert flat_data["extra_status"] == "ok"


# 3. Test Pydantic Constraints
def test_dew_model_constraints():
    # Good Case
    valid = DewModel(temp_c=25.0, humidity=40.0)
    assert valid.temp_c == 25.0

    # Bad Case: Humidity > 100%
    with pytest.raises(ValidationError) as excinfo:
        DewModel(temp_c=25.0, humidity=105.0)

    # Verify we get a helpful error message
    errors = excinfo.value.errors()
    assert errors[0]['loc'] == ('humidity',)
    assert "less than or equal to 100" in errors[0]['msg']


# 4. Test GNSS Model Validation
def test_gnss_validation():
    # Valid
    valid = GnssModel(satellites=4, lat=37.7, lon=-122.4, fix_mode="3D")
    assert valid.satellites == 4

    # Invalid Latitude
    with pytest.raises(ValidationError) as exc:
        GnssModel(satellites=4, lat=95.0, lon=0, fix_mode="3D")  # Lat > 90
    assert "lat" in str(exc.value)


# 5. Test Custom Validator (Uppercase Message)
def test_custom_validator():
    # Valid
    valid = TestModel(iteration=1, value=1.0, message="HELLO", active=True)
    assert valid.message == "HELLO"

    # Invalid (Lower case)
    with pytest.raises(ValidationError) as exc:
        TestModel(iteration=1, value=1.0, message="hello", active=True)
    assert "Message must be uppercase" in str(exc.value)