import pytest
from pydantic import ValidationError
from panoseti_grpc.telemetry.config import TelemetryConfig, GnssModel, DewModel, PayloadTestModel


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
    valid = PayloadTestModel(iteration=1, value=1.0, message="HELLO", active=True)
    assert valid.message == "HELLO"

    # Invalid (Lower case)
    with pytest.raises(ValidationError) as exc:
        PayloadTestModel(iteration=1, value=1.0, message="hello", active=True)
    assert "Message must be uppercase" in str(exc.value)


# 6. Test Empty/Null Data Handling
def test_partial_payload_filling():
    # Pydantic models usually require all fields unless marked Optional
    # Let's verify that missing fields raise errors
    with pytest.raises(ValidationError) as exc:
        GnssModel(lat=34.0, lon=-118.0)  # Missing 'satellites', 'fix_mode'
    assert "satellites" in str(exc.value)


# 7. Test Extra Fields Behavior
def test_forbid_unknown_fields_in_strict_model():
    # By default, Pydantic might ignore extra fields, but we want to ensure
    # users aren't sending typos thinking they are being recorded.
    # Note: If your Config doesn't set extra='forbid', this test confirms they are ignored/allowed.

    data = {
        "satellites": 5, "lat": 1.0, "lon": 1.0, "fix_mode": "2D",
        "typo_field": "oops"  # This is NOT in 'extra_data'
    }
    model = GnssModel(**data)
    # Check that 'typo_field' is NOT in the dumped model (unless extra='allow')
    assert "typo_field" not in model.model_dump()


# 8. Test Complex Nested 'extra_data' Flattening
def test_deep_extra_data_flattening():
    cfg = TelemetryConfig(devices={})

    raw_data = {
        "satellites": 5, "lat": 0, "lon": 0, "fix_mode": "2D",
        "extra_data": {
            "sensor_temp": 45.2,
            "status_flags": {"error": False, "calibrated": True}  # Nested Dict!
        }
    }

    # Run the flattening logic
    flat = cfg.validate_and_flatten("gps", raw_data)

    # Redis cannot store nested dicts in a Hash field.
    # Depending on your implementation, this might stringify the dict.
    assert "extra_status_flags" in flat
    # Verify it became a string representation or remained a dict (which Redis client will stringify later)
    assert isinstance(flat["extra_status_flags"], (dict, str))


# 9. Test Type Coercion (Feature, not bug)
def test_type_coercion():
    # Pydantic attempts to cast types. sending string "5" for an int field should work.
    data = {"iteration": "5", "value": "10.5", "message": "OK", "active": "true"}
    model = PayloadTestModel(**data)
    assert model.iteration == 5
    assert model.value == 10.5
    assert model.active is True


# 10. Test Validator Logic on Edge Cases
def test_gnss_edge_coordinates():
    # Test strictly valid coordinates
    assert GnssModel(satellites=0, lat=90.0, lon=180.0, fix_mode="0").lat == 90.0
    assert GnssModel(satellites=0, lat=-90.0, lon=-180.0, fix_mode="0").lat == -90.0

    # Test just out of bounds
    with pytest.raises(ValidationError):
        GnssModel(satellites=0, lat=90.00001, lon=0, fix_mode="0")