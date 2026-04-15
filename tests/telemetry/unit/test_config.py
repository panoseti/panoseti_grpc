import pytest
from pydantic import ValidationError

from panoseti_grpc.telemetry.config import DeviceConfig, DewModel, GnssModel, PayloadTestModel, TelemetryConfig


# 1. Test Key Generation & whitelist logic
def test_redis_key_formatting() -> None:
    # FIX: Use DeviceConfig objects
    devices = {
        "gps": DeviceConfig(mode="production", redis_prefix="UBLOX_ZED-F9T_"),
        "weather": DeviceConfig(mode="production", redis_prefix="WEATHER_MAST_"),
    }
    cfg = TelemetryConfig(devices)

    # Assert correct formatting
    assert cfg.get_redis_key("gps", "dome_a") == "UBLOX_ZED-F9T_dome_a"

    # Assert unknown device error or sandbox fallback
    # The new logic returns SANDBOX:...
    assert cfg.get_redis_key("nuclear_reactor", "01") == "SANDBOX:nuclear_reactor:01"


# 2. Test Flattening Logic (The R&D Hook)
def test_payload_flattening() -> None:
    # FIX: Must be 'production' mode to trigger the schema validation & extra_data flattening logic
    # Also need to use a type that exists in SCHEMA_MAP (like 'gps' mapping to GnssModel)
    devices = {"gnss": DeviceConfig(mode="production", redis_prefix="GPS_")}
    cfg = TelemetryConfig(devices)

    raw_data = {
        "satellites": 12,
        "lat": 34.0,
        "lon": -118.0,
        "fix_mode": "3D",
        "extra_data": {"temp_correction": 0.05, "status": "ok"},
    }

    # Validate and Flatten
    flat_data = cfg.validate_and_flatten("gnss", raw_data)

    # Check that 'extra_data' dict is gone
    assert "extra_data" not in flat_data
    # Check that fields were hoisted to top level with prefix
    assert flat_data["extra_temp_correction"] == 0.05


# 3. Test Pydantic Constraints
def test_dew_model_constraints() -> None:
    # Good Case
    valid = DewModel(temp_c=25.0, humidity=40.0)
    assert valid.temp_c == 25.0

    # Bad Case: Humidity > 100%
    with pytest.raises(ValidationError) as excinfo:
        DewModel(temp_c=25.0, humidity=105.0)

    # Verify we get a helpful error message
    errors = excinfo.value.errors()
    assert errors[0]["loc"] == ("humidity",)
    assert "less than or equal to 100" in errors[0]["msg"]


# 4. Test GNSS Model Validation
def test_gnss_validation() -> None:
    # Valid
    valid = GnssModel(satellites=4, lat=37.7, lon=-122.4, fix_mode="3D")
    assert valid.satellites == 4

    # Invalid Latitude
    with pytest.raises(ValidationError) as exc:
        GnssModel(satellites=4, lat=95.0, lon=0, fix_mode="3D")  # Lat > 90
    assert "lat" in str(exc.value)


# 5. Test Custom Validator (Uppercase Message)
def test_custom_validator() -> None:
    # Valid
    valid = PayloadTestModel(iteration=1, value=1.0, message="HELLO", active=True)
    assert valid.message == "HELLO"

    # Invalid (Lower case)
    with pytest.raises(ValidationError) as exc:
        PayloadTestModel(iteration=1, value=1.0, message="hello", active=True)
    assert "Message must be uppercase" in str(exc.value)


# 6. Test Empty/Null Data Handling
def test_partial_payload_filling() -> None:
    # Pydantic models usually require all fields unless marked Optional
    # Let's verify that missing fields raise errors
    with pytest.raises(ValidationError) as exc:
        GnssModel(lat=34.0, lon=-118.0)  # Missing 'satellites', 'fix_mode'
    assert "satellites" in str(exc.value)


# 7. Test Extra Fields Behavior
def test_forbid_unknown_fields_in_strict_model() -> None:
    # By default, Pydantic might ignore extra fields, but we want to ensure
    # users aren't sending typos thinking they are being recorded.
    # Note: If your Config doesn't set extra='forbid', this test confirms they are ignored/allowed.

    data = {
        "satellites": 5,
        "lat": 1.0,
        "lon": 1.0,
        "fix_mode": "2D",
        "typo_field": "oops",  # This is NOT in 'extra_data'
    }
    model = GnssModel(**data)
    # Check that 'typo_field' is NOT in the dumped model (unless extra='allow')
    assert "typo_field" not in model.model_dump()


# 8. Test Complex Nested 'extra_data' Flattening
def test_deep_extra_data_flattening() -> None:
    devices = {"gnss": DeviceConfig(mode="production", redis_prefix="GPS_")}
    cfg = TelemetryConfig(devices)

    raw_data = {
        "satellites": 5,
        "lat": 0,
        "lon": 0,
        "fix_mode": "2D",
        "extra_data": {
            "sensor_temp": 45.2,
            "status_flags": {"error": False, "calibrated": True},  # Nested Dict!
        },
    }

    # Run the flattening logic
    flat = cfg.validate_and_flatten("gnss", raw_data)

    # The implementation flattens nested dicts recursively using _flatten_dict
    # After 'extra_data' is popped and its content merged with prefix 'extra_',
    # 'status_flags' becomes 'extra_status_flags'.
    # Since 'status_flags' is a dict, _flatten_dict will recurse.
    # Result: 'extra_status_flags_error'

    assert "extra_status_flags_error" in flat
    assert flat["extra_status_flags_error"] is False


# 9. Test Type Coercion (Feature, not bug)
def test_type_coercion() -> None:
    # Pydantic attempts to cast types. sending string "5" for an int field should work.
    data = {"iteration": "5", "value": "10.5", "message": "OK", "active": "true"}
    model = PayloadTestModel(**data)
    assert model.iteration == 5
    assert model.value == 10.5
    assert model.active is True


# 10. Test Validator Logic on Edge Cases
def test_gnss_edge_coordinates() -> None:
    # Test strictly valid coordinates
    assert GnssModel(satellites=0, lat=90.0, lon=180.0, fix_mode="0").lat == 90.0
    assert GnssModel(satellites=0, lat=-90.0, lon=-180.0, fix_mode="0").lat == -90.0

    # Test just out of bounds
    with pytest.raises(ValidationError):
        GnssModel(satellites=0, lat=90.00001, lon=0, fix_mode="0")


# 11. Test Prefix Enforcement
def test_experimental_prefix_enforcement() -> None:
    with pytest.raises(ValidationError) as exc:
        DeviceConfig(mode="experimental", redis_prefix="WRONG_PREFIX_")
    assert "must start with 'DEV_'" in str(exc.value)

    # Valid config should pass
    valid = DeviceConfig(mode="experimental", redis_prefix="DEV_CORRECT_")
    assert valid.redis_prefix == "DEV_CORRECT_"


# 12. Test TTL Logic
def test_ttl_retrieval() -> None:
    # FIX: Construct with DeviceConfig
    devices = {
        "prod_gps": DeviceConfig(mode="production", redis_prefix="GPS_", ttl_seconds=0),
        "temp_sensor": DeviceConfig(mode="experimental", redis_prefix="DEV_TEMP_", ttl_seconds=3600),
    }
    cfg = TelemetryConfig(devices)

    assert cfg.get_ttl("prod_gps") == 0
    assert cfg.get_ttl("temp_sensor") == 3600
    assert cfg.get_ttl("unknown_thing") == 3600  # Default fallback


# 13. Test Validation Skipping for Experimental
def test_experimental_skips_validation() -> None:
    # Setup config with one experimental type
    from panoseti_grpc.telemetry.config import DeviceConfig

    devices = {"new_proto": DeviceConfig(mode="experimental", redis_prefix="DEV_PROTO_")}
    cfg = TelemetryConfig(devices)

    # Arbitrary data that would fail any strict schema
    raw_data = {"random_junk": 123, "nested": {"a": 1}}

    # Should NOT raise error, just flatten
    result = cfg.validate_and_flatten("new_proto", raw_data)

    assert result["random_junk"] == 123
    assert result["nested_a"] == 1


def test_strict_enforces_schema() -> None:
    from panoseti_grpc.telemetry.config import DeviceConfig

    # 'gnss' is hardcoded in SCHEMA_MAP in config.py
    devices = {"gnss": DeviceConfig(mode="production", redis_prefix="GPS_")}
    cfg = TelemetryConfig(devices)

    # Missing required fields for GNSS
    bad_data = {"lat": 10.0}

    with pytest.raises(ValueError, match="Schema Violation"):
        cfg.validate_and_flatten("gnss", bad_data)
