"""
Unit tests for PanosetiServerConfig — TOML loading, profile switching, and Pydantic validation.

No running server or external services required.
"""

from typing import Any
from pathlib import Path

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_control.config import DaqControlServerConfig
from panoseti_grpc.server import PanosetiServerConfig, ServiceToggles
from panoseti_grpc.telemetry.config import TelemetryServerConfig

# ---------------------------------------------------------------------------
# Bundled profile loading
# ---------------------------------------------------------------------------


def test_load_default_profile() -> None:
    """Default profile enables all three services on port 50051."""
    cfg = PanosetiServerConfig.load_default()
    assert cfg.services.telemetry
    assert cfg.services.daq_data
    assert cfg.services.daq_control
    assert cfg.port == 50051


def test_load_daq_node_profile() -> None:
    """DAQ-node profile disables telemetry, keeps daq_data and daq_control."""
    cfg = PanosetiServerConfig.load_profile("daq_node")
    assert not cfg.services.telemetry
    assert cfg.services.daq_data
    assert cfg.services.daq_control


def test_load_headnode_profile() -> None:
    """Headnode profile enables only telemetry."""
    cfg = PanosetiServerConfig.load_profile("headnode")
    assert cfg.services.telemetry
    assert not cfg.services.daq_data
    assert not cfg.services.daq_control


def test_load_profile_alias_default() -> None:
    """'default' profile is equivalent to load_default()."""
    cfg_alias = PanosetiServerConfig.load_profile("default")
    cfg_default = PanosetiServerConfig.load_default()
    assert cfg_alias.services == cfg_default.services
    assert cfg_alias.port == cfg_default.port


def test_invalid_profile_name() -> None:
    """Unknown profile name raises ValueError."""
    with pytest.raises(ValueError, match="Unknown profile"):
        PanosetiServerConfig.load_profile("bogus_profile")


# ---------------------------------------------------------------------------
# from_toml with custom files
# ---------------------------------------------------------------------------


def test_from_toml_minimal(tmp_path: Path) -> None:
    """Minimal TOML with [server] section loads with correct port."""
    toml_content = b"""
[server]
port = 9999

[server.services]
telemetry   = false
daq_data    = true
daq_control = false
"""
    toml_file = tmp_path / "server.toml"
    toml_file.write_bytes(toml_content)
    cfg = PanosetiServerConfig.from_toml(toml_file)
    assert cfg.port == 9999
    assert not cfg.services.telemetry
    assert cfg.services.daq_data
    assert not cfg.services.daq_control


def test_from_toml_no_server_section_uses_defaults(tmp_path: Path) -> None:
    """TOML without [server] section defaults all server-level fields."""
    toml_file = tmp_path / "server.toml"
    toml_file.write_bytes(b"# empty config\n")
    cfg = PanosetiServerConfig.from_toml(toml_file)
    # All defaults
    assert cfg.port == 50051
    assert cfg.services.telemetry
    assert cfg.services.daq_data
    assert cfg.services.daq_control


def test_from_toml_file_not_found() -> None:
    """Missing TOML file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        PanosetiServerConfig.from_toml("/nonexistent/path/server.toml")


def test_from_toml_unknown_keys_ignored(tmp_path: Path) -> None:
    """Extra unknown keys in TOML are silently ignored (extra='ignore')."""
    toml_content = b"""
[server]
port = 50051
totally_unknown_key = "should be ignored"

[server.services]
telemetry = true
daq_data = false
daq_control = false
"""
    toml_file = tmp_path / "server.toml"
    toml_file.write_bytes(toml_content)
    cfg = PanosetiServerConfig.from_toml(toml_file)
    assert cfg.port == 50051


def test_from_toml_full_config(tmp_path: Path) -> None:
    """Full TOML with per-service sections propagates nested fields."""
    toml_content = b"""
[server]
port = 50060
shutdown_grace_period = 10.0

[server.services]
telemetry   = true
daq_data    = false
daq_control = false

[telemetry]
redis_host = "my-redis"
redis_port = 6380
redis_db   = 3
"""
    toml_file = tmp_path / "server.toml"
    toml_file.write_bytes(toml_content)
    cfg = PanosetiServerConfig.from_toml(toml_file)
    assert cfg.port == 50060
    assert cfg.shutdown_grace_period == 10.0
    assert cfg.telemetry.redis_host == "my-redis"
    assert cfg.telemetry.redis_port == 6380
    assert cfg.telemetry.redis_db == 3


# ---------------------------------------------------------------------------
# _parse_toml_dict — [server] section merging
# ---------------------------------------------------------------------------


def test_parse_toml_dict_merges_server_section() -> None:
    """_parse_toml_dict lifts [server] keys to top level before validation."""
    raw = {
        "server": {
            "port": 9001,
            "shutdown_grace_period": 2.5,
            "services": {"telemetry": False, "daq_data": True, "daq_control": False},
        },
        "daq_data": {},
    }
    # _parse_toml_dict mutates raw (pops "server"), so copy first
    cfg = PanosetiServerConfig._parse_toml_dict(dict(raw))
    assert cfg.port == 9001
    assert cfg.shutdown_grace_period == 2.5
    assert not cfg.services.telemetry
    assert cfg.services.daq_data
    assert not cfg.services.daq_control


def test_parse_toml_dict_no_server_key() -> None:
    """_parse_toml_dict with no 'server' key falls back to all defaults."""
    cfg = PanosetiServerConfig._parse_toml_dict({})
    assert cfg.port == 50051


# ---------------------------------------------------------------------------
# Pydantic field validation
# ---------------------------------------------------------------------------


def test_port_too_low_raises() -> None:
    """Port below 1024 must be rejected."""
    with pytest.raises(ValidationError, match="port"):
        PanosetiServerConfig(port=80)


def test_port_too_high_raises() -> None:
    """Port above 65535 must be rejected."""
    with pytest.raises(ValidationError, match="port"):
        PanosetiServerConfig(port=99999)


def test_port_boundary_values_valid() -> None:
    """Port 1024 and 65535 are both valid."""
    PanosetiServerConfig(port=1024)
    PanosetiServerConfig(port=65535)


def test_shutdown_grace_period_negative_raises() -> None:
    """Negative shutdown_grace_period must be rejected."""
    with pytest.raises(ValidationError):
        PanosetiServerConfig(shutdown_grace_period=-1.0)


def test_service_toggles_roundtrip() -> None:
    """ServiceToggles model_dump → model_validate preserves all values."""
    original = ServiceToggles(telemetry=False, daq_data=True, daq_control=False)
    restored = ServiceToggles.model_validate(original.model_dump())
    assert restored == original


def test_daq_control_config_log_level_invalid() -> None:
    """DaqControlServerConfig rejects an invalid log_level string."""
    with pytest.raises(ValidationError, match="log_level"):
        DaqControlServerConfig(log_level="VERBOSE")


def test_daq_control_config_valid_defaults() -> None:
    """DaqControlServerConfig accepts all default values."""
    cfg = DaqControlServerConfig()
    assert cfg.grpc_port == 50051
    assert cfg.log_level == "INFO"
    assert cfg.shutdown_grace_period == 5.0


# ---------------------------------------------------------------------------
# TelemetryServerConfig — env var defaults
# ---------------------------------------------------------------------------


def test_telemetry_redis_host_from_env( monkeypatch: Any) -> None:
    """TelemetryServerConfig picks up REDIS_HOST env var for redis_host default."""
    monkeypatch.setenv("REDIS_HOST", "my-custom-redis")
    cfg = TelemetryServerConfig()
    assert cfg.redis_host == "my-custom-redis"


def test_telemetry_redis_host_fallback( monkeypatch: Any) -> None:
    """Without REDIS_HOST env var, TelemetryServerConfig defaults to 'localhost'."""
    monkeypatch.delenv("REDIS_HOST", raising=False)
    cfg = TelemetryServerConfig()
    assert cfg.redis_host == "localhost"


def test_telemetry_config_valid_log_levels() -> None:
    """TelemetryServerConfig accepts all five valid log level strings."""
    for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        cfg = TelemetryServerConfig(log_level=level)
        assert cfg.log_level == level


def test_telemetry_config_invalid_log_level() -> None:
    """TelemetryServerConfig rejects an invalid log_level string."""
    with pytest.raises(ValidationError, match="log_level"):
        TelemetryServerConfig(log_level="TRACE")
