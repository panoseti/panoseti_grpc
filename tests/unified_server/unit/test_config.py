"""
Unit tests for PanosetiServerConfig — TOML loading, profile switching, and Pydantic validation.

No running server or external services required.
"""

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_control.config import DaqControlServerConfig
from panoseti_grpc.server import PanosetiServerConfig, ServiceToggles
from panoseti_grpc.telemetry.config import TelemetryServerConfig

_PORT_ENV_VARS = ("GRPC_PORT", "HEADNODE_GRPC_PORT", "DAQNODE_GRPC_PORT")


@pytest.fixture(autouse=True)
def _clean_port_env(monkeypatch: Any) -> None:
    """Every test in this file starts with none of the port env vars set.

    PanosetiServerConfig.port's default_factory reads GRPC_PORT (added in
    33ff557 to make the port configurable without a TOML edit), so a
    literal `assert cfg.port == 50051` is only deterministic if GRPC_PORT
    is actually unset. It usually is on a bare `pytest` invocation, but
    this suite's own docker-compose.test.yml sets `GRPC_PORT=50055` for
    the sibling integration tests that need a real server listening on a
    known, collision-avoiding port -- confirmed live in CI: these tests
    failed with "assert 50055 == 50051" the first time they actually ran
    in that container instead of a bare host `pytest`. This file is
    pure-unit (no running server, per the module docstring), so clearing
    the var here can't affect the integration tests that need it set.
    """
    for var in _PORT_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


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
    """Headnode profile enables telemetry + daq_data (gateway role), not daq_control.

    daq_data must be *wired* as gateway role (role="gateway" + a
    [daq_data.gateway] section), not just toggled on -- without that it
    silently defaults to role="edge", which expects local Hashpipe UDS
    sockets that don't exist on a head node.
    """
    cfg = PanosetiServerConfig.load_profile("headnode")
    assert cfg.services.telemetry
    assert cfg.services.daq_data
    assert not cfg.services.daq_control
    assert cfg.daq_data.role == "gateway"


def test_load_gateway_profile_same_shape_as_headnode() -> None:
    """'gateway' and 'headnode' profiles enable the same services/role."""
    headnode = PanosetiServerConfig.load_profile("headnode")
    gateway = PanosetiServerConfig.load_profile("gateway")
    assert headnode.services == gateway.services
    assert headnode.daq_data.role == gateway.daq_data.role == "gateway"


def test_no_profile_hardcodes_a_port(monkeypatch: Any) -> None:
    """No bundled profile TOML sets an explicit [server].port.

    An explicit TOML port silently wins over every env var (see
    unified_main.resolve_bind_port's docstring) and is exactly how the
    headnode profile desynced from HEADNODE_GRPC_PORT-driven clients in the
    past (it shipped with `port = 50052` hardcoded). Every profile must
    resolve its port from GRPC_PORT (or the default), never a TOML literal,
    so `--port-env` in unified_main.py is the only thing that can move it.
    """
    monkeypatch.delenv("GRPC_PORT", raising=False)
    for profile in ("default", "daq_node", "headnode", "gateway"):
        cfg = PanosetiServerConfig.load_profile(profile)
        assert cfg.port == 50051, f"profile {profile!r} did not fall back to the 50051 default"

    monkeypatch.setenv("GRPC_PORT", "50099")
    for profile in ("default", "daq_node", "headnode", "gateway"):
        cfg = PanosetiServerConfig.load_profile(profile)
        assert cfg.port == 50099, f"profile {profile!r} ignored GRPC_PORT -- an explicit TOML port line leaked back in"


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


def test_telemetry_redis_host_from_env(monkeypatch: Any) -> None:
    """TelemetryServerConfig picks up REDIS_HOST env var for redis_host default."""
    monkeypatch.setenv("REDIS_HOST", "my-custom-redis")
    cfg = TelemetryServerConfig()
    assert cfg.redis_host == "my-custom-redis"


def test_telemetry_redis_host_fallback(monkeypatch: Any) -> None:
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


# ---------------------------------------------------------------------------
# unified_main.resolve_bind_port — the server-side half of the single
# source of truth for gRPC ports (control.utils.util.resolve_grpc_port is
# the client-side half; they must agree on precedence or server and client
# desync exactly like the headnode profile's old hardcoded 50052 did).
#
# Plain (port, port_env, cfg_port) params, not an argparse.Namespace/Typer
# context object -- this function is shared verbatim by BOTH CLI entry
# points that start the unified server: unified_main.main() (argparse,
# `python -m panoseti_grpc`) and _cli/server.py (Typer, the actual
# `pseti-grpc server` console script). They independently duplicate the
# config-load/service-toggle/run sequence and had already drifted once
# (_cli/server.py never gained --port/--port-env at all until this was
# noticed live against real hardware -- pseti-grpc server IS _cli/server.py,
# not unified_main.py, so a fix only applied there is dead code from the
# real CLI's perspective).
# ---------------------------------------------------------------------------


def test_resolve_bind_port_no_override_falls_back_to_cfg_port(monkeypatch: Any) -> None:
    from panoseti_grpc.unified_main import resolve_bind_port

    monkeypatch.delenv("DAQNODE_GRPC_PORT", raising=False)
    assert resolve_bind_port(port=None, port_env="DAQNODE_GRPC_PORT", cfg_port=12345) == 12345


def test_resolve_bind_port_env_var_wins_over_cfg_port(monkeypatch: Any) -> None:
    from panoseti_grpc.unified_main import resolve_bind_port

    monkeypatch.setenv("DAQNODE_GRPC_PORT", "50055")
    assert resolve_bind_port(port=None, port_env="DAQNODE_GRPC_PORT", cfg_port=12345) == 50055


def test_resolve_bind_port_explicit_flag_wins_over_env_var(monkeypatch: Any) -> None:
    from panoseti_grpc.unified_main import resolve_bind_port

    monkeypatch.setenv("DAQNODE_GRPC_PORT", "50055")
    assert resolve_bind_port(port=60000, port_env="DAQNODE_GRPC_PORT", cfg_port=12345) == 60000


def test_resolve_bind_port_no_port_env_ignores_env(monkeypatch: Any) -> None:
    """Without --port-env, an env var of the same name has no effect (must be named explicitly)."""
    from panoseti_grpc.unified_main import resolve_bind_port

    monkeypatch.setenv("DAQNODE_GRPC_PORT", "50055")
    assert resolve_bind_port(port=None, port_env=None, cfg_port=12345) == 12345


def test_resolve_bind_port_distinguishes_head_and_daq_roles(monkeypatch: Any) -> None:
    """The whole reason for --port-env: two roles, two vars, no collision on a co-located node."""
    from panoseti_grpc.unified_main import resolve_bind_port

    monkeypatch.setenv("HEADNODE_GRPC_PORT", "50051")
    monkeypatch.setenv("DAQNODE_GRPC_PORT", "50052")
    head_port = resolve_bind_port(port=None, port_env="HEADNODE_GRPC_PORT", cfg_port=50051)
    daq_port = resolve_bind_port(port=None, port_env="DAQNODE_GRPC_PORT", cfg_port=50051)
    assert head_port == 50051
    assert daq_port == 50052
    assert head_port != daq_port


def test_cli_server_app_exposes_port_env_option() -> None:
    """Regression guard: `pseti-grpc server` is _cli/server.py's Typer app,
    NOT unified_main.py's argparse parser (that's only reached via
    `python -m panoseti_grpc`). A --port-env fix applied solely to
    unified_main.py is invisible to the real console script and every
    docker-compose `command:` / systemd unit that passes it -- confirmed
    live against real hardware (container crash-looped with "No such
    option: --port-env"). This test fails loudly if the two entry points
    drift apart on their CLI surface again.
    """
    from typer.testing import CliRunner

    from panoseti_grpc._cli.server import app

    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "--port-env" in result.output
    assert "--port " in result.output or "--port\n" in result.output or "--port]" in result.output


# ---------------------------------------------------------------------------
# DaqDataGatewayConfig.edge_port — must track DAQNODE_GRPC_PORT so a
# fleet-wide port change doesn't require also editing the gateway's TOML.
# ---------------------------------------------------------------------------


def test_gateway_edge_port_defaults_from_daqnode_grpc_port_env(monkeypatch: Any) -> None:
    from panoseti_grpc.daq_data.config import DaqDataGatewayConfig

    monkeypatch.delenv("DAQNODE_GRPC_PORT", raising=False)
    assert DaqDataGatewayConfig().edge_port == 50051

    monkeypatch.setenv("DAQNODE_GRPC_PORT", "50077")
    assert DaqDataGatewayConfig().edge_port == 50077


def test_gateway_edge_port_explicit_value_still_overrides(monkeypatch: Any) -> None:
    """An explicit edge_port (e.g. from a per-site TOML) still wins -- only the *default* is env-driven."""
    from panoseti_grpc.daq_data.config import DaqDataGatewayConfig

    monkeypatch.setenv("DAQNODE_GRPC_PORT", "50077")
    assert DaqDataGatewayConfig(edge_port=51234).edge_port == 51234
