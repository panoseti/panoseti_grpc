"""Unit tests for DaqDataGatewayServicer.startup()'s per-node host/port
resolution -- previously entirely untested. gRPC channels are lazy
(grpc.aio.insecure_channel doesn't connect until an RPC is issued), so
startup() can be exercised against config files pointing at nodes with
nothing actually listening, and asserted against purely by inspecting the
resulting edge targets.

Precedence under test, highest first:
  1. network_config.json's per-node grpc_port (nested under
     port_forwarding when status=True, the sibling field when status=False)
  2. [daq_data.gateway].edge_port, only when explicitly set in the config
     (not just resolved via its own DAQNODE_GRPC_PORT/50051 default_factory
     -- distinguished here via DaqDataGatewayConfig.model_fields_set)
  3. the EDGENODE_GRPC_PORT env var
  4. the built-in 50051 default
"""

import json
from pathlib import Path
from typing import Any

import pytest

from panoseti_grpc.daq_data.aggregator import DaqDataGatewayServicer
from panoseti_grpc.daq_data.config import DaqDataGatewayConfig, DaqDataServerConfig


@pytest.fixture(autouse=True)
def _clean_gateway_path_env(monkeypatch: Any) -> None:
    """_resolve_gateway_path() falls back to these env vars when a config
    field is left None, and EDGENODE_GRPC_PORT is tier 3 of the port
    resolution itself -- clear all three so a stray dev/.env value can't
    leak into these tests.
    """
    monkeypatch.delenv("PSETI_GRPC_DAQ_CONFIG", raising=False)
    monkeypatch.delenv("PSETI_GRPC_NETWORK_CONFIG", raising=False)
    monkeypatch.delenv("EDGENODE_GRPC_PORT", raising=False)


def _write_configs(tmp_path: Path, daq_nodes: list[dict], net_daq_nodes: list[dict] | None) -> tuple[Path, Path | None]:
    daq_cfg_path = tmp_path / "daq_config.json"
    daq_cfg_path.write_text(json.dumps({"daq_nodes": daq_nodes}))

    net_cfg_path = None
    if net_daq_nodes is not None:
        net_cfg_path = tmp_path / "network_config.json"
        net_cfg_path.write_text(json.dumps({"daq_nodes": net_daq_nodes}))

    return daq_cfg_path, net_cfg_path


def _make_servicer(
    daq_cfg_path: Path, net_cfg_path: Path | None, edge_port: int | None = None
) -> DaqDataGatewayServicer:
    """``edge_port=None`` (the default) omits the field entirely from the
    constructor call, so DaqDataGatewayConfig.model_fields_set does NOT
    contain "edge_port" -- simulating a TOML config that never set it
    (tier 2 doesn't apply). Pass an explicit int to simulate a TOML config
    that does set it.
    """
    gateway_kwargs: dict[str, Any] = {
        "daq_config_path": str(daq_cfg_path),
        "network_config_path": str(net_cfg_path) if net_cfg_path else None,
    }
    if edge_port is not None:
        gateway_kwargs["edge_port"] = edge_port
    gw_cfg = DaqDataServerConfig(role="gateway", gateway=DaqDataGatewayConfig(**gateway_kwargs))
    return DaqDataGatewayServicer(gw_cfg)


# ---------------------------------------------------------------------------
# No network_config.json entry for the node at all -- tiers 2-4 only.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_network_config_uses_explicit_toml_edge_port() -> None:
    """Tier 2: no network_config.json, but edge_port is explicitly set."""
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td), daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}], net_daq_nodes=None
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=60005)
        try:
            await servicer.startup()
            assert "192.168.0.10:60005" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_no_network_config_falls_back_to_edgenode_env_when_edge_port_not_explicit(
    monkeypatch: Any,
) -> None:
    """Tier 3: no network_config.json, edge_port not explicit in config."""
    import tempfile

    monkeypatch.setenv("EDGENODE_GRPC_PORT", "50088")

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td), daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}], net_daq_nodes=None
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path)
        try:
            await servicer.startup()
            assert "192.168.0.10:50088" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_no_network_config_falls_back_to_default_when_nothing_set() -> None:
    """Tier 4: no network_config.json, no explicit edge_port, no env var."""
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td), daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}], net_daq_nodes=None
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path)
        try:
            await servicer.startup()
            assert "192.168.0.10:50051" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


# ---------------------------------------------------------------------------
# network_config.json has an entry -- tier 1 (and its fallthrough to
# tiers 2-4 when the entry's own grpc_port is unset).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_direct_connect_uses_node_sibling_grpc_port_over_toml_edge_port() -> None:
    """Tier 1 beats tier 2: port_forwarding.status == False, sibling
    grpc_port set, even though an explicit TOML edge_port is also present.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td),
            daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}],
            net_daq_nodes=[
                {
                    "ip_addr": "192.168.0.10",
                    "grpc_port": 50077,
                    "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"},
                }
            ],
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=50051)
        try:
            await servicer.startup()
            assert "192.168.0.10:50077" in servicer._edge_stubs
            assert "192.168.0.10:50051" not in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_port_forwarding_status_true_uses_nested_grpc_port_over_toml_edge_port() -> None:
    """Tier 1 beats tier 2: port_forwarding.status == True with its own
    grpc_port set wins over an explicit TOML edge_port.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td),
            daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}],
            net_daq_nodes=[
                {
                    "ip_addr": "192.168.0.10",
                    "grpc_port": 50077,
                    "port_forwarding": {"status": True, "gw_ip": "10.0.1.254", "grpc_port": 50099},
                }
            ],
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=50051)
        try:
            await servicer.startup()
            assert "10.0.1.254:50099" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_port_forwarding_status_true_with_grpc_port_unset_still_switches_host() -> None:
    """port_forwarding.status == True but its own grpc_port is unset: the
    host still switches to the forwarded gateway IP (status alone means
    "route through this host"), while the port falls through to tier 2
    (explicit TOML edge_port here).
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td),
            daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}],
            net_daq_nodes=[
                {
                    "ip_addr": "192.168.0.10",
                    "port_forwarding": {"status": True, "gw_ip": "10.0.1.254"},
                }
            ],
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=60010)
        try:
            await servicer.startup()
            assert "10.0.1.254:60010" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_direct_connect_falls_through_to_toml_edge_port_when_sibling_grpc_port_unset() -> None:
    """Tier 2: network_config.json has an entry (status == False) but
    leaves the sibling grpc_port unset -- falls through to the explicit
    TOML edge_port, not straight to EDGENODE_GRPC_PORT/default.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td),
            daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}],
            net_daq_nodes=[
                {"ip_addr": "192.168.0.10", "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"}}
            ],
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=60020)
        try:
            await servicer.startup()
            assert "192.168.0.10:60020" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_direct_connect_falls_through_to_edgenode_env_when_sibling_and_toml_unset(
    monkeypatch: Any,
) -> None:
    """Tier 3: network_config.json entry exists with grpc_port unset, and
    edge_port is not explicit -- falls through to EDGENODE_GRPC_PORT.
    """
    import tempfile

    monkeypatch.setenv("EDGENODE_GRPC_PORT", "50088")

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td),
            daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}],
            net_daq_nodes=[
                {"ip_addr": "192.168.0.10", "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"}}
            ],
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path)
        try:
            await servicer.startup()
            assert "192.168.0.10:50088" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_direct_connect_falls_through_to_default_when_nothing_set() -> None:
    """Tier 4: network_config.json entry exists with grpc_port unset,
    edge_port not explicit, EDGENODE_GRPC_PORT not set -- 50051 default.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td),
            daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}],
            net_daq_nodes=[
                {"ip_addr": "192.168.0.10", "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"}}
            ],
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path)
        try:
            await servicer.startup()
            assert "192.168.0.10:50051" in servicer._edge_stubs
        finally:
            await servicer.shutdown()
