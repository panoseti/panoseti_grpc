"""Unit tests for DaqDataGatewayServicer.startup()'s per-node host/port
resolution -- previously entirely untested. gRPC channels are lazy
(grpc.aio.insecure_channel doesn't connect until an RPC is issued), so
startup() can be exercised against config files pointing at nodes with
nothing actually listening, and asserted against purely by inspecting the
resulting edge targets.
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
    field is left None -- clear them so a stray dev/.env value can't leak a
    real config file into these tests.
    """
    monkeypatch.delenv("PSETI_GRPC_DAQ_CONFIG", raising=False)
    monkeypatch.delenv("PSETI_GRPC_NETWORK_CONFIG", raising=False)


def _write_configs(tmp_path: Path, daq_nodes: list[dict], net_daq_nodes: list[dict] | None) -> tuple[Path, Path | None]:
    daq_cfg_path = tmp_path / "daq_config.json"
    daq_cfg_path.write_text(json.dumps({"daq_nodes": daq_nodes}))

    net_cfg_path = None
    if net_daq_nodes is not None:
        net_cfg_path = tmp_path / "network_config.json"
        net_cfg_path.write_text(json.dumps({"daq_nodes": net_daq_nodes}))

    return daq_cfg_path, net_cfg_path


def _make_servicer(daq_cfg_path: Path, net_cfg_path: Path | None, edge_port: int = 50051) -> DaqDataGatewayServicer:
    gw_cfg = DaqDataServerConfig(
        role="gateway",
        gateway=DaqDataGatewayConfig(
            daq_config_path=str(daq_cfg_path),
            network_config_path=str(net_cfg_path) if net_cfg_path else None,
            edge_port=edge_port,
        ),
    )
    return DaqDataGatewayServicer(gw_cfg)


@pytest.mark.asyncio
async def test_no_network_config_falls_back_to_gateway_edge_port() -> None:
    """No network_config.json at all -- every node uses the gateway-wide edge_port."""
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        daq_cfg_path, net_cfg_path = _write_configs(
            Path(td), daq_nodes=[{"ip_addr": "192.168.0.10", "data_dir": "/tmp", "username": "test"}], net_daq_nodes=None
        )
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=50051)
        try:
            await servicer.startup()
            assert "192.168.0.10:50051" in servicer._edge_stubs
        finally:
            await servicer.shutdown()


@pytest.mark.asyncio
async def test_direct_connect_uses_node_sibling_grpc_port_over_gateway_edge_port() -> None:
    """port_forwarding.status == False -- the node's own sibling grpc_port
    (network_config.json) must win over the gateway-wide edge_port default.
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
async def test_port_forwarding_status_true_still_wins_over_sibling_grpc_port() -> None:
    """port_forwarding.status == True must still take precedence over the
    new sibling grpc_port -- the sibling field is only a direct-connect
    fallback, not a higher-priority override.
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
async def test_direct_connect_defaults_to_50051_when_sibling_grpc_port_omitted() -> None:
    """A network_config.json entry that omits the new sibling grpc_port
    entirely must still resolve to the field's own default (50051), not
    silently error or fall through to something else.
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
        servicer = _make_servicer(daq_cfg_path, net_cfg_path, edge_port=59999)
        try:
            await servicer.startup()
            assert "192.168.0.10:50051" in servicer._edge_stubs
        finally:
            await servicer.shutdown()
