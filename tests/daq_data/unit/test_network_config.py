"""Unit tests for NetworkConfig / NetworkDaqNode / NetworkHeadnode (network_config.json)."""

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_data.client_models import NetworkConfig, NetworkDaqNode, NetworkHeadnode, PortForwarding


def test_port_forwarding_grpc_port_defaults_to_none_when_omitted() -> None:
    """None (not a 50051 literal) so DaqDataGatewayServicer.startup() can
    tell "port_forwarding.status == True but grpc_port not set" apart from
    "explicitly set" -- the former still switches host to gw_ip but falls
    through to [daq_data.gateway].edge_port / EDGENODE_GRPC_PORT / 50051
    for the port itself.
    """
    pf = PortForwarding.model_validate({"status": True, "gw_ip": "10.0.1.254"})
    assert pf.grpc_port is None


def test_port_forwarding_grpc_port_explicit_value() -> None:
    pf = PortForwarding.model_validate({"status": True, "gw_ip": "10.0.1.254", "grpc_port": 50099})
    assert pf.grpc_port == 50099


def test_headnode_defaults_to_50051_when_omitted() -> None:
    cfg = NetworkConfig.model_validate({})
    assert cfg.headnode.grpc_port == 50051


def test_headnode_grpc_port_explicit_value() -> None:
    cfg = NetworkConfig.model_validate({"headnode": {"grpc_port": 60051}})
    assert cfg.headnode.grpc_port == 60051


def test_headnode_grpc_port_out_of_range_raises() -> None:
    with pytest.raises(ValidationError):
        NetworkHeadnode.model_validate({"grpc_port": 70000})


def test_daq_node_grpc_port_defaults_to_none_when_omitted() -> None:
    """None (not a 50051 literal) so DaqDataGatewayServicer.startup() can
    tell "not set in network_config.json" apart from "explicitly set" and
    fall through to EDGENODE_GRPC_PORT / the 50051 default.
    """
    node = NetworkDaqNode.model_validate(
        {"ip_addr": "192.168.0.10", "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"}}
    )
    assert node.grpc_port is None


def test_daq_node_grpc_port_explicit_value() -> None:
    node = NetworkDaqNode.model_validate(
        {
            "ip_addr": "192.168.0.10",
            "grpc_port": 50077,
            "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"},
        }
    )
    assert node.grpc_port == 50077


def test_daq_node_grpc_port_out_of_range_raises() -> None:
    with pytest.raises(ValidationError):
        NetworkDaqNode.model_validate(
            {"ip_addr": "192.168.0.10", "grpc_port": 0, "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"}}
        )


def test_daq_node_grpc_port_is_independent_of_port_forwarding_grpc_port() -> None:
    """The two grpc_port fields are distinct: the sibling one is for direct
    connections (port_forwarding.status == False), the nested one is only
    consulted when port_forwarding.status == True. Setting one must not
    affect the other.
    """
    node = NetworkDaqNode.model_validate(
        {
            "ip_addr": "192.168.0.10",
            "grpc_port": 50077,
            "port_forwarding": {"status": True, "gw_ip": "10.0.1.254", "grpc_port": 50099},
        }
    )
    assert node.grpc_port == 50077
    assert node.port_forwarding.grpc_port == 50099
