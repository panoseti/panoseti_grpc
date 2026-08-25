"""Unit tests for NetworkConfig / NetworkDaqNode / NetworkHeadnode (network_config.json)."""

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_data.client_models import NetworkConfig, NetworkDaqNode, NetworkHeadnode


def test_headnode_defaults_to_50051_when_omitted() -> None:
    cfg = NetworkConfig.model_validate({})
    assert cfg.headnode.grpc_port == 50051


def test_headnode_grpc_port_explicit_value() -> None:
    cfg = NetworkConfig.model_validate({"headnode": {"grpc_port": 60051}})
    assert cfg.headnode.grpc_port == 60051


def test_headnode_grpc_port_out_of_range_raises() -> None:
    with pytest.raises(ValidationError):
        NetworkHeadnode.model_validate({"grpc_port": 70000})


def test_daq_node_grpc_port_defaults_to_50051_when_omitted() -> None:
    node = NetworkDaqNode.model_validate(
        {"ip_addr": "192.168.0.10", "port_forwarding": {"status": False, "gw_ip": "10.0.1.254"}}
    )
    assert node.grpc_port == 50051


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
