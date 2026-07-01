"""Unit tests for DaqConfig validation (now lives server-side in aggregator.py)."""

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_data.client_models import DaqConfig


def test_missing_daq_nodes_raises() -> None:
    with pytest.raises(ValidationError, match="daq_nodes"):
        DaqConfig.model_validate({"head_node_data_dir": "/tmp", "head_node_ip_addr": "127.0.0.1"})


def test_empty_daq_nodes_allowed() -> None:
    cfg = DaqConfig.model_validate({"head_node_data_dir": "/tmp", "head_node_ip_addr": "127.0.0.1", "daq_nodes": []})
    assert cfg.daq_nodes == []


def test_node_missing_ip_addr_raises() -> None:
    with pytest.raises(ValidationError, match="ip_addr"):
        DaqConfig.model_validate(
            {
                "head_node_data_dir": "/tmp",
                "head_node_ip_addr": "127.0.0.1",
                "daq_nodes": [{"username": "test", "data_dir": "/tmp", "module_ids": [1]}],
            }
        )


def test_minimal_valid_config() -> None:
    cfg = DaqConfig.model_validate(
        {"daq_nodes": [{"ip_addr": "192.168.0.10", "data_dir": "/data", "module_ids": [0, 1]}]}
    )
    assert len(cfg.daq_nodes) == 1
    assert str(cfg.daq_nodes[0].ip_addr) == "192.168.0.10"
