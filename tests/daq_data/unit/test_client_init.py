import pytest

from panoseti_grpc.daq_data.client import DaqDataClient


def test_client_init_with_invalid_config_path() -> None:
    """Test that client initialization fails if the config path does not exist."""
    with pytest.raises(FileNotFoundError):
        DaqDataClient("nonexistent/path/to/config.json", None)


def test_client_init_with_malformed_config() -> None:
    """Test that client init fails if the daq_config dict is missing required keys."""
    # Missing 'daq_nodes' key entirely
    with pytest.raises(ValueError, match="daq_nodes"):
        DaqDataClient({}, None)

    # 'daq_nodes' is empty - now allowed for simulations/testing
    DaqDataClient(
        {
            "head_node_data_dir": "/tmp",
            "head_node_ip_addr": "127.0.0.1",
            "daq_nodes": [],
        },
        None,
    )

    # A node is missing its 'ip_addr'
    with pytest.raises(ValueError, match="ip_addr"):
        DaqDataClient(
            {
                "head_node_data_dir": "/tmp",
                "head_node_ip_addr": "127.0.0.1",
                "daq_nodes": [{"username": "test", "data_dir": "/tmp", "module_ids": [1]}],
            },
            None,
        )
