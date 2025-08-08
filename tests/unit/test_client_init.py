import pytest
from pathlib import Path
from daq_data.client import DaqDataClient, AioDaqDataClient

def test_client_init_with_invalid_config_path():
    """Test that client initialization fails if the config path does not exist."""
    with pytest.raises(FileNotFoundError):
        DaqDataClient("nonexistent/path/to/config.json", None)

def test_client_init_with_malformed_config():
    """Test that client init fails if the daq_config dict is missing required keys."""
    # Missing 'daq_nodes' key entirely
    with pytest.raises(ValueError, match="daq_nodes is empty"):
        DaqDataClient({}, None)

    # 'daq_nodes' is empty
    with pytest.raises(ValueError, match="daq_nodes is empty"):
        DaqDataClient({"daq_nodes": []}, None)

    # A node is missing its 'ip_addr'
    with pytest.raises(ValueError, match="does not have an 'ip_addr' key"):
        DaqDataClient({"daq_nodes": [{"user_name": "test"}]}, None)

