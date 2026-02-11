import pytest
import time
import psutil
    
def test_start_daq(grpc_client):
    """
    Verify startdaq works.
    """
    p = {
        "data_dir": "/app/data",
        "daq_ip_addr": "127.0.0.1",
        "bindhost": "lo",
        "max_file_size_mb": 10,
        "group_ph_frames": True,
        "run_dir": "test.pffd",
        "obs": "ucb-lab",
        "module_id": [
            250,
            251
        ]
    }
    assert grpc_client.StartDaq(p) == True
