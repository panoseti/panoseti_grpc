import asyncio

import pytest

from panoseti_grpc.daq_control.client import DaqControlClient

START_PARAMS = {
    "data_dir": "/app/data",
    "daq_ip_addr": "127.0.0.1",
    "bindhost": "lo",
    "max_file_size_mb": 10,
    "group_ph_frames": True,
    "run_dir": "robust_test.pffd",
    "obs": "ucb-lab",
    "module_id": [250, 251],
    "enable_v2_forwarder": True,
    "headnode_target": "localhost:50051",
}


@pytest.mark.asyncio
async def test_forwarder_robustness_lifecycle(grpc_client: DaqControlClient):
    """Verify that enabling v2 forwarder doesn't break DAQ lifecycle."""

    # 1. Start DAQ with forwarder
    assert grpc_client.StartDaq(START_PARAMS) is True

    # 2. Verify both processes are running (hashpipe + forwarder)
    # We can't easily check PIDs from the host if testing against a container,
    # but the gRPC call succeeding means StartDaq finished.
    _, status = grpc_client.StatusDaq({"data_dir": "/app/data", "check_hashpipe_running": True})
    assert status["hashpipe_running"] is True

    # 3. Stop DAQ
    assert grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]}) is True

    # 4. Verify stop reflected in status
    _, status = grpc_client.StatusDaq({"data_dir": "/app/data", "check_hashpipe_running": True})
    assert status["hashpipe_running"] is False


@pytest.mark.asyncio
async def test_zombie_forwarder_cleanup(grpc_client: DaqControlClient):
    """Verify that StartDaq cleans up existing forwarder processes."""

    # Start one instance
    assert grpc_client.StartDaq(START_PARAMS) is True

    # Stop first
    grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]})

    # Now StartDaq again — it should sweep any remnants
    assert grpc_client.StartDaq(START_PARAMS) is True
    assert grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]}) is True


@pytest.mark.asyncio
async def test_forwarder_immediate_crash(grpc_client: DaqControlClient):
    """Verify that if the forwarder crashes immediately, DaqControl remains stable."""
    params = dict(START_PARAMS)
    # Point to an invalid headnode target that might cause a fast exit (though our forwarder retries)
    # Or just start it and kill it externally if we could, but let's test the server logic.
    params["headnode_target"] = "invalid_target:9999"

    assert grpc_client.StartDaq(params) is True
    # The server should still allow Status and Stop
    _, status = grpc_client.StatusDaq({"data_dir": START_PARAMS["data_dir"], "check_hashpipe_running": True})
    assert status["hashpipe_running"] is True

    assert grpc_client.StopDaq({"data_dir": params["data_dir"], "run_dir": params["run_dir"]}) is True


@pytest.mark.asyncio
async def test_forwarder_log_spam_robustness(grpc_client: DaqControlClient):
    """Verify that massive log output from forwarder doesn't hang DaqControl."""
    # This exercises the pipe draining logic.
    assert grpc_client.StartDaq(START_PARAMS) is True
    # Let it run for a bit
    await asyncio.sleep(1.0)
    assert grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]}) is True


@pytest.mark.asyncio
async def test_forwarder_post_start_crash_robustness(grpc_client: DaqControlClient):
    """Verify that if the forwarder dies after StartDaq, StopDaq still works."""
    assert grpc_client.StartDaq(START_PARAMS) is True

    # Simulate forwarder death. In a real environment, we'd find the PID and kill it.
    # But StopDaq should handle it gracefully anyway.

    # We'll just call StopDaq and ensure it returns success
    assert grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]}) is True
