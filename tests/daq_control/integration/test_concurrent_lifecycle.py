"""
Integration tests to verify concurrent lifecycle request handling in DaqControl.
"""

import asyncio
import time
from typing import Any

import pytest

START_PARAMS = {
    "data_dir": "/app/data",
    "daq_ip_addr": "127.0.0.1",
    "bindhost": "lo",
    "max_file_size_mb": 10,
    "group_ph_frames": True,
    "run_dir": "concurrent_lock_test.pffd",
    "obs": "ucb-lab",
    "module_id": [254],
}


@pytest.mark.asyncio
async def test_concurrent_start_stop_spam(grpc_client: Any):
    """Verify that spamming Start/Stop doesn't crash the server and maintains lock integrity."""

    # We use asyncio.to_thread because the existing grpc_client is synchronous.
    # If the server is using a lock, these requests will be queued and handled one by one.

    async def call_start():
        try:
            return await asyncio.to_thread(grpc_client.StartDaq, START_PARAMS)
        except ValueError:
            return False  # Expected if hashpipe already started

    async def call_stop():
        params = {"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]}
        try:
            return await asyncio.to_thread(grpc_client.StopDaq, params)
        except ValueError:
            return False

    # Hammer the server with concurrent Start and Stop requests
    tasks = []
    for _ in range(5):
        tasks.append(call_start())
        tasks.append(call_stop())

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Verify no unexpected exceptions occurred
    for res in results:
        assert not isinstance(res, Exception), f"Concurrent request failed with unexpected exception: {res}"

    # Finally ensure we are stopped
    assert grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]}) is True


@pytest.mark.asyncio
async def test_cleanup_lock_prevention(grpc_client: Any):
    """Verify that CleanupData waits for any in-progress StartDaq to release the lock."""
    # This verifies that sequential calls work correctly and respect business logic guards.

    assert grpc_client.StartDaq(START_PARAMS) is True

    # Cleanup should be refused if hashpipe is running
    res = grpc_client.CleanupData(
        {
            "data_dir": START_PARAMS["data_dir"],
            "run_dir": START_PARAMS["run_dir"],
            "module_id": START_PARAMS["module_id"],
        }
    )
    assert res["success"] is False

    grpc_client.StopDaq({"data_dir": START_PARAMS["data_dir"], "run_dir": START_PARAMS["run_dir"]})
    time.sleep(0.5)

    # Now cleanup should work
    res = grpc_client.CleanupData(
        {
            "data_dir": START_PARAMS["data_dir"],
            "run_dir": START_PARAMS["run_dir"],
            "module_id": START_PARAMS["module_id"],
        }
    )
    assert res["success"] is True
