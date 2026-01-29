# tests/conftest.py

import sys
import pytest
import pytest_asyncio
import asyncio
import json
import logging
import grpc
from pathlib import Path
from ublox_control.server import UbloxControlServicer, serve
from ublox_control.resources import make_rich_logger
from ublox_control import ublox_control_pb2_grpc

TEST_CFG_DIR = Path("tests/config")
TEST_CFG_DIR.mkdir(exist_ok=True, parents=True)
TEST_DATA_DIR = Path("tests/ublox_control/test_data")

@pytest.fixture(scope="session")
def ubx_packets_data_path():
    return TEST_DATA_DIR / "ubx_packets.jsonl"

# Create a dummy server config for testing
@pytest.fixture(scope="session")
def server_config():
    cfg = {"max_workers": 5, "max_read_queue_size": 200, "shutdown_grace_period": 0.1}
    config_path = TEST_CFG_DIR / "ublox_control_server_config.json"
    with open(config_path, "w") as f:
        json.dump(cfg, f)
    return cfg


@pytest_asyncio.fixture(scope="function")
async def sim_servicer(server_config):
    """Provides a UbloxControlServicer instance for simulation without starting a server."""
    logger = make_rich_logger("SimTestLogger", level=logging.DEBUG)
    servicer = UbloxControlServicer(server_config, logger)
    yield servicer
    await servicer.stop()


@pytest_asyncio.fixture(scope="function")
async def live_server(server_config):
    """
    Starts the full gRPC server in a separate subprocess for integration tests.
    This provides better isolation and stability than running in a background task.
    """
    host = "localhost"
    port = 50051
    address = f"{host}:{port}"

    # Command to run the server module in a new Python process
    cmd = [sys.executable, "-m", "ublox_control.server"]

    # Start the server as a subprocess
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Wait for the server to become ready by polling the gRPC port
    try:
        for _ in range(40):  # 4-second timeout
            try:
                async with grpc.aio.insecure_channel(address) as channel:
                    await channel.channel_ready()
                # Server is up, break the loop
                break
            except (grpc.aio.AioRpcError, asyncio.exceptions.TimeoutError):
                await asyncio.sleep(0.1)
        else:
            # If the loop completes without breaking, the server failed to start
            pytest.fail("The gRPC server did not become ready in time.")

        # Yield the address to the tests
        yield {"address": address, "host": host, "port": port}

    finally:
        # Teardown: Terminate the server process
        if proc.returncode is None:
            proc.terminate()
            await proc.wait()

        # Optional: Log any output from the server process for debugging
        stdout, stderr = await proc.communicate()
        if stdout:
            print(f"--- Server STDOUT ---\n{stdout.decode()}\n--------------------")
        if stderr:
            print(f"--- Server STDERR ---\n{stderr.decode()}\n--------------------")


