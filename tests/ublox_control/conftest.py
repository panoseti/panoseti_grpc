# tests/conftest.py

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
TEST_DATA_DIR = Path("tests/data")

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
    # Instantiate the servicer directly for manipulation
    servicer = UbloxControlServicer(server_config, logger)
    yield servicer
    # Cleanup, if any, can go here
    await servicer.stop()


@pytest_asyncio.fixture(scope="function")
async def live_server(server_config):
    """
    Starts the full gRPC server in a background task for integration tests.
    """
    host = "localhost"
    port = 50051
    address = f"{host}:{port}"

    shutdown_event = asyncio.Event()
    server_task = asyncio.create_task(serve(shutdown_event, in_main_thread=False))

    # Wait for the server to be ready
    for _ in range(30):  # 3-second timeout
        try:
            async with grpc.aio.insecure_channel(address) as channel:
                stub = ublox_control_pb2_grpc.UbloxControlStub(channel)
                # A simple health check could be a dummy RPC call or just channel readiness
                await channel.channel_ready()
            break
        except (grpc.aio.AioRpcError, asyncio.exceptions.TimeoutError):
            await asyncio.sleep(0.1)
    else:
        pytest.fail("Server did not become ready in time.")

    yield {"address": address, "host": host, "port": port}

    # Teardown: gracefully shut down the server
    shutdown_event.set()
    try:
        await asyncio.wait_for(server_task, timeout=5.0)
    except asyncio.TimeoutError:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)

