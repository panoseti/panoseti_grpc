import pytest
import asyncio
import json
import logging
from pathlib import Path
import threading

from daq_data.server import serve
from daq_data.client import AioDaqDataClient

TEST_CFG_DIR = Path("tests/config")


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def server_config():
    """Load the server configuration for testing."""
    config_path = TEST_CFG_DIR / "daq_data_server_config.json"
    with open(config_path, "r") as f:
        config = json.load(f)
    # Use a unique socket for testing to avoid conflicts
    config["unix_domain_socket"] = "unix:///tmp/test_daq_data.sock"
    return config


@pytest.fixture(scope="session")
async def server_and_client(server_config):
    """
    Starts the gRPC server in a background thread and yields an async client.
    This fixture has a session scope, so the server runs once for all tests.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Event to signal when the server is ready to accept connections
    server_ready_event = asyncio.Event()
    # Event to signal the server to shut down
    shutdown_event = asyncio.Event()

    async def run_server_with_event():
        # This is a modified serve function that signals when it's ready
        server_task = asyncio.create_task(serve(server_config, shutdown_event))
        await asyncio.sleep(1)  # Give it a moment to start
        server_ready_event.set()
        await server_task

    server_thread = threading.Thread(
        target=lambda: asyncio.run(run_server_with_event()),
        daemon=True
    )
    server_thread.start()

    # Wait for the server to be ready
    await server_ready_event.wait()
    logger.info("Test server is running in background thread.")

    # Setup the client
    daq_config = {"daq_nodes": [{"ip_addr": server_config["unix_domain_socket"]}]}
    client = AioDaqDataClient(daq_config, network_config=None)

    async with client as active_client:
        yield active_client  # Provide the client to the tests

    # Teardown: stop the server
    logger.info("Tests finished, shutting down server.")
    shutdown_event.set()
    server_thread.join(timeout=5)
    logger.info("Test server shut down.")

