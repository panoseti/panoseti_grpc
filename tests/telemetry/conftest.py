import pytest
import redis
import time
import os
import multiprocessing
import socket
from influxdb import InfluxDBClient
from panoseti_grpc.telemetry.client import TelemetryClient
# We import the serve function, but we will run it in a wrapper
from panoseti_grpc.telemetry.server import serve

# Get Hosts from Env (set by Docker Compose)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
SERVER_PORT = 50051


@pytest.fixture(scope="session")
def redis_client():
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.fail(f"Could not connect to Redis at {REDIS_HOST}")
    yield r
    r.flushall()


@pytest.fixture(scope="session")
def influx_client():
    # Retry logic for InfluxDB startup
    client = None
    for _ in range(10):
        try:
            client = InfluxDBClient(host=INFLUX_HOST, port=8086, username='root', password='root', database='metadata')
            client.create_database('metadata')
            break
        except Exception:
            time.sleep(1)

    if not client:
        pytest.fail("Could not connect to InfluxDB")

    return client


# --- HELPER: Wrapper to run async server in a separate process ---
def _run_server_process(config_path, redis_host, port):
    """
    This runs in a completely separate OS process.
    It creates its OWN asyncio loop, separate from pytest.
    """
    import asyncio
    # Run the server
    asyncio.run(serve(config_path, redis_host=redis_host, port=port))


@pytest.fixture(scope="session")
def start_grpc_server():
    """
    Starts the gRPC server in a separate multiprocessing.Process.
    This prevents the server from blocking the test runner's event loop.
    """
    config_path = "telemetry_config.toml"

    # 1. Start Server Process
    proc = multiprocessing.Process(
        target=_run_server_process,
        args=(config_path, REDIS_HOST, SERVER_PORT),
        daemon=True  # Ensures process dies if main test process dies
    )
    proc.start()

    # 2. Wait for Port to Open (Health Check)
    start_time = time.time()
    server_ready = False
    while time.time() - start_time < 10:
        if not proc.is_alive():
            raise RuntimeError("gRPC Server process died immediately! Check config loading.")

        try:
            # Try to connect to the TCP port
            with socket.create_connection(("localhost", SERVER_PORT), timeout=0.1):
                server_ready = True
                break
        except (OSError, ConnectionRefusedError):
            time.sleep(0.1)

    if not server_ready:
        proc.terminate()
        raise TimeoutError(f"gRPC server failed to bind port {SERVER_PORT} within 10 seconds")

    yield

    # 3. Cleanup
    proc.terminate()
    proc.join(timeout=2)
    if proc.is_alive():
        proc.kill()


@pytest.fixture(scope="session")
def grpc_client(start_grpc_server):
    # This client is strictly synchronous, which is easier for testing
    return TelemetryClient(host="localhost", port=SERVER_PORT)