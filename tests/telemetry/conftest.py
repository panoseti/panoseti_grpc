import pytest
import redis
import time
import os
import multiprocessing
import socket
from influxdb import InfluxDBClient
from panoseti_grpc.telemetry.client import TelemetryClient
# Import the serve function
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


def _run_server_process(redis_host, grpc_port):
    """
    Runs the server in a separate process.
    Updated to match the new server.py signature.
    """
    import asyncio
    asyncio.run(serve(redis_host=redis_host, grpc_port=grpc_port))


@pytest.fixture(scope="session")
def start_grpc_server():
    """
    Starts the gRPC server in a separate multiprocessing.Process.
    """
    # 1. Start Server Process
    proc = multiprocessing.Process(
        target=_run_server_process,
        args=(REDIS_HOST, SERVER_PORT),
        daemon=True
    )
    proc.start()

    # 2. Wait for Port to Open (Health Check)
    start_time = time.time()
    server_ready = False
    while time.time() - start_time < 10:
        if not proc.is_alive():
            # If it dies, it likely printed the error to stderr already
            raise RuntimeError("gRPC Server process died immediately! Check container logs.")

        try:
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
    return TelemetryClient(host="localhost", port=SERVER_PORT)