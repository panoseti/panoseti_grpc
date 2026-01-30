import pytest
import pytest_asyncio
import redis
import time
import asyncio
from influxdb import InfluxDBClient
from panoseti_grpc.telemetry.server import serve
from panoseti_grpc.telemetry.client import TelemetryClient
import threading
import os

# Get Hosts from Env (set by Docker Compose)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")

@pytest.fixture(scope="session")
def redis_client():
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    yield r
    r.flushall()

@pytest.fixture(scope="session")
def influx_client():
    # Retry logic for InfluxDB startup
    for _ in range(10):
        try:
            client = InfluxDBClient(host=INFLUX_HOST, port=8086, username='root', password='root', database='metadata')
            client.create_database('metadata')
            return client
        except:
            time.sleep(1)
    raise ConnectionError("Could not connect to InfluxDB")


@pytest_asyncio.fixture(scope="session")
async def start_grpc_server():
    task = asyncio.create_task(serve("telemetry_config.toml", redis_host=REDIS_HOST))

    # Robust wait: Ping the port until it is open
    import socket
    start_time = time.time()
    while True:
        try:
            with socket.create_connection(("localhost", 50051), timeout=0.1):
                break
        except (OSError, ConnectionRefusedError):
            if time.time() - start_time > 5:
                raise TimeoutError("gRPC server failed to start within 5 seconds")
            await asyncio.sleep(0.1)

    yield
    task.cancel()

@pytest.fixture(scope="session")
def grpc_client(start_grpc_server):
    return TelemetryClient(host="localhost", port=50051)