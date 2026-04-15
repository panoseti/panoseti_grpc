import asyncio
import multiprocessing
import os
import socket
import time
import uuid

import pytest
import redis

from panoseti_grpc.telemetry.client import TelemetryClient
from panoseti_grpc.telemetry.logger import PanosetiLogFactory
from panoseti_grpc.telemetry.server import serve

# ---------------------------------------------------------------------------
# Shared polling utilities — preferred over hardcoded time.sleep() calls
# because the RedisBatcher has a variable flush latency.
# ---------------------------------------------------------------------------


def poll_redis_key(redis_client, key, timeout=10.0, interval=0.1) -> bool:
    """Sync poll: return True once the Redis key exists, False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if redis_client.exists(key):
            return True
        time.sleep(interval)
    return False


def poll_redis_field(redis_client, key, field, expected=None, timeout=10.0, interval=0.1) -> bool:
    """
    Sync poll: return True once `redis_client.hget(key, field)` is not None.
    If `expected` is provided, also check that the value equals str(expected).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = redis_client.hget(key, field)
        if val is not None:
            if expected is None or val == str(expected):
                return True
        time.sleep(interval)
    return False


# Get Hosts from Env
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
SERVER_PORT = 50051
MAX_GRPC_SERVER_STARTUP_DELAY = 30

# to avoid distributed workers from polluting the test database
TEST_DB_INDEX = 1


@pytest.fixture(autouse=True)
def reset_log_factory():
    """
    Resets the singleton gRPC client cache before every test.
    This prevents state pollution where a client created in Test A
    is reused in Test B, breaking mocks.
    """
    PanosetiLogFactory.reset_clients()
    yield
    PanosetiLogFactory.reset_clients()


@pytest.fixture(scope="session")
def redis_connection():
    """Establishes the connection once per session."""
    r = redis.Redis(host=REDIS_HOST, port=6379, db=TEST_DB_INDEX, decode_responses=True)
    try:
        r.ping()
        # Ensure clean slate for the session
        # r.flushdb()
    except redis.ConnectionError:
        pytest.fail(f"Could not connect to Redis at {REDIS_HOST}")
    return r


@pytest.fixture(scope="session")
def redis_client(redis_connection):
    """
    Provides a clean Redis for EACH test function.
    """
    # redis_connection.flushdb()
    yield redis_connection
    # redis_connection.flushdb()


@pytest.fixture(scope="session", autouse=True)
def clean_redis(redis_connection):
    """Ensure a clean slate for the integration tests."""
    redis_connection.flushdb()
    yield


def _run_server_process(redis_host, port):
    # We need to tell the server to use the TEST DB
    # Since the server code might hardcode DB=0, we should pass it or mock it.
    # A cleaner way for integration tests without changing server code
    # is to set an ENV var that the server reads, or modify server.py to accept db.

    # Assuming we modify server.py (see step 2 below)
    asyncio.run(serve(redis_host=redis_host, port=port, redis_db=TEST_DB_INDEX))


@pytest.fixture(scope="session")
def start_grpc_server():
    proc = multiprocessing.Process(target=_run_server_process, args=(REDIS_HOST, SERVER_PORT), daemon=True)
    proc.start()

    # Wait for startup
    start_time = time.time()
    server_ready = False
    while time.time() - start_time < MAX_GRPC_SERVER_STARTUP_DELAY:
        if not proc.is_alive():
            raise RuntimeError("Server process died!")
        try:
            with socket.create_connection(("localhost", SERVER_PORT), timeout=0.1):
                server_ready = True
                break
        except (OSError, ConnectionRefusedError):
            time.sleep(0.1)

    if not server_ready:
        proc.terminate()
        raise TimeoutError("Server failed to bind port")

    yield

    proc.terminate()
    proc.join(timeout=2)
    if proc.is_alive():
        proc.kill()


@pytest.fixture(scope="function")
def grpc_client(start_grpc_server):
    return TelemetryClient(host="localhost", port=SERVER_PORT)


@pytest.fixture(scope="module")
def distributed_session(redis_client, start_grpc_server):
    """
    Manages a unique session ID for workers to synchronize on.
    Ensures a clean slate before and after the test run.
    """
    session_id = str(uuid.uuid4())
    print(f"🚀 STARTING Distributed Session: {session_id}")

    # Broadcast the session ID so all Docker workers start sending logs
    redis_client.set("DISTRIBUTED_SESSION_ID", session_id)

    yield session_id

    # Cleanup: Workers will stop when they see the key is gone
    redis_client.delete("DISTRIBUTED_SESSION_ID")
    print(f"🛑 ENDED Session: {session_id}")
