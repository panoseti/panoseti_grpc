"""
Shared fixtures for unified server integration tests.

Architecture
------------
All integration tests use session-scoped servers started in a
multiprocessing.Process (same pattern as tests/telemetry/conftest.py and
tests/daq_control/conftest.py).  Each server uses a dynamically generated
TOML config written to a tmp_path, ensuring UDS socket paths and ports are
unique and isolated.

Three server fixtures are provided:

- ``start_unified_server``: all services (telemetry + daq_data sim + daq_control).
  Uses GRPC_PORT (default 50055) and requires Redis.

- ``start_headnode_server``: telemetry only.
  Uses HEADNODE_PORT (default 50056) and requires Redis.

- ``start_daq_node_server``: daq_data + daq_control (no telemetry).
  Uses DAQ_NODE_PORT (default 50057), no Redis needed.

Ports are intentionally different from 50051 to avoid conflicts with other
running services or test suites.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import socket
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import redis as redis_sync

from panoseti_grpc.server import PanosetiServer, PanosetiServerConfig
from panoseti_grpc.telemetry.logger import PanosetiLogFactory

# ---------------------------------------------------------------------------
# Port / host constants (overridable via env vars in Docker)
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TEST_DB: int = 2  # Isolated DB index to avoid clobbering other tests

GRPC_PORT: int = int(os.getenv("GRPC_PORT", "50055"))  # All-services server
HEADNODE_PORT: int = int(os.getenv("HEADNODE_PORT", "50056"))  # Telemetry-only server
DAQ_NODE_PORT: int = int(os.getenv("DAQ_NODE_PORT", "50057"))  # daq_data + daq_control

SERVER_STARTUP_TIMEOUT: float = 30.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_port(host: str, port: int, timeout: float = SERVER_STARTUP_TIMEOUT) -> bool:
    """Poll until a TCP port accepts connections or timeout expires.

    Returns True on success, False on timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def poll_redis_list_len(r: Any, key: str, min_len: int, timeout: float = 10.0, interval: float = 0.1) -> bool:
    """Block until a Redis list has at least ``min_len`` entries, or timeout.

    Returns True if condition was met, False on timeout.
    Prefer this over time.sleep() to account for RedisBatcher flush latency.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if r.llen(key) >= min_len:
            return True
        time.sleep(interval)
    return False


def poll_redis_key(r: Any, key: str, timeout: float = 10.0, interval: float = 0.1) -> bool:
    """Block until a Redis key exists, or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if r.exists(key):
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Server subprocess entry point
# ---------------------------------------------------------------------------


def _run_unified_server(toml_path: str) -> Any:
    """Target function for server subprocess.

    Loads PanosetiServerConfig from the given TOML path and runs the unified
    server until the process is terminated.
    """
    cfg = PanosetiServerConfig.from_toml(toml_path)
    asyncio.run(PanosetiServer.run(cfg))


def _start_server_process(toml_path: str, port: int) -> multiprocessing.Process:
    """Spawn a server subprocess and wait for its port to be ready."""
    proc = multiprocessing.Process(
        target=_run_unified_server,
        args=(str(toml_path),),
        daemon=True,
    )
    proc.start()
    assert wait_for_port("localhost", port), (
        f"Unified server on port {port} did not start within {SERVER_STARTUP_TIMEOUT}s"
    )
    return proc


def _stop_server_process(proc: multiprocessing.Process, timeout: float = 5.0) -> Any:
    """Terminate a server subprocess, escalating to SIGKILL if needed."""
    proc.terminate()
    proc.join(timeout=timeout)
    if proc.is_alive():
        proc.kill()
        proc.join(timeout=2.0)


# ---------------------------------------------------------------------------
# TOML config builders
# ---------------------------------------------------------------------------


def _build_unified_toml(
    toml_path: Path, socket_dir: Path, redis_host: str, redis_port: int, redis_db: int, port: int
) -> Any:
    """Write a complete all-services TOML suitable for simulation testing."""
    socket_template = str(socket_dir / "hashpipe_grpc.dp_{dp_name}.sock")
    content = f"""
[server]
port = {port}
shutdown_grace_period = 3.0
log_dir = "/tmp"
grpc_logging = false

[server.services]
telemetry   = true
daq_data    = true
daq_control = true

[telemetry]
redis_host = "{redis_host}"
redis_port = {redis_port}
redis_db   = {redis_db}
shutdown_grace_period = 3.0

[daq_data]
init_from_default       = false
max_concurrent_rpcs     = 10
max_read_queue_size     = 50
min_hp_io_update_interval_seconds = 0.01
max_client_update_interval_seconds = 60.0
max_reader_enqueue_timeouts = 5
max_reader_dequeue_timeouts = 5
reader_timeout          = 15.0
shutdown_grace_period   = 3.0
hp_io_stop_timeout      = 5.0
valid_data_products     = ["img8", "img16", "ph256", "ph1024"]
grpc_logging            = false

[daq_data.acquisition_methods.uds]
enabled              = true
data_products        = ["img8", "img16", "ph256", "ph1024"]
socket_path_template = "{socket_template}"
read_timeout         = 30.0

[daq_data.simulate_daq_cfg]
simulation_mode  = "uds"
sim_module_ids   = [225]
movie_type       = "img16"
ph_type          = "ph256"

[daq_data.simulate_daq_cfg.source_data]
real_module_id = 1
movie_pff_path = "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.debug_TRUNCATED.pff"
ph_pff_path    = "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04_34_46Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_ph256.bpp_2.module_3.seqno_0.debug_TRUNCATED.pff"

[daq_data.simulate_daq_cfg.strategies.uds]
data_products = ["ph256", "img16"]

[daq_control]
grpc_logging      = false
shutdown_grace_period = 3.0
log_level         = "INFO"
"""
    toml_path.write_text(content)


def _build_headnode_toml(toml_path: Path, redis_host: str, redis_port: int, redis_db: int, port: int) -> Any:
    """Write a headnode-profile (telemetry only) TOML."""
    content = f"""
[server]
port = {port}
shutdown_grace_period = 3.0

[server.services]
telemetry   = true
daq_data    = false
daq_control = false

[telemetry]
redis_host = "{redis_host}"
redis_port = {redis_port}
redis_db   = {redis_db}
shutdown_grace_period = 3.0
"""
    toml_path.write_text(content)


def _build_daq_node_toml(toml_path: Path, socket_dir: Path, port: int) -> Any:
    """Write a daq_node-profile (daq_data + daq_control, no telemetry) TOML."""
    socket_template = str(socket_dir / "daq_node_grpc.dp_{dp_name}.sock")
    content = f"""
[server]
port = {port}
shutdown_grace_period = 3.0
grpc_logging = false

[server.services]
telemetry   = false
daq_data    = true
daq_control = true

[daq_data]
init_from_default       = false
max_concurrent_rpcs     = 10
max_read_queue_size     = 50
min_hp_io_update_interval_seconds = 0.01
max_client_update_interval_seconds = 60.0
max_reader_enqueue_timeouts = 5
max_reader_dequeue_timeouts = 5
reader_timeout          = 15.0
shutdown_grace_period   = 3.0
hp_io_stop_timeout      = 5.0
valid_data_products     = ["img8", "img16", "ph256", "ph1024"]
grpc_logging            = false

[daq_data.acquisition_methods.uds]
enabled              = true
data_products        = ["img8", "img16", "ph256", "ph1024"]
socket_path_template = "{socket_template}"
read_timeout         = 30.0

[daq_data.simulate_daq_cfg]
simulation_mode  = "uds"
sim_module_ids   = [226]
movie_type       = "img16"
ph_type          = "ph256"

[daq_data.simulate_daq_cfg.source_data]
real_module_id = 1
movie_pff_path = "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.debug_TRUNCATED.pff"
ph_pff_path    = "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04_34_46Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_ph256.bpp_2.module_3.seqno_0.debug_TRUNCATED.pff"

[daq_data.simulate_daq_cfg.strategies.uds]
data_products = ["ph256", "img16"]

[daq_control]
grpc_logging      = false
shutdown_grace_period = 3.0
log_level         = "INFO"
"""
    toml_path.write_text(content)


# ---------------------------------------------------------------------------
# Session fixtures: temp directories
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def unified_socket_dir(tmp_path_factory: Any) -> Any:
    """Temporary directory for UDS sockets used by the all-services server."""
    return tmp_path_factory.mktemp("unified_socks")


@pytest.fixture(scope="session")
def daq_node_socket_dir(tmp_path_factory: Any) -> Any:
    """Temporary directory for UDS sockets used by the daq_node server."""
    return tmp_path_factory.mktemp("daq_node_socks")


# ---------------------------------------------------------------------------
# Session fixtures: server TOML files
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def unified_server_toml(tmp_path_factory: Any, unified_socket_dir: Any) -> Any:
    """Write and return the TOML path for the all-services unified server."""
    toml_path = tmp_path_factory.mktemp("unified_cfg") / "server.toml"
    _build_unified_toml(
        toml_path,
        socket_dir=unified_socket_dir,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_db=REDIS_TEST_DB,
        port=GRPC_PORT,
    )
    return toml_path


@pytest.fixture(scope="session")
def headnode_server_toml(tmp_path_factory: Any) -> Any:
    """Write and return the TOML path for the headnode (telemetry-only) server."""
    toml_path = tmp_path_factory.mktemp("headnode_cfg") / "server.toml"
    _build_headnode_toml(
        toml_path,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_db=REDIS_TEST_DB,
        port=HEADNODE_PORT,
    )
    return toml_path


@pytest.fixture(scope="session")
def daq_node_server_toml(tmp_path_factory: Any, daq_node_socket_dir: Any) -> Any:
    """Write and return the TOML path for the daq_node (no telemetry) server."""
    toml_path = tmp_path_factory.mktemp("daq_node_cfg") / "server.toml"
    _build_daq_node_toml(
        toml_path,
        socket_dir=daq_node_socket_dir,
        port=DAQ_NODE_PORT,
    )
    return toml_path


# ---------------------------------------------------------------------------
# Session fixtures: running server processes
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _require_redis() -> Any:
    """Session-scoped guard: fail fast if Redis is unavailable.

    Server fixtures that start a telemetry-enabled server depend on this so
    that the subprocess never times out waiting for a port that will never
    open (because telemetry fails to connect to Redis).

    In CI the docker-compose always provides a Redis sidecar, so this check
    always passes.  Locally, start Redis before running integration tests:
        redis-server --daemonize yes
    """
    r = redis_sync.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_TEST_DB)
    try:
        r.ping()
    except redis_sync.ConnectionError:
        pytest.fail(
            f"Redis is not available at {REDIS_HOST}:{REDIS_PORT}. "
            "The unified server telemetry integration tests require Redis. "
            "Start Redis locally (redis-server) or run via Docker Compose."
        )
    finally:
        r.close()


@pytest.fixture(scope="session")
def start_unified_server(unified_server_toml: Any, _require_redis: Any) -> Any:
    """Start the all-services unified server; stop it after the session."""
    proc = _start_server_process(str(unified_server_toml), GRPC_PORT)
    # Extra wait for telemetry → Redis connection and daq_data UDS sockets
    time.sleep(1.5)
    yield proc
    _stop_server_process(proc)


@pytest.fixture(scope="session")
def start_headnode_server(headnode_server_toml: Any, _require_redis: Any) -> Any:
    """Start the headnode (telemetry-only) server; stop it after the session."""
    proc = _start_server_process(str(headnode_server_toml), HEADNODE_PORT)
    time.sleep(1.0)
    yield proc
    _stop_server_process(proc)


@pytest.fixture(scope="session")
def start_daq_node_server(daq_node_server_toml: Any) -> Any:
    """Start the daq_node server (daq_data + daq_control); stop after session."""
    proc = _start_server_process(str(daq_node_server_toml), DAQ_NODE_PORT)
    time.sleep(1.5)
    yield proc
    _stop_server_process(proc)


# ---------------------------------------------------------------------------
# Redis fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def redis_client() -> Generator[redis_sync.Redis, None, None]:
    """Session-scoped Redis client connected to the test DB."""
    r = redis_sync.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_TEST_DB, decode_responses=True)
    try:
        r.ping()
    except redis_sync.ConnectionError:
        pytest.fail(f"Redis not available at {REDIS_HOST}:{REDIS_PORT}. Start Redis locally or run via docker compose.")
    r.flushdb()
    yield r
    r.flushdb()
    r.close()


# ---------------------------------------------------------------------------
# Auto-use: reset PanosetiLogFactory singleton between tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_log_factory() -> Any:
    """Prevent gRPC client state from leaking between tests."""
    PanosetiLogFactory.reset_clients()
    yield
    PanosetiLogFactory.reset_clients()
