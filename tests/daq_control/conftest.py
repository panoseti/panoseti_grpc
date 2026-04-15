import asyncio
import multiprocessing
import socket
import time
from pathlib import Path

import pytest

from panoseti_grpc.daq_control.client import DaqControlClient

# Import the serve function
from panoseti_grpc.daq_control.server import serve

# ---------------------------------------------------------------------------
# Shared polling utilities — preferred over hardcoded time.sleep() waits
# ---------------------------------------------------------------------------


def wait_for_file(path, timeout=10.0, poll=0.1) -> bool:
    """Return True once the file at `path` exists; False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if Path(path).exists():
            return True
        time.sleep(poll)
    return False


def wait_for_pid_gone(pid, timeout=10.0, poll=0.1) -> bool:
    """Return True once the given PID no longer exists; False on timeout."""
    import psutil

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not psutil.pid_exists(pid):
            return True
        time.sleep(poll)
    return False


SERVER_PORT = 50051


def _run_server_process(grpc_port):
    """
    Runs the server in a separate process.
    """
    # NOTE: Config is loaded from default location (resources.py) or env var
    asyncio.run(serve(grpc_port=grpc_port))


@pytest.fixture(scope="session")
def start_grpc_server():
    """
    Starts the gRPC server in a separate multiprocessing.Process.
    """
    # 1. Start Server Process
    proc = multiprocessing.Process(target=_run_server_process, args=[SERVER_PORT], daemon=True)
    proc.start()

    # 2. Wait for Port to Open (Health Check)
    start_time = time.time()
    server_ready = False
    while time.time() - start_time < 10:
        if not proc.is_alive():
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
    return DaqControlClient(host="localhost", port=SERVER_PORT)
