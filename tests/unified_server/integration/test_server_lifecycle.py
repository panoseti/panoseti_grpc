"""
Integration tests: unified server lifecycle and basic service routing.

Verifies that the PanosetiServer starts correctly, routes RPCs to all three
servicers on a single TCP port, and cleans up after shutdown.

All three services are tested on the SAME port (GRPC_PORT / 50055) to
demonstrate the multiplexed gRPC routing that is the core feature of the
unified server.
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import time
from typing import Any

import grpc
from google.protobuf.empty_pb2 import Empty

from panoseti_grpc.generated import (
    daq_control_pb2,
    daq_control_pb2_grpc,
    daq_data_pb2_grpc,
)
from panoseti_grpc.telemetry.client import TelemetryClient
from tests.unified_server.conftest import (
    DAQ_NODE_PORT,
    GRPC_PORT,
    _start_server_process,
    _stop_server_process,
    poll_redis_list_len,
    wait_for_port,
)

# ---------------------------------------------------------------------------
# TCP connectivity
# ---------------------------------------------------------------------------


def test_server_accepts_tcp_connections(start_unified_server: Any) -> None:
    """TCP connection to GRPC_PORT succeeds after server start."""
    with socket.create_connection(("localhost", GRPC_PORT), timeout=2.0) as s:
        assert s.fileno() != -1


# ---------------------------------------------------------------------------
# DaqData service on unified port
# ---------------------------------------------------------------------------


def test_daq_data_ping_on_unified_server(start_unified_server: Any) -> None:
    """DaqData Ping RPC reaches the DaqDataServicer on the shared port."""
    with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = daq_data_pb2_grpc.DaqDataStub(channel)
        resp = stub.Ping(Empty(), timeout=5.0, wait_for_ready=True)
        # Ping returns an Empty; no exception means success
        assert resp is not None


# ---------------------------------------------------------------------------
# Telemetry service on unified port
# ---------------------------------------------------------------------------


def test_telemetry_log_rpc_on_unified_server(start_unified_server: Any, redis_client: Any) -> None:
    """Telemetry Log RPC routes to TelemetryServicer on the shared port."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    future = client.send_log_future(
        service="lifecycle-test",
        severity=2,
        message=json.dumps({"event": "lifecycle_test", "iteration": 1}),
    )
    result = future.result(timeout=10.0)
    assert result.success, f"Log RPC returned success=False: {result}"

    # Log should eventually appear in Redis (RedisBatcher flush latency)
    reached_redis = poll_redis_list_len(redis_client, "logs:ingress", 1, timeout=15.0)
    assert reached_redis, "Telemetry log did not arrive in Redis within timeout"


# ---------------------------------------------------------------------------
# DaqControl service on unified port
# ---------------------------------------------------------------------------


def test_daq_control_status_on_unified_server(start_unified_server: Any, tmp_path: Any) -> None:
    """DaqControl StatusDaq routes to DaqControlServicer on the shared port."""
    with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = daq_control_pb2_grpc.DaqControlStub(channel)
        request = daq_control_pb2.DaqStatusRequest(
            data_dir=str(tmp_path),
            check_hashpipe_running=True,
            check_disk_usage=False,
            check_run_dirs=False,
        )
        resp = stub.StatusDaq(request, timeout=5.0, wait_for_ready=True)
        assert resp.success
        # hashpipe_running is False (no actual hashpipe in test env)
        assert isinstance(resp.hashpipe_running, bool)


# ---------------------------------------------------------------------------
# All three on the same port
# ---------------------------------------------------------------------------


def test_all_three_services_reachable_same_port(start_unified_server: Any, redis_client: Any, tmp_path: Any) -> None:
    """All three gRPC services respond on the same TCP port without interference."""
    # 1. DaqData Ping
    with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        dd_stub = daq_data_pb2_grpc.DaqDataStub(channel)
        dd_stub.Ping(Empty(), timeout=5.0, wait_for_ready=True)

    # 2. DaqControl StatusDaq
    with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        dc_stub = daq_control_pb2_grpc.DaqControlStub(channel)
        dc_req = daq_control_pb2.DaqStatusRequest(
            data_dir=str(tmp_path),
            check_hashpipe_running=True,
            check_disk_usage=False,
            check_run_dirs=False,
        )
        dc_resp = dc_stub.StatusDaq(dc_req, timeout=5.0, wait_for_ready=True)
        assert dc_resp.success

    # 3. Telemetry Log
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    f = client.send_log_future("lifecycle-all-services", 2, '{"test": "all_three"}')
    result = f.result(timeout=10.0)
    assert result.success


# ---------------------------------------------------------------------------
# Graceful shutdown: port is freed after process exits
# ---------------------------------------------------------------------------


def test_graceful_shutdown_frees_port(daq_node_server_toml: Any, tmp_path_factory: Any) -> None:
    """After the server process exits, the TCP port is released and re-bindable.

    Uses the daq_node profile (no telemetry, no Redis) for an isolated test
    so this can run without any external services.
    """
    # Use a dedicated port to avoid affecting other test fixtures
    SHUTDOWN_PORT = 50070

    # Rewrite TOML with a unique port for this test
    shutdown_toml = tmp_path_factory.mktemp("shutdown_cfg") / "shutdown.toml"
    original = daq_node_server_toml.read_text()
    patched = original.replace(f"port = {DAQ_NODE_PORT}", f"port = {SHUTDOWN_PORT}", 1)
    shutdown_toml.write_text(patched)

    proc = _start_server_process(str(shutdown_toml), SHUTDOWN_PORT)

    # Verify port is open
    assert wait_for_port("localhost", SHUTDOWN_PORT, timeout=5.0), "Server did not bind"

    # Stop the server
    _stop_server_process(proc)

    # Port should no longer accept connections
    deadline = time.monotonic() + 5.0
    port_freed = False
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", SHUTDOWN_PORT), timeout=0.2):
                time.sleep(0.1)
        except OSError:
            port_freed = True
            break
    assert port_freed, f"Port {SHUTDOWN_PORT} was not freed after server shutdown"


# ---------------------------------------------------------------------------
# pseti-grpc daqnode status — live server
# ---------------------------------------------------------------------------


def test_daqnode_status_reports_serving_against_live_server(start_unified_server: Any) -> None:
    """pseti-grpc daqnode with a live server: all active services report SERVING."""
    exe = shutil.which("pseti-grpc") or "pseti-grpc"
    result = subprocess.run(
        [exe, "--port", str(GRPC_PORT), "--json", "daqnode", "--skip-alloy", "--log-dir", "/tmp"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, f"pseti-grpc daqnode exited {result.returncode}: {result.stderr}"
    data = json.loads(result.stdout)
    serving = {s["service"] for s in data["grpc_services"] if "SERVING" in s["detail"]}
    # The unified server hosts all three active services.
    assert "daqdata.DaqData" in serving
    assert "panoseti.daq_control.DaqControl" in serving
    assert "panoseti.telemetry.Telemetry" in serving
