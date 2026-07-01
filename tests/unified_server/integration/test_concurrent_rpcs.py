"""
Integration tests: concurrent and interleaved RPCs across all three services.

Verifies that the unified server handles concurrent load without race
conditions, dropped requests, or cross-service interference.
"""

from __future__ import annotations

import asyncio
import json
import threading
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
    GRPC_PORT,
    poll_redis_list_len,
)

# ---------------------------------------------------------------------------
# Concurrent telemetry logs
# ---------------------------------------------------------------------------


def test_20_concurrent_telemetry_logs(start_unified_server: Any, redis_client: Any) -> None:
    """20 concurrent Log RPCs complete without errors and all appear in Redis."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen("logs:ingress")
    n = 20

    futures = [
        client.send_log_future(
            service="concurrent-log-test",
            severity=2,
            message=json.dumps({"i": i}),
        )
        for i in range(n)
    ]
    results = [f.result(timeout=15.0) for f in futures]
    assert all(r.success for r in results), f"Some log futures failed: {[r for r in results if not r.success]}"

    # Wait for all to land in Redis (RedisBatcher batches up to 100)
    reached = poll_redis_list_len(redis_client, "logs:ingress", before_len + n, timeout=30.0)
    assert reached, f"Expected {before_len + n} Redis entries, got {redis_client.llen('logs:ingress')}"


# ---------------------------------------------------------------------------
# Concurrent cross-service RPCs
# ---------------------------------------------------------------------------


def test_concurrent_pings_and_logs(start_unified_server: Any, redis_client: Any) -> None:
    """Concurrent DaqData pings and Telemetry logs complete without blocking each other."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen("logs:ingress")

    # Fire 10 telemetry log futures
    log_futures = [
        client.send_log_future(
            service="concurrent-cross-svc",
            severity=2,
            message=json.dumps({"i": i}),
        )
        for i in range(10)
    ]

    # Concurrently do 5 DaqData Pings in threads
    ping_errors: list[Exception] = []

    def do_ping() -> None:
        try:
            with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as ch:
                stub = daq_data_pb2_grpc.DaqDataStub(ch)
                stub.Ping(Empty(), timeout=5.0, wait_for_ready=True)
        except Exception as exc:
            ping_errors.append(exc)

    threads = [threading.Thread(target=do_ping) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    # All pings should have succeeded
    assert not ping_errors, f"Ping errors during concurrent test: {ping_errors}"

    # All log futures should succeed
    results = [f.result(timeout=10.0) for f in log_futures]
    assert all(r.success for r in results)

    reached = poll_redis_list_len(redis_client, "logs:ingress", before_len + 10, timeout=30.0)
    assert reached, "Not all concurrent logs appeared in Redis"


def test_concurrent_daq_control_and_telemetry(start_unified_server: Any, redis_client: Any, tmp_path: Any) -> None:
    """Concurrent DaqControl and Telemetry RPCs complete independently."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    redis_client.llen("logs:ingress")

    # Fire telemetry logs
    log_futures = [
        client.send_log_future(
            service="concurrent-dc-telem",
            severity=2,
            message=json.dumps({"k": k}),
        )
        for k in range(8)
    ]

    # Concurrent DaqControl StatusDaq in threads
    dc_errors: list[Exception] = []

    def do_status() -> None:
        try:
            with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as ch:
                stub = daq_control_pb2_grpc.DaqControlStub(ch)
                req = daq_control_pb2.DaqStatusRequest(
                    data_dir=str(tmp_path),
                    check_hashpipe_running=True,
                    check_disk_usage=False,
                    check_run_dirs=False,
                )
                resp = stub.StatusDaq(req, timeout=5.0, wait_for_ready=True)
                if not resp.success:
                    dc_errors.append(ValueError("StatusDaq returned success=False"))
        except Exception as exc:
            dc_errors.append(exc)

    threads = [threading.Thread(target=do_status) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not dc_errors, f"DaqControl errors: {dc_errors}"

    results = [f.result(timeout=10.0) for f in log_futures]
    assert all(r.success for r in results)


# ---------------------------------------------------------------------------
# Repeated pings: connection reuse and stability
# ---------------------------------------------------------------------------


def test_repeated_daq_data_pings_stable(start_unified_server: Any) -> None:
    """25 sequential DaqData Pings all succeed — verifies connection stability."""
    with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = daq_data_pb2_grpc.DaqDataStub(channel)
        for i in range(25):
            resp = stub.Ping(Empty(), timeout=3.0, wait_for_ready=True)
            assert resp is not None, f"Ping {i} returned None"


# ---------------------------------------------------------------------------
# Async concurrent RPCs
# ---------------------------------------------------------------------------


async def test_async_concurrent_cross_service_rpcs(start_unified_server, redis_client, tmp_path):
    """asyncio.gather of DaqData Ping + DaqControl Status + Telemetry Log completes."""
    redis_client.llen("logs:ingress")

    async def ping_daq_data():
        async with grpc.aio.insecure_channel(f"localhost:{GRPC_PORT}") as ch:
            stub = daq_data_pb2_grpc.DaqDataStub(ch)
            return await stub.Ping(Empty(), timeout=5.0, wait_for_ready=True)

    async def status_daq_control():
        async with grpc.aio.insecure_channel(f"localhost:{GRPC_PORT}") as ch:
            stub = daq_control_pb2_grpc.DaqControlStub(ch)
            req = daq_control_pb2.DaqStatusRequest(
                data_dir=str(tmp_path),
                check_hashpipe_running=True,
                check_disk_usage=False,
                check_run_dirs=False,
            )
            return await stub.StatusDaq(req, timeout=5.0, wait_for_ready=True)

    async def log_telemetry():
        # Use sync client future in a thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        client = TelemetryClient(host="localhost", port=GRPC_PORT)
        future = client.send_log_future("async-concurrent", 2, '{"async": true}')
        result = await loop.run_in_executor(None, lambda: future.result(timeout=10.0))
        return result

    ping_resp, dc_resp, log_result = await asyncio.gather(
        ping_daq_data(),
        status_daq_control(),
        log_telemetry(),
    )

    assert ping_resp is not None
    assert dc_resp.success
    assert log_result.success
