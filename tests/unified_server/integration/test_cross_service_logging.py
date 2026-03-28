"""
Integration tests: cross-service log routing via Telemetry.

When the unified server runs all three services, daq_data and daq_control
have grpc_logging=false in the test config (to keep tests hermetic). This
suite tests direct Telemetry Log RPCs and verifies the log schema written
to Redis — covering the plumbing that daq_data/daq_control would use in
production with grpc_logging=true.
"""
from __future__ import annotations

import json
import time

import grpc
import pytest

from panoseti_grpc.telemetry.client import TelemetryClient
from panoseti_grpc.generated import daq_data_pb2, daq_data_pb2_grpc
from google.protobuf.empty_pb2 import Empty

from tests.unified_server.conftest import (
    GRPC_PORT,
    poll_redis_list_len,
    poll_redis_key,
)

# Redis list key used by the TelemetryServicer's RedisBatcher
LOGS_KEY = "logs:ingress"

# Required fields per the LogSchema
REQUIRED_LOG_FIELDS = {"host", "service_name", "timestamp", "severity", "git_commit"}


# ---------------------------------------------------------------------------
# Basic log ingestion into Redis
# ---------------------------------------------------------------------------

def test_log_arrives_in_redis(start_unified_server, redis_client):
    """A single Log RPC from TelemetryClient appears in the logs:ingress list."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen(LOGS_KEY)
    future = client.send_log_future(
        service="cross-svc-test",
        severity=2,
        message=json.dumps({"event": "basic_ingestion"}),
    )
    result = future.result(timeout=10.0)
    assert result.success

    reached = poll_redis_list_len(redis_client, LOGS_KEY, before_len + 1, timeout=15.0)
    assert reached, "Log did not appear in Redis logs:ingress within timeout"


def test_log_entry_has_required_fields(start_unified_server, redis_client):
    """Log entries stored in Redis contain all required LogSchema fields."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen(LOGS_KEY)

    future = client.send_log_future(
        service="schema-validation-test",
        severity=3,
        message=json.dumps({"check": "fields"}),
    )
    future.result(timeout=10.0)

    poll_redis_list_len(redis_client, LOGS_KEY, before_len + 1, timeout=15.0)

    # Fetch the most recent entry
    raw_entries = redis_client.lrange(LOGS_KEY, before_len, -1)
    assert raw_entries, "No new log entries found in Redis"
    log_json = json.loads(raw_entries[-1])

    missing = REQUIRED_LOG_FIELDS - set(log_json.keys())
    assert not missing, f"Log entry missing required fields: {missing}. Entry: {log_json}"


def test_log_entry_preserves_service_name(start_unified_server, redis_client):
    """The service_name field in Redis matches what was sent."""
    service_name = "test-service-name-check"
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen(LOGS_KEY)

    future = client.send_log_future(
        service=service_name,
        severity=2,
        message=json.dumps({"check": "service_name"}),
    )
    future.result(timeout=10.0)

    poll_redis_list_len(redis_client, LOGS_KEY, before_len + 1, timeout=15.0)
    raw_entries = redis_client.lrange(LOGS_KEY, before_len, -1)
    assert raw_entries
    log_json = json.loads(raw_entries[-1])
    assert log_json.get("service_name") == service_name, (
        f"service_name mismatch: expected '{service_name}', got '{log_json.get('service_name')}'"
    )


# ---------------------------------------------------------------------------
# Multiple concurrent logs
# ---------------------------------------------------------------------------

def test_multiple_logs_all_reach_redis(start_unified_server, redis_client):
    """Five sequential log futures all succeed and appear in Redis."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen(LOGS_KEY)
    n = 5

    futures = [
        client.send_log_future(
            service="multi-log-test",
            severity=2,
            message=json.dumps({"i": i}),
        )
        for i in range(n)
    ]
    for f in futures:
        result = f.result(timeout=10.0)
        assert result.success, f"Log future returned success=False"

    reached = poll_redis_list_len(redis_client, LOGS_KEY, before_len + n, timeout=20.0)
    assert reached, (
        f"Expected {before_len + n} entries in Redis, got {redis_client.llen(LOGS_KEY)}"
    )


# ---------------------------------------------------------------------------
# DaqData + Telemetry coexistence
# ---------------------------------------------------------------------------

def test_daq_data_ping_does_not_corrupt_telemetry_redis(start_unified_server, redis_client):
    """DaqData Ping RPC does not write to or corrupt the telemetry Redis state."""
    client = TelemetryClient(host="localhost", port=GRPC_PORT)
    before_len = redis_client.llen(LOGS_KEY)

    # Send a telemetry log, record the expected length after flush
    future = client.send_log_future(
        service="coexistence-test",
        severity=2,
        message=json.dumps({"phase": "before_ping"}),
    )
    future.result(timeout=10.0)
    poll_redis_list_len(redis_client, LOGS_KEY, before_len + 1, timeout=15.0)
    after_log_len = redis_client.llen(LOGS_KEY)

    # Now do a DaqData Ping
    with grpc.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = daq_data_pb2_grpc.DaqDataStub(channel)
        stub.Ping(Empty(), timeout=5.0, wait_for_ready=True)

    # Redis length should not have decreased (Ping writes nothing to Redis)
    current_len = redis_client.llen(LOGS_KEY)
    assert current_len >= after_log_len, (
        f"Redis log count decreased after DaqData Ping: {after_log_len} → {current_len}"
    )
