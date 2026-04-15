from typing import Any
import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor

from panoseti_grpc.telemetry.logger import get_logger

LOG_KEY = "logs:ingress"


def test_concurrent_loggers_race_condition( redis_client: Any) -> None:
    """
    Spins up multiple logger instances in different threads to ensure
    the gRPC server handles concurrent connections without dropping logs.
    """
    num_threads = 5
    logs_per_thread = 50
    total_expected = num_threads * logs_per_thread

    start_len = redis_client.llen(LOG_KEY)

    def worker_logger( worker_id: Any) -> None:
        # Each thread gets its own logger instance (simulating different modules)
        # They share the same gRPC port but are distinct clients
        # client = TelemetryClient(host="localhost", port=50051)
        name = f"WORKER_{worker_id}"
        logger = get_logger(name, grpc_enabled=True, level=logging.INFO)

        for i in range(logs_per_thread):
            # Send structured data
            payload = {"worker": worker_id, "seq": i, "data": random.random()}
            logger.info(json.dumps(payload))
            time.sleep(0.005)  # Slight delay to interleave

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_logger, i) for i in range(num_threads)]
        for f in futures:
            f.result()  # Wait for all

    # Allow time for async flush
    time.sleep(2.0)

    end_len = redis_client.llen(LOG_KEY)
    delta = end_len - start_len

    # 1. Check Packet Count
    assert delta == total_expected, f"Expected {total_expected} logs, got {delta}"

    # 2. Check Data Integrity
    # Scan all items to ensure every worker's logs made it
    items = redis_client.lrange(LOG_KEY, -total_expected, -1)

    worker_counts = {i: 0 for i in range(num_threads)}

    for item in items:
        try:
            data = json.loads(item)
            # Unwrap payload if needed (handling the 'text' wrapper)
            raw_payload = json.loads(data["payload_json"])
            if "text" in raw_payload and "worker" not in raw_payload:
                payload = json.loads(raw_payload["text"])
            else:
                payload = raw_payload

            if "worker" in payload:
                worker_counts[payload["worker"]] += 1
        except Exception:
            continue

    for i in range(num_threads):
        assert worker_counts[i] == logs_per_thread, f"Worker {i} missing logs! Got {worker_counts[i]}"


def test_server_enforces_log_schema( grpc_client: Any, redis_client: Any) -> None:
    """
    Verifies that the Server strictly enforces the LogSchema from config.py.
    This checks if the 'host' and 'service_name' validators are working.
    """
    # 1. Invalid Service Name (Too short)
    # config.py: service_name = Field(..., min_length=2)

    # We construct a raw message that violates the schema
    from google.protobuf.timestamp_pb2 import Timestamp

    from panoseti_grpc.generated import telemetry_pb2

    ts = Timestamp()
    ts.GetCurrentTime()

    req = telemetry_pb2.LogMessage(
        host="valid_host",
        service_name="x",  # INVALID: Length 1 < 2
        timestamp=ts,
        severity=2,
        payload_json='{"msg": "test"}',
    )

    # Send directly via stub to bypass client-side checks (if any)
    resp = grpc_client.stub.Log(req)

    # The server should catch the ValidationError and return success=False
    assert resp.success is False
    assert "validation error" in resp.message.lower()

    # 2. Invalid Payload JSON
    req_bad_json = telemetry_pb2.LogMessage(
        host="valid_host",
        service_name="valid_service",
        timestamp=ts,
        severity=2,
        payload_json="{NOT_JSON}",  # INVALID
    )

    resp = grpc_client.stub.Log(req_bad_json)

    assert resp.success is False
    assert "value error" in resp.message.lower() or "json" in resp.message.lower()
