import pytest
import time
import json
import logging
from panoseti_grpc.telemetry.client import make_grpc_logger, TelemetryClient

LOG_KEY = "logs:ingress"


def test_basic_log_rpc(grpc_client, redis_client):
    """Verifies that a direct gRPC Log call ends up in Redis."""
    msg = "Integration Test - Raw RPC"
    timestamp = time.time()

    grpc_client.send_log_sync(
        service="TEST_INTEGRATION_RPC",
        severity=2,
        message=json.dumps({"text": msg}),
        timestamp=timestamp,
        file_path=__file__,
        line_number=15,
        function_name="test_basic_log_rpc"
    )

    time.sleep(0.5)

    log_json = redis_client.lindex(LOG_KEY, -1)
    assert log_json is not None, "Redis list is empty"
    data = json.loads(log_json)

    assert data["service_name"] == "test_integration_rpc"
    assert data["severity"] == 2

    payload = json.loads(data["payload_json"])
    assert payload["text"] == msg


def test_async_logger_pipeline(redis_client):
    """Verifies the end-to-end Python Logger -> Redis flow."""
    logger_name = "TEST_LOGGER_PIPELINE"

    # FIXED: Use 'port' argument
    client = TelemetryClient(host="localhost", port=50051)

    logger = make_grpc_logger(
        logger_name,
        grpc_client=client,
        level=logging.INFO
    )

    test_payload = {"sensor_id": 99, "status": "nominal"}
    logger.info(json.dumps(test_payload))

    time.sleep(1.0)

    items = redis_client.lrange(LOG_KEY, -10, -1)
    found_item = None

    for item in items:
        try:
            data = json.loads(item)
            if data.get("service_name") == logger_name.lower():
                found_item = data
                break
        except json.JSONDecodeError:
            continue

    assert found_item is not None, f"Log from {logger_name} not found in Redis."

    stored_payload = json.loads(found_item["payload_json"])

    # FIX: Handle the case where the client wrapped the JSON string in {"text": "..."}
    if "text" in stored_payload and len(stored_payload) == 1:
        try:
            # Try to parse the inner content as JSON
            inner_payload = json.loads(stored_payload["text"])
            stored_payload = inner_payload
        except json.JSONDecodeError:
            pass  # It was just a text message called "text"

    assert stored_payload == test_payload
    assert found_item["severity"] == 2


def test_burst_logging(redis_client):
    """Chaos Test: Sends 500 logs rapidly."""
    burst_count = 500
    service_name = "BURST_TEST"
    # FIXED: Use 'port' argument
    client = TelemetryClient(host="localhost", port=50051)
    logger = make_grpc_logger(service_name, grpc_client=client, queue_size=burst_count + 100, level=logging.INFO)

    start_len = redis_client.llen(LOG_KEY)

    for i in range(burst_count):
        logger.info(json.dumps({"seq": i}))

    time.sleep(3.0)

    end_len = redis_client.llen(LOG_KEY)
    delta = end_len - start_len

    assert delta >= burst_count, f"Packet Loss Detected! Sent {burst_count}, Stored {delta}"


def test_severity_mapping(redis_client):
    """Verifies Python Log Levels -> PANOSETI Severities."""
    service_name = "LEVEL_TEST"
    # FIXED: Use 'port' argument
    client = TelemetryClient(host="localhost", port=50051)
    logger = make_grpc_logger(service_name, grpc_client=client, level=logging.DEBUG)

    logger.debug("Test Debug")  # 1
    logger.info("Test Info")  # 2
    logger.warning("Test Warning")  # 3
    logger.error("Test Error")  # 4
    logger.critical("Test Crit")  # 5

    time.sleep(1.0)

    items = redis_client.lrange(LOG_KEY, -20, -1)
    mapped_levels = []

    for item in items:
        data = json.loads(item)
        if data["service_name"] == service_name.lower():
            payload = json.loads(data["payload_json"])
            msg = payload.get("text", "")
            if "Test" in msg:
                mapped_levels.append((msg, data["severity"]))

    mapped_levels.sort(key=lambda x: x[1])

    assert ("Test Debug", 1) in mapped_levels
    assert ("Test Info", 2) in mapped_levels
    assert ("Test Warning", 3) in mapped_levels
    assert ("Test Error", 4) in mapped_levels
    assert ("Test Crit", 5) in mapped_levels