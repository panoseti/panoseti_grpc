import json
import time
import uuid
from typing import Any

from panoseti_grpc.telemetry.logger import get_logger


def fetch_logs(redis_client: Any, session_id: str) -> list[dict[str, Any]]:
    """
    Parses the 'logs:ingress' list.
    The Telemetry Server stores logs as JSON strings.
    The worker's data is inside the 'payload_json' field.
    """
    # Grab a large enough buffer from the ingress list
    raw_entries = redis_client.lrange("logs:ingress", 0, -1)
    parsed_logs = []

    for entry in raw_entries:
        try:
            # Level 1: The outer LogSchema (host, service_name, etc.)
            envelope = json.loads(entry)

            # Level 2: The worker's payload (the 'message' we sent)
            # The Telemetry Server puts the gRPC payload into 'payload_json'
            worker_payload = json.loads(envelope.get("payload_json", "{}"))

            if worker_payload.get("session_id") == session_id:
                # Attach envelope metadata for better debugging
                worker_payload["_remote_host"] = envelope.get("host")
                parsed_logs.append(worker_payload)
        except json.JSONDecodeError, TypeError:
            continue
    return parsed_logs


def wait_for_logs(redis_client: Any, session_id: Any, min_count: Any, timeout: Any = 20) -> None:
    """Wait for at least X logs to appear in Redis, with a timeout."""
    start = time.time()
    while time.time() - start < timeout:
        logs = fetch_logs(redis_client, session_id)
        if len(logs) >= min_count:
            return logs
        time.sleep(0.5)
    raise TimeoutError(f"Only found {len(fetch_logs(redis_client, session_id))} logs after {timeout}s")


def test_distributed_throughput(redis_client: Any, distributed_session: Any) -> None:
    """
    Verifies the system can handle concurrent traffic from multiple workers.
    Each worker sends ~20 logs/sec.
    """
    target_count = 200  # Expecting 200 logs total from all workers
    logs = wait_for_logs(redis_client, distributed_session, target_count)

    assert len(logs) >= target_count

    # Verify we are hearing from multiple unique hosts
    hosts = {log.get("host") for log in logs}
    print(f"Heard from {len(hosts)} workers: {hosts}")
    assert len(hosts) > 1, "Only one worker is sending logs!"


def test_distributed_integrity(redis_client: Any, distributed_session: Any) -> None:
    """
    Verifies that for every worker, the sequence numbers are strictly
    monotonic (no dropped logs).
    """
    logs = wait_for_logs(redis_client, distributed_session, 100)

    # Group logs by worker hostname
    worker_data = {}
    for log in logs:
        h = log["host"]
        if h not in worker_data:
            worker_data[h] = []
        worker_data[h].append(log["seq"])

    for host, seqs in worker_data.items():
        sorted_seqs = sorted(seqs)
        # Check for gaps in sequence
        for i in range(len(sorted_seqs) - 1):
            diff = sorted_seqs[i + 1] - sorted_seqs[i]
            assert diff == 1, f"Gap detected on {host}: {sorted_seqs[i]} -> {sorted_seqs[i + 1]}"


def test_distributed_concurrency_isolation(redis_client: Any, distributed_session: Any) -> None:
    """
    Ensures that logs from different workers don't "bleed" into each other
    and that metadata (host/service) remains consistent.
    """
    # Wait for a decent sample size from the swarm
    logs = wait_for_logs(redis_client, distributed_session, min_count=100)

    for log in logs:
        # Every log must have the current session ID
        assert log["session_id"] == distributed_session
        # Ensure 'host' is present and not the headnode (it's a worker)
        assert "worker-" in log["host"]
        # Ensure the sequence number is an integer
        assert isinstance(log["seq"], int)


def test_high_frequency_burst(redis_client: Any, distributed_session: Any) -> None:
    """
    Validates that the system remains responsive when workers
    increase their log rate.
    """
    # We poll Redis to see if the count is steadily increasing
    initial_count = len(fetch_logs(redis_client, distributed_session))
    time.sleep(2)
    new_count = len(fetch_logs(redis_client, distributed_session))

    # Check that we are actually receiving data in real-time
    assert new_count > initial_count, "Data flow has stalled"

    # Calculate approximate messages per second across the cluster
    mps = (new_count - initial_count) / 2
    print(f"📈 Cluster Throughput: ~{mps:.2f} msg/sec")
    assert mps > 10, "Cluster throughput is lower than expected for 4 workers"


def test_mixed_traffic_stability(redis_client: Any, distributed_session: Any) -> None:
    """
    Scenario: The system is under load from 4 standard workers.
    We inject 'Rogue' data from the Test Runner to ensure the Server/Redis pipeline handles
    mixed content types without crashing.
    """
    rogue_service = "ROGUE_AGENT"
    # Use get_logger factory (which now uses the shared client registry)
    rogue_logger = get_logger(rogue_service, grpc_enabled=True)

    complex_payload = {
        "user": "test_runner",
        "nested": {"level1": {"level2": [1, 2, "3"]}},
        "special_chars": "µ-service 🚀 | \n \t",
        "session_id": distributed_session,
    }

    print("⚔️ Injecting complex data into the active stream...")

    for i in range(20):
        complex_payload["seq"] = i
        rogue_logger.info(complex_payload)
        time.sleep(0.1)

    time.sleep(2)

    # Use generic fetch which already handles fetching logs:ingress
    # Note: We can't strictly use fetch_logs because rogue logs might not have 'host' field matched
    # So we do a manual scan similar to the failure case but with decoding.

    logs = redis_client.lrange("logs:ingress", -500, -1)
    rogue_count = 0

    for entry in logs:
        try:
            data = json.loads(entry)

            if data.get("service_name") == rogue_service.lower():
                payload_str = data.get("payload_json", "{}")

                # FIX: Decode the inner JSON payload!
                # The server stores: "payload_json": "{\"text\": \"{'user': ...}\"}"
                # or directly the json dict depending on implementation.

                inner_payload = json.loads(payload_str)

                # If the logger wrapped the dict in a "text" field string representation
                if "text" in inner_payload and isinstance(inner_payload["text"], str):
                    # It might be a string representation of a dict.
                    # Python's logging often str()s the dict if passed as msg.
                    # We check the raw string for the unicode char (which JSON unescapes)
                    # BUT: If it was logged as a dict, client.py might send it as Struct or JSON string.

                    # Robust check: Just check the unescaped values in whatever container they arrived
                    text_blob = str(inner_payload)
                    assert "µ-service 🚀" in text_blob
                else:
                    # It's a proper dict
                    assert "µ-service 🚀" in inner_payload.get("special_chars", "")

                rogue_count += 1

        except json.JSONDecodeError, TypeError:
            continue

    assert rogue_count >= 20, "Server dropped the complex payloads!"


def test_session_bleeding(redis_client: Any) -> None:
    """
    Scenario: Rapidly switch Session IDs.
    Goal: Ensure logs from 'Session A' do not appear in the time window
    assigned to 'Session B'. This verifies workers update their state cleanly.
    """
    # Phase 1: Session A
    session_a = str(uuid.uuid4())
    redis_client.set("DISTRIBUTED_SESSION_ID", session_a)
    time.sleep(4)  # Let them blast

    # Phase 2: HARD SWITCH to Session B
    session_b = str(uuid.uuid4())
    print(f"🔀 Switching {session_a} -> {session_b}")
    redis_client.set("DISTRIBUTED_SESSION_ID", session_b)

    # Let logs accumulate for B
    time.sleep(4)

    # Fetch recent logs
    logs = redis_client.lrange("logs:ingress", -500, -1)

    # Logic: Find the FIRST occurrence of Session B.
    # Any log *after* that index should NOT contain Session A.

    first_b_index = -1
    for i, entry in enumerate(logs):
        if session_b in entry:
            first_b_index = i
            break

    assert first_b_index != -1, "Workers never picked up Session B!"

    # Check tail
    tail_logs = logs[first_b_index:]
    leaked_a_logs = [log for log in tail_logs if session_a in log]

    assert len(leaked_a_logs) == 0, (
        f"Bleeding detected! Found {len(leaked_a_logs)} logs from Session A *after* Session B started."
    )


def test_worker_recovery(redis_client: Any) -> None:
    """
    Scenario: The coordination key (Session ID) disappears (simulating network split
    or leader failover). Workers should go silent.
    Then key reappears. Workers should resume.
    """
    session_id = str(uuid.uuid4())

    # 1. Start
    redis_client.set("DISTRIBUTED_SESSION_ID", session_id)
    time.sleep(3)
    initial_count = len(redis_client.lrange("logs:ingress", 0, -1))

    # 2. Kill Key (Partition)
    print("✂️ Severing connection (Deleting Session Key)")
    redis_client.delete("DISTRIBUTED_SESSION_ID")
    time.sleep(3)

    count_during_outage = len(redis_client.lrange("logs:ingress", 0, -1))
    # Delta should be low (just logs that were already in flight)
    delta = count_during_outage - initial_count
    assert delta < 20, f"Workers kept sending during outage! Delta: {delta}"

    # 3. Restore Key (Recovery)
    print("🩹 Restoring connection")
    redis_client.set("DISTRIBUTED_SESSION_ID", session_id)
    time.sleep(5)

    final_count = len(redis_client.lrange("logs:ingress", 0, -1))
    recovery_delta = final_count - count_during_outage

    print(f"📈 Recovery Volume: {recovery_delta}")
    assert recovery_delta > 50, "Workers failed to recover after outage!"
