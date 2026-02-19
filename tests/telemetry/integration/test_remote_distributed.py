import pytest
import json
import time
import uuid
from typing import List, Dict


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


def fetch_logs(redis_client, session_id: str) -> List[Dict]:
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
            worker_payload = json.loads(envelope.get('payload_json', '{}'))

            if worker_payload.get('session_id') == session_id:
                # Attach envelope metadata for better debugging
                worker_payload['_remote_host'] = envelope.get('host')
                parsed_logs.append(worker_payload)
        except (json.JSONDecodeError, TypeError):
            continue
    return parsed_logs


def wait_for_logs(redis_client, session_id, min_count, timeout=20):
    """Wait for at least X logs to appear in Redis, with a timeout."""
    start = time.time()
    while time.time() - start < timeout:
        logs = fetch_logs(redis_client, session_id)
        if len(logs) >= min_count:
            return logs
        time.sleep(0.5)
    raise TimeoutError(f"Only found {len(fetch_logs(redis_client, session_id))} logs after {timeout}s")


def test_distributed_throughput(redis_client, distributed_session):
    """
    Verifies the system can handle concurrent traffic from multiple workers.
    Each worker sends ~20 logs/sec.
    """
    target_count = 200  # Expecting 200 logs total from all workers
    logs = wait_for_logs(redis_client, distributed_session, target_count)

    assert len(logs) >= target_count

    # Verify we are hearing from multiple unique hosts
    hosts = {l.get('host') for l in logs}
    print(f"Heard from {len(hosts)} workers: {hosts}")
    assert len(hosts) > 1, "Only one worker is sending logs!"


def test_distributed_integrity(redis_client, distributed_session):
    """
    Verifies that for every worker, the sequence numbers are strictly
    monotonic (no dropped logs).
    """
    logs = wait_for_logs(redis_client, distributed_session, 100)

    # Group logs by worker hostname
    worker_data = {}
    for l in logs:
        h = l['host']
        if h not in worker_data: worker_data[h] = []
        worker_data[h].append(l['seq'])

    for host, seqs in worker_data.items():
        sorted_seqs = sorted(seqs)
        # Check for gaps in sequence
        for i in range(len(sorted_seqs) - 1):
            diff = sorted_seqs[i + 1] - sorted_seqs[i]
            assert diff == 1, f"Gap detected on {host}: {sorted_seqs[i]} -> {sorted_seqs[i + 1]}"


def test_distributed_concurrency_isolation(redis_client, distributed_session):
    """
    Ensures that logs from different workers don't "bleed" into each other
    and that metadata (host/service) remains consistent.
    """
    # Wait for a decent sample size from the swarm
    logs = wait_for_logs(redis_client, distributed_session, min_count=100)

    for log in logs:
        # Every log must have the current session ID
        assert log['session_id'] == distributed_session
        # Ensure 'host' is present and not the headnode (it's a worker)
        assert 'worker-' in log['host']
        # Ensure the sequence number is an integer
        assert isinstance(log['seq'], int)


def test_high_frequency_burst(redis_client, distributed_session):
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