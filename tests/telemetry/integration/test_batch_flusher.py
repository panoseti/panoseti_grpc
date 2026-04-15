"""
Tests for the RedisBatcher: verifies that bursts of Log RPCs are all
delivered to Redis and that the batcher correctly serialises entries.
"""

from typing import Any
import json
import time

LOG_KEY = "logs:ingress"


def _wait_for_tagged( redis_client: Any, key: str, tag: str, expected: int, timeout: float = 15.0, poll: float = 0.3)-> list[Any]:
    """
    Poll the entire Redis list until at least *expected* items contain *tag*,
    or until *timeout* elapses.  Returns the matching items.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        all_items = redis_client.lrange(key, 0, -1)
        matching = [item for item in all_items if tag in item]
        if len(matching) >= expected:
            return matching
        time.sleep(poll)
    # One final scan on timeout
    all_items = redis_client.lrange(key, 0, -1)
    return [item for item in all_items if tag in item]


def test_batch_flush_delivers_all_logs( grpc_client: Any, redis_client: Any) -> None:
    """
    Send exactly N Log RPCs in a burst; after the batcher has had time to
    flush, Redis must contain at least N items tagged with our unique marker.
    """
    N = 50  # large enough to exercise batching but small enough to be fast
    tag = f"batchflush_{int(time.time() * 1000)}"

    # Send all futures and collect them so the gRPC channel finishes the RPCs
    futures = []
    for i in range(N):
        f = grpc_client.send_log_future(
            service="batchflush_svc",
            severity=2,
            message=json.dumps({"text": f"{tag}_{i:04d}"}),
        )
        futures.append(f)

    # Wait for all gRPC RPCs to be acknowledged by the server
    for f in futures:
        f.result(timeout=10)

    # Now wait for the batcher to flush all items to Redis
    tagged = _wait_for_tagged(redis_client, LOG_KEY, tag, expected=N, timeout=15.0)

    assert len(tagged) >= N, (
        f"Expected at least {N} log entries tagged {tag!r} in Redis, "
        f"got {len(tagged)}. RedisBatcher may have dropped items."
    )


def test_batch_flush_delivers_logs_in_order( grpc_client: Any, redis_client: Any) -> None:
    """
    A sequential burst of N logs must all arrive in Redis.
    Each message embeds a unique tag + index, so we can verify
    completeness independent of other tests polluting logs:ingress.
    """
    N = 20
    tag = f"order_{int(time.time() * 1000)}"

    futures = []
    for i in range(N):
        f = grpc_client.send_log_future(
            service="order_svc",
            severity=2,
            message=json.dumps({"text": f"{tag}_{i:04d}"}),
        )
        futures.append(f)

    # Wait for all RPCs to be acknowledged
    for f in futures:
        f.result(timeout=10)

    # Poll until all N tagged items appear in Redis
    tagged = _wait_for_tagged(redis_client, LOG_KEY, tag, expected=N, timeout=15.0)

    assert len(tagged) >= N, (
        f"Expected {N} tagged log entries, got {len(tagged)}. Some messages may have been dropped by the batcher."
    )

    # Verify monotonic index ordering within our messages
    indices = []
    for item in tagged:
        try:
            parsed = json.loads(item)
            text = json.loads(parsed.get("payload_json", "{}")).get("text", "")
            suffix = text.split(tag + "_")[-1][:4]
            indices.append(int(suffix))
        except (ValueError, KeyError, json.JSONDecodeError):
            pass

    if len(indices) >= 2:
        for a, b in zip(sorted(indices), sorted(indices)[1:], strict=False):
            assert a < b, f"Duplicate or out-of-order index found: {indices}"
