from __future__ import annotations

import json
from typing import Any


def build_retry_service_config(
    max_attempts: int = 3,
    initial_backoff: str = "0.5s",
    max_backoff: str = "30s",
    backoff_multiplier: float = 2.0,
    retryable_status_codes: list[str] | None = None,
) -> str:
    """Build a gRPC service_config JSON string enabling transport-level retries.

    Pass the result as the ``grpc.service_config`` channel option::

        channel = grpc.aio.insecure_channel(
            target,
            options=[("grpc.service_config", build_retry_service_config())],
        )

    Args:
        max_attempts: Maximum total attempts (including the first try).
        initial_backoff: Initial retry delay (e.g. "0.5s", "1s").
        max_backoff: Maximum retry delay.
        backoff_multiplier: Exponential factor between retries.
        retryable_status_codes: gRPC status codes that trigger a retry.
            Defaults to ["UNAVAILABLE"].
    """
    if retryable_status_codes is None:
        retryable_status_codes = ["UNAVAILABLE"]

    policy: dict[str, Any] = {
        "methodConfig": [
            {
                "name": [{}],
                "retryPolicy": {
                    "maxAttempts": max_attempts,
                    "initialBackoff": initial_backoff,
                    "maxBackoff": max_backoff,
                    "backoffMultiplier": backoff_multiplier,
                    "retryableStatusCodes": retryable_status_codes,
                },
            }
        ]
    }
    return json.dumps(policy)
