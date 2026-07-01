"""Configuration model for the MLInference gRPC service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class MLInferenceServerConfig(BaseModel):
    """Configuration for the MLInference service (loaded from server.toml)."""

    # Alert threshold: predictions with cloud_score >= this value are also
    # broadcast on SubscribeAlerts streams.  Default matches the model's
    # baked-in decision threshold.
    alert_threshold: float = Field(default=0.5, ge=0.0, le=1.0)

    # Maximum number of concurrent StreamPredictions / SubscribeAlerts
    # subscribers.  Beyond this limit new subscriptions are rejected with
    # RESOURCE_EXHAUSTED.
    max_subscribers: int = Field(default=100, ge=1)

    # asyncio.Queue maxsize for each subscriber queue.  Backpressure: if a
    # slow subscriber fills its queue, the oldest prediction is dropped and a
    # warning is logged.
    subscriber_queue_size: int = Field(default=256, ge=1)

    model_config = {"extra": "ignore"}
