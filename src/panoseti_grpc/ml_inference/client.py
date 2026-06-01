"""
MLInference gRPC client.

Provides both sync and async clients for the MLInference service.
The primary use case inside panoseti_analysis is the *emitter* path:
the Ray Serve inference deployment calls ``emit_prediction()`` (or
``async_emit_prediction()``) to push each scored window to the servicer.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

import grpc
import grpc.aio

from panoseti_grpc.generated import ml_inference_pb2, ml_inference_pb2_grpc


# ---------------------------------------------------------------------------
# Sync client
# ---------------------------------------------------------------------------


class MLInferenceClient:
    """
    Synchronous MLInference client.

    Usage::

        with MLInferenceClient("localhost", 50051) as client:
            client.emit_prediction(module_id=1, data_product="img16", ...)
    """

    def __init__(self, host: str = "localhost", port: int = 50051) -> None:
        self._target = f"{host}:{port}"
        self._channel: grpc.Channel | None = None
        self._stub: ml_inference_pb2_grpc.MLInferenceStub | None = None

    # -- context manager --

    def __enter__(self) -> "MLInferenceClient":
        self._channel = grpc.insecure_channel(self._target)
        self._stub = ml_inference_pb2_grpc.MLInferenceStub(self._channel)
        return self

    def __exit__(self, *_: Any) -> None:
        if self._channel is not None:
            self._channel.close()

    @property
    def stub(self) -> ml_inference_pb2_grpc.MLInferenceStub:
        if self._stub is None:
            raise RuntimeError("Client not started — use as context manager")
        return self._stub

    # -- RPCs --

    def emit_prediction(
        self,
        *,
        module_id: int,
        data_product: str,
        model_name: str,
        model_version: str,
        recipe_hash: str,
        git_sha: str,
        cloud_score: float,
        cloud_label: bool,
        t_start_ns: int,
        t_end_ns: int,
        calibration_maturity: float = 1.0,
        timeout: float = 10.0,
    ) -> ml_inference_pb2.EmitAck:
        """Emit a scored window prediction to the ML service."""
        from google.protobuf.timestamp_pb2 import Timestamp

        ts = Timestamp()
        ts.GetCurrentTime()
        prediction = ml_inference_pb2.Prediction(
            module_id=module_id,
            data_product=data_product,
            model_name=model_name,
            model_version=model_version,
            recipe_hash=recipe_hash,
            git_sha=git_sha,
            cloud_score=cloud_score,
            cloud_label=cloud_label,
            t_start_ns=t_start_ns,
            t_end_ns=t_end_ns,
            calibration_maturity=calibration_maturity,
            emitted_at=ts,
        )
        return self.stub.EmitPrediction(prediction, timeout=timeout)

    def stream_predictions(
        self,
        *,
        module_ids: list[int] | None = None,
        data_products: list[str] | None = None,
        model_names: list[str] | None = None,
        timeout: float | None = None,
    ) -> Iterator[ml_inference_pb2.Prediction]:
        """Iterate over live predictions (blocking server-streaming call)."""
        request = ml_inference_pb2.PredictionRequest(
            module_ids=module_ids or [],
            data_products=data_products or [],
            model_names=model_names or [],
        )
        yield from self.stub.StreamPredictions(request, timeout=timeout)

    def subscribe_alerts(
        self,
        *,
        module_ids: list[int] | None = None,
        data_products: list[str] | None = None,
        alert_threshold: float = 0.0,
        timeout: float | None = None,
    ) -> Iterator[ml_inference_pb2.Alert]:
        """Iterate over alert-level predictions (cloud_score >= alert_threshold)."""
        request = ml_inference_pb2.AlertRequest(
            module_ids=module_ids or [],
            data_products=data_products or [],
            alert_threshold=alert_threshold,
        )
        yield from self.stub.SubscribeAlerts(request, timeout=timeout)


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


class AioMLInferenceClient:
    """
    Async MLInference client.

    The Ray Serve deployment uses this to emit predictions::

        async with AioMLInferenceClient("localhost", 50051) as client:
            await client.emit_prediction(module_id=1, ...)
    """

    def __init__(self, host: str = "localhost", port: int = 50051) -> None:
        self._target = f"{host}:{port}"
        self._channel: grpc.aio.Channel | None = None
        self._stub: ml_inference_pb2_grpc.MLInferenceStub | None = None

    async def __aenter__(self) -> "AioMLInferenceClient":
        self._channel = grpc.aio.insecure_channel(self._target)
        self._stub = ml_inference_pb2_grpc.MLInferenceStub(self._channel)
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._channel is not None:
            await self._channel.close()

    @property
    def stub(self) -> ml_inference_pb2_grpc.MLInferenceStub:
        if self._stub is None:
            raise RuntimeError("Client not started — use as async context manager")
        return self._stub

    async def emit_prediction(
        self,
        *,
        module_id: int,
        data_product: str,
        model_name: str,
        model_version: str,
        recipe_hash: str,
        git_sha: str,
        cloud_score: float,
        cloud_label: bool,
        t_start_ns: int,
        t_end_ns: int,
        calibration_maturity: float = 1.0,
        timeout: float = 10.0,
    ) -> ml_inference_pb2.EmitAck:
        """Async emit of a scored window prediction."""
        from google.protobuf.timestamp_pb2 import Timestamp

        ts = Timestamp()
        ts.GetCurrentTime()
        prediction = ml_inference_pb2.Prediction(
            module_id=module_id,
            data_product=data_product,
            model_name=model_name,
            model_version=model_version,
            recipe_hash=recipe_hash,
            git_sha=git_sha,
            cloud_score=cloud_score,
            cloud_label=cloud_label,
            t_start_ns=t_start_ns,
            t_end_ns=t_end_ns,
            calibration_maturity=calibration_maturity,
            emitted_at=ts,
        )
        return await self.stub.EmitPrediction(prediction, timeout=timeout)

    def stream_predictions(
        self,
        *,
        module_ids: list[int] | None = None,
        data_products: list[str] | None = None,
        model_names: list[str] | None = None,
    ) -> AsyncIterator[ml_inference_pb2.Prediction]:
        """Async generator over live predictions."""
        request = ml_inference_pb2.PredictionRequest(
            module_ids=module_ids or [],
            data_products=data_products or [],
            model_names=model_names or [],
        )
        return self.stub.StreamPredictions(request)

    def subscribe_alerts(
        self,
        *,
        module_ids: list[int] | None = None,
        data_products: list[str] | None = None,
        alert_threshold: float = 0.0,
    ) -> AsyncIterator[ml_inference_pb2.Alert]:
        """Async generator over alert-level predictions."""
        request = ml_inference_pb2.AlertRequest(
            module_ids=module_ids or [],
            data_products=data_products or [],
            alert_threshold=alert_threshold,
        )
        return self.stub.SubscribeAlerts(request)
