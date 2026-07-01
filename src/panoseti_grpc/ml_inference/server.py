"""
MLInference gRPC servicer.

This is a *thin pub-sub broker* — it holds no ML state.  The Ray Serve
inference deployment (in panoseti_analysis) emits predictions via
``EmitPrediction``; connected clients receive them via ``StreamPredictions``
or ``SubscribeAlerts``.

Clean dependency direction:
  panoseti_grpc.ml_inference (shell)  →  [no panoseti_analysis import here]
  panoseti_analysis.adapters.stream   →  panoseti_grpc.ml_inference.client

ML logic stays entirely in panoseti_analysis.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

import grpc

from panoseti_grpc.generated import ml_inference_pb2, ml_inference_pb2_grpc

from .config import MLInferenceServerConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subscriber registry
# ---------------------------------------------------------------------------


class _SubscriberSet:
    """Thread-safe (asyncio) registry of active subscriber queues."""

    def __init__(self, max_subscribers: int, queue_size: int) -> None:
        self._max = max_subscribers
        self._qsize = queue_size
        self._prediction_queues: dict[int, asyncio.Queue[ml_inference_pb2.Prediction | None]] = {}
        self._alert_queues: dict[int, asyncio.Queue[ml_inference_pb2.Alert | None]] = {}
        self._next_id = 0

    def add_prediction_subscriber(self) -> tuple[int, asyncio.Queue[ml_inference_pb2.Prediction | None]]:
        if len(self._prediction_queues) >= self._max:
            raise RuntimeError(f"Max StreamPredictions subscribers ({self._max}) reached")
        sid = self._next_id
        self._next_id += 1
        q: asyncio.Queue[ml_inference_pb2.Prediction | None] = asyncio.Queue(maxsize=self._qsize)
        self._prediction_queues[sid] = q
        return sid, q

    def remove_prediction_subscriber(self, sid: int) -> None:
        self._prediction_queues.pop(sid, None)

    def add_alert_subscriber(self) -> tuple[int, asyncio.Queue[ml_inference_pb2.Alert | None]]:
        if len(self._alert_queues) >= self._max:
            raise RuntimeError(f"Max SubscribeAlerts subscribers ({self._max}) reached")
        sid = self._next_id
        self._next_id += 1
        q: asyncio.Queue[ml_inference_pb2.Alert | None] = asyncio.Queue(maxsize=self._qsize)
        self._alert_queues[sid] = q
        return sid, q

    def remove_alert_subscriber(self, sid: int) -> None:
        self._alert_queues.pop(sid, None)

    def broadcast_prediction(self, prediction: ml_inference_pb2.Prediction) -> int:
        """Fan prediction out to all StreamPredictions queues. Returns delivered count."""
        count = 0
        for sid, q in list(self._prediction_queues.items()):
            try:
                q.put_nowait(prediction)
                count += 1
            except asyncio.QueueFull:
                # Slow subscriber — drop oldest item to make room.
                try:
                    q.get_nowait()
                    q.put_nowait(prediction)
                    count += 1
                    logger.warning("Prediction queue full for subscriber %d; dropped oldest item", sid)
                except asyncio.QueueEmpty:
                    pass
        return count

    def broadcast_alert(self, alert: ml_inference_pb2.Alert) -> None:
        """Fan alert out to all SubscribeAlerts queues."""
        for sid, q in list(self._alert_queues.items()):
            try:
                q.put_nowait(alert)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(alert)
                    logger.warning("Alert queue full for subscriber %d; dropped oldest item", sid)
                except asyncio.QueueEmpty:
                    pass


# ---------------------------------------------------------------------------
# MLInference servicer
# ---------------------------------------------------------------------------


def _matches_prediction_filter(
    prediction: ml_inference_pb2.Prediction,
    request: ml_inference_pb2.PredictionRequest,
) -> bool:
    """Return True if prediction passes all PredictionRequest filters."""
    if request.module_ids and prediction.module_id not in request.module_ids:
        return False
    if request.data_products and prediction.data_product not in request.data_products:
        return False
    return not (request.model_names and prediction.model_name not in request.model_names)


def _matches_alert_filter(
    prediction: ml_inference_pb2.Prediction,
    request: ml_inference_pb2.AlertRequest,
    default_threshold: float,
) -> bool:
    """Return True if prediction should trigger an alert."""
    threshold = request.alert_threshold if request.alert_threshold > 0.0 else default_threshold
    if prediction.cloud_score < threshold:
        return False
    if request.module_ids and prediction.module_id not in request.module_ids:
        return False
    return not (request.data_products and prediction.data_product not in request.data_products)


class MLInferenceServicer(ml_inference_pb2_grpc.MLInferenceServicer):
    """
    Pub-sub broker for real-time ML predictions.

    The servicer holds *no ML state* — it only routes ``Prediction`` messages
    emitted by the panoseti_analysis Ray Serve deployment to subscribed clients.
    """

    def __init__(self, cfg: MLInferenceServerConfig) -> None:
        self._cfg = cfg
        self._subs = _SubscriberSet(cfg.max_subscribers, cfg.subscriber_queue_size)

    async def EmitPrediction(
        self,
        request: ml_inference_pb2.Prediction,
        context: grpc.aio.ServicerContext,
    ) -> ml_inference_pb2.EmitAck:
        """
        Receive one scored window from the Ray Serve accumulator and fan it
        out to all active StreamPredictions / SubscribeAlerts subscribers.
        """
        n = self._subs.broadcast_prediction(request)

        # Also check alert subscribers.
        if _matches_alert_filter(request, ml_inference_pb2.AlertRequest(), self._cfg.alert_threshold):
            alert = ml_inference_pb2.Alert(
                prediction=request,
                description=(
                    f"Cloud detected: module={request.module_id} dp={request.data_product} "
                    f"score={request.cloud_score:.3f} model={request.model_name}@{request.model_version}"
                ),
            )
            self._subs.broadcast_alert(alert)

        logger.debug(
            "EmitPrediction: module=%d dp=%s score=%.3f → %d prediction subscribers",
            request.module_id,
            request.data_product,
            request.cloud_score,
            n,
        )
        return ml_inference_pb2.EmitAck(success=True, subscriber_count=n)

    async def StreamPredictions(
        self,
        request: ml_inference_pb2.PredictionRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[ml_inference_pb2.Prediction]:
        """Server-streaming: yield each new prediction that matches the filter."""
        try:
            sid, q = self._subs.add_prediction_subscriber()
        except RuntimeError as exc:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, str(exc))
            return

        logger.info("StreamPredictions subscriber %d connected", sid)
        try:
            while True:
                if await context.is_active() is False:
                    break
                try:
                    prediction = await asyncio.wait_for(q.get(), timeout=5.0)
                except TimeoutError:
                    continue
                if prediction is None:
                    break
                if _matches_prediction_filter(prediction, request):
                    yield prediction
        finally:
            self._subs.remove_prediction_subscriber(sid)
            logger.info("StreamPredictions subscriber %d disconnected", sid)

    async def SubscribeAlerts(
        self,
        request: ml_inference_pb2.AlertRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[ml_inference_pb2.Alert]:
        """Server-streaming: yield an Alert whenever cloud_score >= alert_threshold."""
        try:
            sid, q = self._subs.add_alert_subscriber()
        except RuntimeError as exc:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, str(exc))
            return

        logger.info("SubscribeAlerts subscriber %d connected (threshold=%.2f)", sid, request.alert_threshold)
        try:
            while True:
                if await context.is_active() is False:
                    break
                try:
                    alert = await asyncio.wait_for(q.get(), timeout=5.0)
                except TimeoutError:
                    continue
                if alert is None:
                    break
                # Re-check threshold in case the subscriber used a different value than default.
                if _matches_alert_filter(alert.prediction, request, self._cfg.alert_threshold):
                    yield alert
        finally:
            self._subs.remove_alert_subscriber(sid)
            logger.info("SubscribeAlerts subscriber %d disconnected", sid)
