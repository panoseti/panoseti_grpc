"""
DAQ Data v2 Servicer implementation.
Centralized aggregator that receives data from DAQ node forwarders
and fans it out to end-user clients.

Supports two modes:
1. aggregator: Runs the centralized cache and serves clients.
2. forwarder: Runs the background sidecar to push local data.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Coroutine
from dataclasses import dataclass, field
from typing import Any

import grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp

from panoseti_grpc.generated import daq_data_v2_pb2, daq_data_v2_pb2_grpc
from panoseti_grpc.util.error_handling import grpc_error_handler

from .config import DaqDataV2ServerConfig


@dataclass
class CachedImage:
    frame_id: int
    pano_image: daq_data_v2_pb2.PanoImage


@dataclass
class ClientSubscription:
    module_ids: set[int]
    stream_movie: bool
    stream_ph: bool
    update_interval: float
    last_movie_id: int = -1
    last_ph_id: int = -1
    last_update_t: float = field(default_factory=time.monotonic)


class DaqDataV2Servicer(daq_data_v2_pb2_grpc.DaqDataV2Servicer):
    def __init__(self, cfg: DaqDataV2ServerConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logger
        # module_id -> {"movie": CachedImage, "ph": CachedImage}
        self.cache: dict[int, dict[str, CachedImage | None]] = defaultdict(lambda: {"movie": None, "ph": None})
        self.frame_id_counter = 0
        self.cache_lock = asyncio.Lock()
        self.forwarder: Any | None = None

    def start_initial_task(self) -> Coroutine[Any, Any, None] | None:
        """Starts the forwarder task if configured in forwarder mode."""
        if self.cfg.mode == "forwarder":
            from .forwarder import Forwarder

            self.logger.info("Starting DaqDataV2 in FORWARDER mode")
            self.forwarder = Forwarder(self.cfg, self.logger)
            return self.forwarder.run()
        else:
            self.logger.info("Starting DaqDataV2 in AGGREGATOR mode")
            return None

    async def shutdown(self) -> None:
        """Gracefully stop the forwarder if it's running."""
        if self.forwarder:
            self.logger.info("Stopping DaqDataV2 Forwarder task")
            self.forwarder.stop_event.set()

    @grpc_error_handler
    async def UploadImages(
        self, request_iterator: AsyncIterator[daq_data_v2_pb2.UploadImageRequest], context: grpc.aio.ServicerContext
    ) -> Empty:
        """Receives images from forwarders and updates the central cache."""
        if self.cfg.mode != "aggregator":
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Server is not in aggregator mode")

        peer = context.peer().replace("[", "(").replace("]", ")")
        self.logger.info(f"UploadImages called from {peer}")
        count = 0
        try:
            async for request in request_iterator:
                img = request.pano_image
                count += 1
                if count == 1 or count % 100 == 0:
                    self.logger.info(f"Received frame {img.frame_number} from {peer}. Total frames: {count}")

                async with self.cache_lock:
                    self.frame_id_counter += 1
                    cached = CachedImage(self.frame_id_counter, img)
                    # Support mapping to PH or Movie cache
                    key = "ph" if img.type == daq_data_v2_pb2.PanoImage.Type.PULSE_HEIGHT else "movie"
                    self.cache[img.module_id][key] = cached

            self.logger.info(f"Forwarder {peer} stream ended normally after {count} frames")
        except asyncio.CancelledError:
            self.logger.info(f"Forwarder {peer} stream cancelled after {count} frames")
        except Exception as e:
            self.logger.error(f"Error in UploadImages from {peer}: {e}")
            raise
        return Empty()

    @grpc_error_handler
    async def StreamImages(
        self, request: daq_data_v2_pb2.StreamImagesRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[daq_data_v2_pb2.StreamImagesResponse]:
        """Fans out images from the cache to end-user clients."""
        if self.cfg.mode != "aggregator":
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Server is not in aggregator mode")

        self.logger.info(f"New client subscription from {context.peer()}")

        sub = ClientSubscription(
            module_ids=set(request.module_ids),
            stream_movie=request.stream_movie_data,
            stream_ph=request.stream_pulse_height_data,
            update_interval=max(0.001, request.update_interval_seconds),
        )

        while not context.cancelled():
            try:
                now = time.monotonic()
                if now - sub.last_update_t >= sub.update_interval:
                    images_to_send = []

                    async with self.cache_lock:
                        target_modules = sub.module_ids if sub.module_ids else self.cache.keys()
                        for mid in target_modules:
                            module_data = self.cache.get(mid)
                            if not module_data:
                                continue

                            if sub.stream_movie:
                                cached = module_data["movie"]
                                if cached and cached.frame_id > sub.last_movie_id:
                                    images_to_send.append(cached.pano_image)
                                    sub.last_movie_id = cached.frame_id

                            if sub.stream_ph:
                                cached = module_data["ph"]
                                if cached and cached.frame_id > sub.last_ph_id:
                                    images_to_send.append(cached.pano_image)
                                    sub.last_ph_id = cached.frame_id

                    for img in images_to_send:
                        ts = Timestamp()
                        ts.GetCurrentTime()
                        yield daq_data_v2_pb2.StreamImagesResponse(pano_image=img, timestamp=ts, name="v2_aggregator")

                    sub.last_update_t = now

                await asyncio.sleep(sub.update_interval / 2)
            except Exception as e:
                self.logger.error(f"Error streaming to {context.peer()}: {e}")
                break

    async def Ping(self, request: Empty, context: grpc.aio.ServicerContext) -> Empty:
        return Empty()
