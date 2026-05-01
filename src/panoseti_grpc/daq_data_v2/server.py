"""
DAQ Data v2 Servicer implementation.
Centralized aggregator that receives data from DAQ node forwarders
and fans it out to end-user clients.
"""

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp

from panoseti_grpc.generated import daq_data_v2_pb2, daq_data_v2_pb2_grpc
from panoseti_grpc.telemetry.logger import get_logger
from panoseti_grpc.util.error_handling import grpc_error_handler

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
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        # module_id -> {"movie": CachedImage, "ph": CachedImage}
        self.cache: dict[int, dict[str, CachedImage | None]] = defaultdict(
            lambda: {"movie": None, "ph": None}
        )
        self.frame_id_counter = 0
        self.cache_lock = asyncio.Lock()

    @grpc_error_handler
    async def UploadImages(
        self, 
        request_iterator: AsyncIterator[daq_data_v2_pb2.UploadImageRequest], 
        context: grpc.aio.ServicerContext
    ) -> Empty:
        """Receives images from forwarders and updates the central cache."""
        self.logger.info(f"New forwarder connection from {context.peer()}")
        try:
            async for request in request_iterator:
                img = request.pano_image
                if img.frame_number % 10 == 0:
                    self.logger.info(f"Received frame {img.frame_number} for module {img.module_id}")
                async with self.cache_lock:
                    self.frame_id_counter += 1
                    cached = CachedImage(self.frame_id_counter, img)
                    key = "ph" if img.type == daq_data_v2_pb2.PanoImage.Type.PULSE_HEIGHT else "movie"
                    self.cache[img.module_id][key] = cached
        except asyncio.CancelledError:
            self.logger.info(f"Forwarder {context.peer()} disconnected")
        return Empty()

    @grpc_error_handler
    async def StreamImages(
        self, 
        request: daq_data_v2_pb2.StreamImagesRequest, 
        context: grpc.aio.ServicerContext
    ) -> AsyncIterator[daq_data_v2_pb2.StreamImagesResponse]:
        """Fans out images from the cache to end-user clients."""
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
                            if not module_data: continue
                            
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
                        yield daq_data_v2_pb2.StreamImagesResponse(
                            pano_image=img,
                            timestamp=ts,
                            name="v2_aggregator"
                        )
                    
                    sub.last_update_t = now

                await asyncio.sleep(sub.update_interval / 2)
            except Exception as e:
                self.logger.error(f"Error streaming to {context.peer()}: {e}")
                break

    async def Ping(self, request: Empty, context: grpc.aio.ServicerContext) -> Empty:
        return Empty()
