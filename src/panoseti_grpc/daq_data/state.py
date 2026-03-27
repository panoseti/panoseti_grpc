
"""Dataclasses for managing DaqData server state."""
import uuid
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, List
import asyncio
import time

# Package imports
from panoseti_grpc.generated.daq_data_pb2 import PanoImage


@dataclass
class CachedPanoImage:
    """Wraps a PanoImage with a unique, server-assigned frame ID."""
    frame_id: int
    pano_image: PanoImage

@dataclass
class ReaderState:
    """Holds the state for a single client streaming RPC."""
    is_allocated: bool = False
    uid: Optional[uuid.UUID] = None
    client_ip: Optional[str] = None
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=100))
    cancel_reader_event: Optional[asyncio.Event] = None
    shutdown_event: Optional[asyncio.Event] = None

    config: Dict = field(default_factory=lambda: {
        "stream_movie_data": True,
        "stream_pulse_height_data": True,
        "update_interval_seconds": 1.0,
        "module_ids": [],
    })

    last_sent_movie_id: int = -1
    last_sent_ph_id: int = -1

    last_update_t: float = field(default_factory=time.monotonic)
    enqueue_timeouts: int = 0
    dequeue_timeouts: int = 0

    def allocate(self, client_ip: str, uid: uuid.UUID):
        self.is_allocated = True
        self.client_ip = client_ip
        self.uid = uid

    def reset(self):
        """Resets the state for reuse."""
        self.is_allocated = False
        self.client_ip = None
        self.uid = None
        self.config = {
            "stream_movie_data": True, "stream_pulse_height_data": True,
            "update_interval_seconds": 1.0, "module_ids": [],
        }
        self.last_sent_movie_id = -1
        self.last_sent_ph_id = -1
        self.enqueue_timeouts = 0
        self.dequeue_timeouts = 0
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

@dataclass
class DataProductState:
    """Configuration and state for a single data product."""
    name: str
    is_ph: bool
    pano_image_type: PanoImage.Type
    image_shape: Tuple[int, int]
    bytes_per_pixel: int
    bytes_per_image: int


class ModuleState:
    """Manages the state for a single PANOSETI module's data acquisition."""
    def __init__(self, module_id: int, logger: logging.Logger):
        self.module_id = module_id
        self.logger = logger
        self.dp_configs: Dict[str, DataProductState] = {}

    def add_dp(self, dp_name: str):
        """Adds a data product configuration discovered from the UDS stream."""
        if dp_name in self.dp_configs:
            return
        try:
            self.dp_configs[dp_name] = get_dp_config([dp_name])[dp_name]
            self.logger.info(f"Module {self.module_id}: Added config for data product '{dp_name}'")
        except ValueError as e:
            self.logger.error(f"Module {self.module_id}: Could not get config for DP '{dp_name}': {e}")


def get_dp_config(dps: List[str]) -> Dict[str, DataProductState]:
    """
    Returns a dictionary of DataProductConfig objects for the given data products.
    """
    dp_cfg = {}
    for dp in dps:
        if dp == 'img16' or dp == 'ph1024':
            image_shape = (32, 32)
            bytes_per_pixel = 2
        elif dp == 'img8':
            image_shape = (32, 32)
            bytes_per_pixel = 1
        elif dp == 'ph256':
            image_shape = (16, 16)
            bytes_per_pixel = 2
        else:
            raise ValueError(f"Unknown data product: {dp}")

        bytes_per_image = bytes_per_pixel * image_shape[0] * image_shape[1]
        is_ph = 'ph' in dp
        pano_image_type = PanoImage.Type.PULSE_HEIGHT if is_ph else PanoImage.Type.MOVIE

        dp_cfg[dp] = DataProductState(
            name=dp,
            is_ph=is_ph,
            pano_image_type=pano_image_type,
            image_shape=image_shape,
            bytes_per_pixel=bytes_per_pixel,
            bytes_per_image=bytes_per_image,
        )
    return dp_cfg
