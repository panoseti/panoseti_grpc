
"""Dataclasses for managing DaqData server state."""

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
import asyncio
import time
from pathlib import Path
from daq_data.daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest

@dataclass
class ReaderState:
    """Holds the state for a single client streaming RPC."""
    is_allocated: bool = False
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=100))  # Example size
    client_ip: str or None = None
    cancel_reader_event: asyncio.Event = None
    shutdown_event: asyncio.Event = None

    # Configuration for the stream
    config: Dict = field(default_factory=lambda: {
        "stream_movie_data": True,
        "stream_pulse_height_data": True,
        "update_interval_seconds": 1.0,
        "module_ids": [],
    })

    # Counters for tracking health
    last_update_t: float = field(default_factory=time.monotonic)
    enqueue_timeouts: int = 0
    dequeue_timeouts: int = 0

    def reset(self):
        """Resets the state for reuse, keeping the queue object."""
        self.is_allocated = False
        self.client_ip = None
        self.config = {
            "stream_movie_data": True,
            "stream_pulse_height_data": True,
            "update_interval_seconds": 1.0,
            "module_ids": [],
        }
        self.enqueue_timeouts = 0
        self.dequeue_timeouts = 0
        # Clear any stale data from the queue
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

@dataclass
class DataProductConfig:
    """Configuration and state for a single data product."""
    name: str
    is_ph: bool
    pano_image_type: PanoImage.Type
    image_shape: Tuple[int, int]
    bytes_per_pixel: int
    bytes_per_image: int
    frame_size: int = 0
    glob_pat: str = ""
    last_known_filesize: int = 0
    current_filepath: Optional[Path] = None

