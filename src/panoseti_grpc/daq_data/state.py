"""Dataclasses and enums for managing DaqData server state."""
from __future__ import annotations
import uuid
import logging
from dataclasses import dataclass, field
from enum import Enum
import time

# Package imports
from panoseti_grpc.generated.daq_data_pb2 import PanoImage


class DataProduct(str, Enum):
    """Canonical data product definitions. Inherits from str so values compare equal to their string names."""
    IMG16  = "img16"
    IMG8   = "img8"
    PH256  = "ph256"
    PH1024 = "ph1024"

    @property
    def image_shape(self) -> tuple[int, int]:
        return (16, 16) if self == DataProduct.PH256 else (32, 32)

    @property
    def bytes_per_pixel(self) -> int:
        return 1 if self == DataProduct.IMG8 else 2

    @property
    def is_ph(self) -> bool:
        return self in (DataProduct.PH256, DataProduct.PH1024)

    @property
    def pano_image_type(self) -> "PanoImage.Type":
        return PanoImage.Type.PULSE_HEIGHT if self.is_ph else PanoImage.Type.MOVIE

    @property
    def bytes_per_image(self) -> int:
        r, c = self.image_shape
        return r * c * self.bytes_per_pixel


@dataclass
class CachedPanoImage:
    """Wraps a PanoImage with a unique, server-assigned frame ID."""
    frame_id: int
    pano_image: PanoImage


@dataclass
class ReaderState:
    """Holds the state for a single client streaming RPC."""
    is_allocated: bool = False
    uid: uuid.UUID | None = None
    client_ip: str | None = None
    cancel_reader_event: asyncio.Event | None = None
    shutdown_event: asyncio.Event | None = None

    config: dict = field(default_factory=lambda: {
        "stream_movie_data": True,
        "stream_pulse_height_data": True,
        "update_interval_seconds": 1.0,
        "module_ids": [],
    })

    last_sent_movie_id: int = -1
    last_sent_ph_id: int = -1
    last_update_t: float = field(default_factory=time.monotonic)
    dequeue_timeouts: int = 0

    def reset(self):
        """Resets the state for reuse by the next client."""
        self.is_allocated = False
        self.client_ip = None
        self.uid = None
        self.config = {
            "stream_movie_data": True, "stream_pulse_height_data": True,
            "update_interval_seconds": 1.0, "module_ids": [],
        }
        self.last_sent_movie_id = -1
        self.last_sent_ph_id = -1
        self.last_update_t = time.monotonic()
        self.dequeue_timeouts = 0


@dataclass
class DataProductState:
    """Configuration for a single data product."""
    name: str
    is_ph: bool
    pano_image_type: "PanoImage.Type"
    image_shape: tuple[int, int]
    bytes_per_pixel: int
    bytes_per_image: int


class ModuleState:
    """Manages the state for a single PANOSETI module's data acquisition."""
    def __init__(self, module_id: int, logger: logging.Logger):
        self.module_id = module_id
        self.logger = logger
        self.dp_configs: dict[str, DataProductState] = {}

    def add_dp(self, dp_name: str):
        """Adds a data product configuration discovered from the UDS stream."""
        if dp_name in self.dp_configs:
            return
        try:
            self.dp_configs[dp_name] = get_dp_config([dp_name])[dp_name]
            self.logger.info(f"Module {self.module_id}: Added config for data product '{dp_name}'")
        except ValueError as e:
            self.logger.error(f"Module {self.module_id}: Could not get config for DP '{dp_name}': {e}")


def get_dp_config(dps: list[str]) -> dict[str, DataProductState]:
    """Returns DataProductState objects for the given data product names. Raises ValueError on unknown names."""
    dp_cfg = {}
    for dp in dps:
        dp_enum = DataProduct(dp)  # raises ValueError on unknown name
        dp_cfg[dp] = DataProductState(
            name=dp,
            is_ph=dp_enum.is_ph,
            pano_image_type=dp_enum.pano_image_type,
            image_shape=dp_enum.image_shape,
            bytes_per_pixel=dp_enum.bytes_per_pixel,
            bytes_per_image=dp_enum.bytes_per_image,
        )
    return dp_cfg
