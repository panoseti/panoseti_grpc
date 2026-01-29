
"""Dataclasses for managing DaqData server state."""
import uuid
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, IO, List
import asyncio
import time
from pathlib import Path

# Package imports
from panoseti_grpc.generated.daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest
from panoseti_grpc.panoseti_util import pff

from .resources import _parse_dp_name, _parse_seqno

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
    frame_size: int = 0
    glob_pat: str = ""
    last_known_filesize: int = 0
    current_filepath: Optional[Path] = None
    f: Optional[IO[bytes]] = None  # Cached file handle
    last_frame_idx: int = -1  # Index of the last successfully read frame
    last_seqno: int = -1


class ModuleState:
    """Manages the state for a single PANOSETI module's data acquisition."""
    def __init__(self, module_id: int, data_dir: Path, logger: logging.Logger):
        self.module_id = module_id
        self.data_dir = data_dir
        self.run_path: Optional[Path] = None
        self.logger = logger
        self.dp_configs: Dict[str, DataProductState] = {}

    async def discover_and_initialize_from_fs(self, timeout: float = 2.0) -> bool:
        """Finds the active run directory and initializes all discoverable data products."""
        module_path = self.data_dir / f"module_{self.module_id}"
        
        # First, check if the module directory exists.
        if not await asyncio.to_thread(module_path.is_dir):
             self.logger.warning(f'Module directory does not exist: {module_path}')
             return False
        
        self.logger.debug(f"Searching for active run in: {module_path}")
        run_paths = [p for p in list(await asyncio.to_thread(module_path.glob, "obs_*")) if p.is_dir()]
        
        if not run_paths:
            self.logger.warning(f'No run directory found for module {self.module_id} in {module_path}')
            return False

        self.run_path = max(run_paths, key=lambda p: p.stat().st_mtime)
        self.logger.info(f"Module {self.module_id}: Found active run at {self.run_path}")

        pff_files = list(self.run_path.glob('*.pff'))
        discovered_dp_names = set()
        for f in pff_files:
            try:
                discovered_dp_names.add(_parse_dp_name(f.name))
            except ValueError:
                continue
        
        if not discovered_dp_names:
            return True

        self.logger.info(f"Module {self.module_id}: Discovered data products: {discovered_dp_names}")
        results = await asyncio.gather(*(self.add_dp_from_fs(dp_name, timeout) for dp_name in discovered_dp_names))
        return any(results)

    async def add_dp_from_fs(self, dp_name: str, timeout: float = 1.0) -> bool:
        if dp_name in self.dp_configs: return True
        try:
            dp_config = get_dp_config([dp_name])[dp_name]
            if await self._initialize_dp(dp_config, timeout):
                self.dp_configs[dp_name] = dp_config
                self.logger.info(f"Module {self.module_id}: Successfully initialized data product '{dp_name}'")
                return True
        except ValueError as e:
            self.logger.error(f"Module {self.module_id}: Could not get config for '{dp_name}': {e}")
        return False
        
    def add_dp_for_upload(self, dp_name: str):
        """Adds a data product configuration for data received via upload."""
        if dp_name in self.dp_configs: return
        try:
            self.dp_configs[dp_name] = get_dp_config([dp_name])[dp_name]
            self.logger.info(f"Module {self.module_id}: Added config for uploaded data product '{dp_name}'")
        except ValueError as e:
            self.logger.error(f"Module {self.module_id}: Could not get config for uploaded DP '{dp_name}': {e}")
            
    async def _initialize_dp(self, dp_config: DataProductState, timeout: float) -> bool:
        """Initializes state for a data product by inspecting its files."""
        if not self.run_path: return False
        start_time = time.monotonic()
        glob_pat = self.run_path / f'*{dp_config.name}*.pff'
        while time.monotonic() - start_time < timeout:
            files = list(glob_pat.parent.glob(glob_pat.name))
            if not files:
                await asyncio.sleep(0.25)
                continue
            
            latest_file = max(files, key=lambda p: os.path.getmtime(p))
            curr_size = 0
            try:
                curr_size = await asyncio.to_thread(os.path.getsize, latest_file)
                if curr_size >= dp_config.bytes_per_image:
                    with open(latest_file, 'rb') as f:
                        dp_config.frame_size = pff.img_frame_size(f, dp_config.bytes_per_image)
                    dp_config.current_filepath = latest_file
                    dp_config.last_known_filesize = await asyncio.to_thread(os.path.getsize, latest_file)
                    dp_config.last_seqno = _parse_seqno(latest_file.name)
                    return True
            except (FileNotFoundError, ValueError, Exception) as e:
                self.logger.warning(f"Failed to initialize {dp_config.name} for module {self.module_id}: {e}")
                return False
            self.logger.warning(f"File {latest_file} has size {curr_size} which is too small to be a valid {dp_config.name} frame.")
            await asyncio.sleep(0.25)

        self.logger.warning(f"Timeout initializing data product {dp_config.name} for module {self.module_id}")
        return False


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

        # Directly instantiate the dataclass instead of creating a dictionary
        dp_cfg[dp] = DataProductState(
            name=dp,
            is_ph=is_ph,
            pano_image_type=pano_image_type,
            image_shape=image_shape,
            bytes_per_pixel=bytes_per_pixel,
            bytes_per_image=bytes_per_image,
        )
    return dp_cfg
