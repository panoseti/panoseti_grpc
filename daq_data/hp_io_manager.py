"""
Orchestrates filesystem monitoring and data broadcasting for PANOSETI DAQ.

There are two primary data paths from hashpipe:
    1a. Filesystem snapshot monitoring
    1b. Named pipe inter-process communication between Hashpipe and gRPC
    2. the UploadImages RPC for bypassing the filesystem entirely. Requires a Hashpipe C++ gRPC client (not currently supported).

Creates snapshots of active run directories for each module directory. Assumes the following structure:
    data_dir/
        ├── module_1/
        │   ├── obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd
        │   │   ├── start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.pff
        │   │   ├── start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_1.pff
        │   │   ...
        │   │
        │   ├── obs_*/
        │   │   ├──
        │   │   ...
        │   ...
        │
        ├── module_2/
        │   └── obs_*/
        │       ...
        │
        └── module_N/
            └── obs_*/
"""
import asyncio
import json
import logging
import os
import re
import time
import stat
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
import select

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict
from watchfiles import awatch

from .daq_data_pb2 import PanoImage
from .resources import get_dp_config, get_dp_name_from_props, is_daq_active, _parse_dp_name, _parse_seqno
from .state import ReaderState, DataProductConfig, CachedPanoImage
from .data_sources import UdsDataSource, PollWatcherDataSource, PipeWatcherDataSource
from panoseti_util import pff


class ModuleState:
    """Manages the state for a single PANOSETI module's data acquisition."""
    def __init__(self, module_id: int, data_dir: Path, logger: logging.Logger):
        self.module_id = module_id
        self.data_dir = data_dir
        self.logger = logger
        self.run_path: Path
        self.dp_configs: Dict[str, DataProductConfig] = {}

    async def discover_and_initialize_from_fs(self, timeout: float = 2.0) -> bool:
        """Finds the active run directory and initializes all discoverable data products."""
        run_pattern = self.data_dir / f"module_{self.module_id}" / "obs_*"
        runs = await asyncio.to_thread(os.listdir, self.data_dir / f"module_{self.module_id}")
        run_paths = [self.data_dir / f"module_{self.module_id}" / r for r in runs if r.startswith("obs_")]
        if not run_paths:
            self.logger.warning(f'No run directory found for module {self.module_id}')
            return False
        self.run_path = Path(sorted(run_paths, key=os.path.getmtime)[-1])
        self.logger.info(f"Module {self.module_id}: Found active run at {self.run_path}")
        pff_files = list(self.run_path.glob('*.pff'))
        discovered_dp_names = set(_parse_dp_name(f.name) for f in pff_files if _parse_dp_name(f.name))
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
        if dp_name in self.dp_configs: return
        try:
            self.dp_configs[dp_name] = get_dp_config([dp_name])[dp_name]
            self.logger.info(f"Module {self.module_id}: Added config for uploaded data product '{dp_name}'")
        except ValueError as e:
            self.logger.error(f"Module {self.module_id}: Could not get config for uploaded DP '{dp_name}': {e}")
            
    async def _initialize_dp(self, dp_config: DataProductConfig, timeout: float) -> bool:
        start_time = time.monotonic()
        glob_pat = self.run_path / f'*{dp_config.name}*.pff'
        while time.monotonic() - start_time < timeout:
            files = list(glob_pat.parent.glob(glob_pat.name))
            if not files:
                await asyncio.sleep(0.25)
                continue
            latest_file = Path(sorted(files, key=os.path.getmtime)[-1])
            try:
                if os.path.getsize(latest_file) >= dp_config.bytes_per_image:
                    with open(latest_file, 'rb') as f:
                        dp_config.frame_size = pff.img_frame_size(f, dp_config.bytes_per_image)
                    dp_config.current_filepath = latest_file
                    dp_config.last_known_filesize = os.path.getsize(latest_file)
                    dp_config.last_seqno = _parse_seqno(latest_file.name)
                    return True
            except (FileNotFoundError, ValueError, Exception) as e:
                self.logger.warning(f"Failed to initialize {dp_config.name} for module {self.module_id}: {e}")
                return False
            await asyncio.sleep(0.25)
        self.logger.warning(f"Timeout initializing data product {dp_config.name} for module {self.module_id}")
        return False


class HpIoManager:
    """Orchestrates data acquisition from multiple sources and broadcasts to clients."""

    def __init__(self, server_config: Dict, reader_states: List[ReaderState], stop_io: asyncio.Event, valid: asyncio.Event,
                 active_data_products_queue: asyncio.Queue, logger: logging.Logger):
        self.server_config = server_config
        self.reader_states = reader_states
        self.stop_io = stop_io
        self.valid = valid
        self.active_data_products_queue = active_data_products_queue
        self.logger = logger
        
        self.data_queue = asyncio.Queue(maxsize=500)
        self.data_sources = []

        # State management
        self.hp_io_cfg = server_config['hp_io_cfg']
        self.data_dir = Path(self.hp_io_cfg['data_dir'])
        self.update_interval_seconds = self.hp_io_cfg['update_interval_seconds']
        self.simulate_daq = self.hp_io_cfg['simulate_daq']
        self.read_status_pipe_name = server_config['read_status_pipe_name']
        self.modules: Dict[int, ModuleState] = {}
        self.module_id_re = re.compile(r'module_(\d+)')
        self.latest_data_cache: Dict[int, Dict[str, CachedPanoImage]] = defaultdict(lambda: {'ph': None, 'movie': None})
        self._frame_id_counter = 0

        self._configure_data_sources()

    def _configure_data_sources(self):
        """Instantiates data sources based on server configuration."""
        acq_config = self.server_config.get("acquisition_methods", {})
        self.logger.info(f"Configuring data sources: {acq_config}")

        # UDS Data Source
        uds_cfg = acq_config.get("uds", {})
        if uds_cfg.get("enabled"):
            for dp_name in uds_cfg.get("data_products", []):
                source_cfg = {"dp_name": dp_name, "module_id": 0}  # Module ID is fixed for UDS
                self.data_sources.append(UdsDataSource(source_cfg, self.logger, self.data_queue, self.stop_io))

        # Filesystem Polling Data Source
        poll_cfg = acq_config.get("filesystem_poll", {})
        if poll_cfg.get("enabled"):
            self.data_sources.append(PollWatcherDataSource(self, poll_cfg, self.logger, self.data_queue, self.stop_io))
        
        # Filesystem Pipe-based Data Source
        pipe_cfg = acq_config.get("filesystem_pipe", {})
        if pipe_cfg.get("enabled"):
            self.data_sources.append(PipeWatcherDataSource(self, pipe_cfg, self.logger, self.data_queue, self.stop_io))

    async def run(self):
        """Main entry point: starts data sources and the processing loop."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()
        
        is_fs_mode = any(isinstance(s, (PollWatcherDataSource, PipeWatcherDataSource)) for s in self.data_sources)
        if is_fs_mode:
            if not await self._initialize_modules_from_fs():
                 self.logger.warning("HpIoManager did not find any modules on filesystem at startup.")

        if not self.data_sources and not self.simulate_daq:
            self.logger.error("No data acquisition sources configured. HpIoManager cannot run.")
            return

        source_tasks = [asyncio.create_task(source.run()) for source in self.data_sources]
        processing_task = asyncio.create_task(self._processing_loop())

        await self._update_active_data_products()
        self.valid.set()
        
        try:
            await asyncio.gather(processing_task, *source_tasks)
        except Exception as e:
            self.logger.error(f"HpIoManager run error: {e}", exc_info=True)
        finally:
            self.valid.clear()
            self.logger.info("HpIoManager task exited.")


    async def _processing_loop(self):
        """
        ** FIX: Assigns a unique frame_id to each incoming image before caching. **
        """
        self.logger.info("Starting freshness-aware processing loop.")
        while not self.stop_io.is_set():
            try:
                pano_image = await self.data_queue.get()
                
                await self._discover_module_from_image(pano_image)
                
                # Assign a new ID and cache the wrapped image object
                self._frame_id_counter += 1
                cached_image = CachedPanoImage(
                    frame_id=self._frame_id_counter,
                    pano_image=pano_image
                )
                await self._cache_pano_image(cached_image)
                
                self.data_queue.task_done()
            except asyncio.CancelledError:
                break
        self.logger.info("Freshness-aware processing loop finished.")

    async def _cache_pano_image(self, cached_image: CachedPanoImage):
        """Caches the received CachedPanoImage, overwriting the previous one."""
        pano_image = cached_image.pano_image
        is_ph = (pano_image.type == PanoImage.Type.PULSE_HEIGHT)
        cache_key = 'ph' if is_ph else 'movie'
        self.latest_data_cache[pano_image.module_id][cache_key] = cached_image

    async def enqueue_uploaded_image(self, pano_image: PanoImage):
        """Public method for the UploadImages RPC to add an image to the queue."""
        try:
            self.data_queue.put_nowait(pano_image)
        except asyncio.QueueFull:
            self.logger.warning("Data queue is full. Dropping uploaded image.")

    async def _handle_received_image(self, pano_image: PanoImage):
        """Unified handler for any PanoImage, regardless of source."""
        await self._discover_module_from_image(pano_image)
        await self._cache_pano_image(pano_image)
        self._broadcast_if_ready()

    async def _discover_module_from_image(self, pano_image: PanoImage):
        """Discovers a new module or data product from a received image."""
        module_id = pano_image.module_id
        if module_id not in self.modules:
            self.logger.info(f"Discovered new module {module_id} via data stream.")
            self.modules[module_id] = ModuleState(module_id, self.data_dir, self.logger)
        
        module = self.modules[module_id]
        try:
            dp_name = get_dp_name_from_props(pano_image.type, list(pano_image.shape), pano_image.bytes_per_pixel)
            if dp_name not in module.dp_configs:
                module.add_dp_for_upload(dp_name)
                await self._update_active_data_products()
        except ValueError as e:
            self.logger.warning(f"Could not identify data product from image for module {module_id}: {e}")

    def _broadcast_if_ready(self):
        """Checks reader states and broadcasts data if they are ready."""
        now = time.monotonic()
        ready_readers = [rs for rs in self.reader_states if rs.is_allocated and (now - rs.last_update_t) >= rs.config['update_interval_seconds']]
        if ready_readers:
            self._broadcast_data(ready_readers, now)


    async def discover_new_module(self, module_id: int) -> bool:
        """Utility to discover and initialize a single new module."""
        if module_id in self.modules: return True
        self.logger.info(f"Discovering new module from filesystem: {module_id}")
        module = ModuleState(module_id, self.data_dir, self.logger)
        if await module.discover_and_initialize_from_fs():
            self.modules[module_id] = module
            await self._update_active_data_products()
            return True
        return False
        
    async def fetch_latest_frame_from_file(self, filepath: Path, dp_config: DataProductConfig) -> Tuple[Optional[dict], Optional[bytes], int]:
        """Reads the last complete frame from a PFF file."""
        try:
            with open(filepath, 'rb') as f:
                current_size = os.fstat(f.fileno()).st_size
                if current_size < dp_config.bytes_per_image or dp_config.frame_size == 0:
                    return None, None, -1
                
                nframes = current_size // dp_config.frame_size
                if nframes == 0:
                    return None, None, -1
                
                new_frame_idx = nframes - 1
                f.seek(new_frame_idx * dp_config.frame_size)
                header_str = pff.read_json(f)
                img = pff.read_image(f, dp_config.image_shape[0], dp_config.bytes_per_pixel)
                
                return (json.loads(header_str), img, new_frame_idx) if header_str and img is not None else (None, None, -1)
        except (IOError, ValueError, FileNotFoundError) as e:
            self.logger.warning(f"Could not read latest frame from {filepath}: {e}")
            return None, None, -1
            
    async def _update_active_data_products(self):
        active_dps = set().union(*(m.dp_configs.keys() for m in self.modules.values()))
        await self.active_data_products_queue.put(active_dps)
    
    async def _initialize_modules_from_fs(self) -> bool:
        """Initial discovery of modules and data products from the filesystem."""
        if not await is_daq_active(self.simulate_daq, self.server_config.get('simulate_daq_cfg'), retries=2, delay=0.5):
            self.logger.warning("DAQ data flow not active, filesystem scan may be incomplete.")
        
        module_dirs = [p for p in self.data_dir.glob("module_*") if p.is_dir()]
        all_module_ids = [int(p.name.split('_')[1]) for p in module_dirs if p.name.split('_')[1].isdigit()]
        if not all_module_ids: return False

        for mid in all_module_ids:
            await self.discover_new_module(mid)
        return len(self.modules) > 0
        
    def _broadcast_data(self, ready_readers, now):
        """Broadcasts cached data to ready clients."""
        for mid in list(self.latest_data_cache.keys()):
            data = self.latest_data_cache[mid]
            ph_image = data.get('ph')
            movie_image = data.get('movie')

            if ph_image:
                for rs in ready_readers:
                    if rs.config['stream_pulse_height_data'] and (not rs.config['module_ids'] or mid in rs.config['module_ids']):
                        try:
                            rs.queue.put_nowait(ph_image)
                            rs.enqueue_timeouts = 0
                        except asyncio.QueueFull:
                            rs.enqueue_timeouts += 1
                            if rs.enqueue_timeouts % 10 == 1: # Log less frequently
                                self.logger.warning(f"Reader queue full for {rs.client_ip} (PH). Timeout count: {rs.enqueue_timeouts}")
                self.latest_data_cache[mid]['ph'] = None

            if movie_image:
                for rs in ready_readers:
                    if rs.config['stream_movie_data'] and (not rs.config['module_ids'] or mid in rs.config['module_ids']):
                        try:
                            rs.queue.put_nowait(movie_image)
                            rs.enqueue_timeouts = 0
                        except asyncio.QueueFull:
                            rs.enqueue_timeouts += 1
                            if rs.enqueue_timeouts % 10 == 1:
                                self.logger.warning(f"Reader queue full for {rs.client_ip} (Movie). Timeout count: {rs.enqueue_timeouts}")
                self.latest_data_cache[mid]['movie'] = None

        for rs in ready_readers:
            rs.last_update_t = now
