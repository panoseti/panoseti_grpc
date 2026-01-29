"""
Orchestrates filesystem monitoring and data broadcasting for PANOSETI DAQ.

There are 2 primary data paths from Hashpipe, from which the server can receive data:

1. Socket-based: Hashpipe sends data directly to the server via Unix-Domain Sockets (UDS) or the UploadImages RPC.
2. Filesystem-based:
    a. Hashpipe signals the server via a pipe when new data is available.
    b. The server polls the filesystem for new data files and computes the latest frames.

The Filesystem tasks create snapshots of active run directories for each module directory and assume the following structure:
    data_dir/
        ├── module_1/
        │   ├── obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd
        │   │   ├── start_2024-07-25T04_34_46Z.dp_ph{256, 1025}.bpp_2.module_1.seqno_*.pff
        │   │   ├── start_2024-07-25T04_34_46Z.dp_img{8, 16}.bpp_2.module_1.seqno_*.pff
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
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

from panoseti_grpc.generated.daq_data_pb2 import PanoImage
from panoseti_grpc.panoseti_util import pff

from .resources import get_dp_name_from_props, is_daq_active
from .state import ReaderState, DataProductState, CachedPanoImage, ModuleState
from .data_sources import UdsDataSource, PollWatcherDataSource, PipeWatcherDataSource


class HpIoManager:
    """Orchestrates data acquisition from multiple sources and broadcasts to clients."""

    def __init__(self, server_config: Dict, reader_states: List[ReaderState], stop_event: asyncio.Event, valid: asyncio.Event,
                 active_data_products_queue: asyncio.Queue, logger: logging.Logger):
        self.server_config = server_config
        self.reader_states = reader_states
        self.stop_event = stop_event
        self.valid = valid
        self.active_data_products_queue = active_data_products_queue
        self.logger = logger
        self.processing_loop_timeout = 0.75
        
        self.data_queue = asyncio.Queue(maxsize=500)
        self.data_sources = []

        # State management
        self.hp_io_cfg = server_config['hp_io_cfg']
        self.data_dir = Path(self.hp_io_cfg['data_dir'])
        self.logger.info(f"HpIoManager data directory set to: {self.data_dir}")
        self.update_interval_seconds = self.hp_io_cfg['update_interval_seconds']
        self.simulate_daq = self.hp_io_cfg['simulate_daq']
        self.read_status_pipe_name = server_config['read_status_pipe_name']
        self.modules: Dict[int, ModuleState] = {}
        self.module_id_re = re.compile(r'module_(\d+)')
        self.latest_data_cache: Dict[int, Dict[str, Optional[CachedPanoImage]]] = defaultdict(lambda: {'ph': None, 'movie': None})
        self._frame_id_counter = 0

        self._configure_data_sources()

    def _configure_data_sources(self):
        """Instantiates data sources based on server configuration."""
        acq_config = self.server_config.get("acquisition_methods", {})
        self.logger.info(f"Configuring data sources: {acq_config}")

        # UDS Data Source (Server Mode)
        uds_cfg = acq_config.get("uds", {})
        if uds_cfg.get("enabled"):
            self.logger.info("Configuring UDS data sources (Server Mode).")
            socket_template = uds_cfg.get("socket_path_template")
            if not socket_template:
                self.logger.error("UDS is enabled, but 'socket_path_template' is not defined.")
            else:
                data_products = uds_cfg.get("data_products", [])
                for dp_name in data_products:
                    source_cfg = {
                        "dp_name": dp_name,
                        "socket_path_template": socket_template,
                        "read_timeout": uds_cfg.get("read_timeout", 60.0),
                    }
                    self.logger.info(f"Creating UDS server for data product '{dp_name}'")
                    self.data_sources.append(
                        UdsDataSource(source_cfg, self.logger, self.data_queue, self.stop_event)
                    )

        # Filesystem Polling Data Source
        poll_cfg = acq_config.get("filesystem_poll", {})
        if poll_cfg.get("enabled"):
            self.data_sources.append(PollWatcherDataSource(self, poll_cfg, self.logger, self.data_queue, self.stop_event))
        
        # Filesystem Pipe-based Data Source
        pipe_cfg = acq_config.get("filesystem_pipe", {})
        if pipe_cfg.get("enabled"):
            self.data_sources.append(PipeWatcherDataSource(self, pipe_cfg, self.logger, self.data_queue, self.stop_event))
        self.logger.info(f"Configured {len(self.data_sources)} data sources: {self.data_sources}")

    async def run(self):
        """Main entry point: starts data sources and the processing loop."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()

        # Filesystem modes need to scan for modules first.
        is_fs_mode = any(isinstance(s, (PollWatcherDataSource, PipeWatcherDataSource)) for s in self.data_sources)
        if is_fs_mode:
            if not await self._initialize_modules_from_fs():
                self.logger.warning("HpIoManager did not find any modules on filesystem at startup.")

        if not self.data_sources and not self.simulate_daq:
            self.logger.error("No data acquisition sources configured. HpIoManager cannot run.")
            return

        source_tasks = [asyncio.create_task(source.run()) for source in self.data_sources]
        processing_task = asyncio.create_task(self._processing_loop())

        # Wait for all data sources to signal they are ready.
        if source_tasks:
            try:
                self.logger.info("Waiting for all data sources to become ready.")
                all_sources_ready = asyncio.gather(*(s.ready_event.wait() for s in self.data_sources))
                await asyncio.wait_for(all_sources_ready, timeout=10.0)
                self.logger.info("All data sources have reported ready.")
            except asyncio.TimeoutError:
                self.logger.error(
                    "Timeout waiting for all data sources to become ready. HpIoManager will not be valid.")
                # Cancel all started tasks and exit
                for task in source_tasks + [processing_task]:
                    if not task.done():
                        task.cancel()
                return  # Exit without setting self.valid

        # Now that sources are ready, the manager can be considered valid.
        await self._update_active_data_products()
        self.valid.set()
        self.logger.info("HpIoManager task started and is valid.")

        try:
            await asyncio.gather(processing_task, *source_tasks)
        except Exception as e:
            self.logger.error(f"HpIoManager run error: {e}", exc_info=True)
        finally:
            self.valid.clear()
            self.logger.info("HpIoManager task exited.")


    async def _processing_loop(self):
        """ Assigns a unique frame_id to each incoming image before caching.  """
        self.logger.info("Starting freshness-aware processing loop.")
        while not self.stop_event.is_set():
            try:
                pano_image = await asyncio.wait_for(self.data_queue.get(), timeout=self.processing_loop_timeout)
                
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
            except asyncio.TimeoutError:
                continue
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
        
    async def fetch_latest_frame_from_file(self, filepath: Path, dp_config: DataProductState) -> Tuple[Optional[dict], Optional[Tuple[int]], int]:
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
                if header_str is None or img is None:
                    return None, None, -1
                return json.loads(header_str), img, new_frame_idx
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
        