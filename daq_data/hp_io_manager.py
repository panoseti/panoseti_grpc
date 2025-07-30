"""
Orchestrates filesystem monitoring and data broadcasting for PANOSETI DAQ.
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
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict
from watchfiles import awatch, Change

from .daq_data_pb2 import PanoImage
from .resources import get_dp_config, is_daq_active, DataProductConfig
from panoseti_util import pff


def _parse_seqno(filename: str) -> int:
    """Extracts the sequence number from a PFF filename."""
    match = re.search(r'seqno_(\d+)', filename)
    return int(match.group(1)) if match else -1


class ModuleState:
    """Manages the state and logic for a single PANOSETI module's data acquisition."""

    def __init__(self, module_id: int, data_dir: Path, data_products: List[str], logger: logging.Logger):
        self.module_id = module_id
        self.data_dir = data_dir
        self.logger = logger
        self.run_path: Optional[Path] = None
        self.dp_configs: Dict[str, DataProductConfig] = get_dp_config(data_products)

    async def initialize(self, timeout: float = 2.0) -> bool:
        """Finds the active run directory and initializes data product configurations."""
        run_pattern = self.data_dir / f"module_{self.module_id}" / "obs_*"
        runs = glob(str(run_pattern))
        if not runs:
            self.logger.warning(f'No run directory found for module {self.module_id} matching {run_pattern}')
            return False

        self.run_path = Path(sorted(runs, key=os.path.getmtime)[-1])
        self.logger.info(f"Module {self.module_id}: Found active run directory {self.run_path}")

        init_tasks = [self._initialize_dp(dp_config, timeout) for dp_config in self.dp_configs.values()]
        results = await asyncio.gather(*init_tasks)

        active_dps = [dp_cfg.name for dp_cfg, is_active in zip(self.dp_configs.values(), results) if is_active]
        self.dp_configs = {name: self.dp_configs[name] for name in active_dps}
        return len(self.dp_configs) > 0

    async def _initialize_dp(self, dp_config: DataProductConfig, timeout: float) -> bool:
        """Initializes a single data product by finding a valid data file and its frame size."""
        start_time = time.monotonic()
        dp_config.glob_pat = str(self.run_path / f'*{dp_config.name}*.pff')

        while time.monotonic() - start_time < timeout:
            files = glob(dp_config.glob_pat)
            if not files:
                await asyncio.sleep(0.5)
                continue

            latest_file = Path(sorted(files, key=os.path.getmtime)[-1])
            try:
                if os.path.getsize(latest_file) >= dp_config.bytes_per_image:
                    with open(latest_file, 'rb') as f:
                        dp_config.frame_size = pff.img_frame_size(f, dp_config.bytes_per_image)

                    # Set initial state for file tracking
                    dp_config.current_filepath = latest_file
                    dp_config.last_known_filesize = os.path.getsize(latest_file)
                    return True
            except (FileNotFoundError, ValueError):
                return False
            await asyncio.sleep(0.5)
        return False


class HpIoManager:
    """
    Orchestrates filesystem monitoring and data broadcasting for PANOSETI DAQ.
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

    def __init__(
            self,
            data_dir: Path,
            data_products: List[str],
            update_interval_seconds: float,
            module_id_whitelist: List[int],
            simulate_daq: bool,
            reader_states: List[Dict],
            stop_io: asyncio.Event,
            valid: asyncio.Event,
            max_reader_enqueue_timeouts: int,
            dp_queue: asyncio.Queue,
            logger: logging.Logger,
            sim_cfg: Dict[str, Any],
    ):
        """Initializes the manager with explicit configuration arguments."""
        self.data_dir = data_dir
        self.data_products = data_products
        self.update_interval_seconds = update_interval_seconds
        self.module_id_whitelist = module_id_whitelist
        self.simulate_daq = simulate_daq
        self.reader_states = reader_states
        self.stop_io = stop_io
        self.valid = valid
        self.max_reader_enqueue_timeouts = max_reader_enqueue_timeouts
        self.dp_queue = dp_queue
        self.logger = logger
        self.sim_cfg = sim_cfg

        self.modules: Dict[int, ModuleState] = {}
        self.latest_data_cache: Dict[int, Dict[str, PanoImage]] = defaultdict(lambda: {'ph': None, 'movie': None})
        self.change_queue = asyncio.Queue()

    async def run(self):
        """Main entry point to start the monitoring and broadcasting task."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()
        try:
            if not await self._initialize_all_modules():
                self.logger.error("HpIoManager failed to initialize any modules. Exiting.")
                return
            self.valid.set()

            watcher_task = asyncio.create_task(self._file_watcher())
            processing_task = asyncio.create_task(self._processing_loop())
            await asyncio.gather(watcher_task, processing_task)

        except Exception as err:
            self.logger.error(f"HpIoManager task encountered a fatal exception: {err}", exc_info=True)
            if 'watcher_task' in locals() and not watcher_task.done(): watcher_task.cancel()
            if 'processing_task' in locals() and not processing_task.done(): processing_task.cancel()
            raise
        finally:
            self.valid.clear()
            self.logger.info("HpIoManager task exited.")

    async def _initialize_all_modules(self) -> bool:
        if not await is_daq_active(self.simulate_daq, self.sim_cfg, retries=2, delay=0.5):
            raise EnvironmentError("DAQ data flow not active.")

        module_dirs = glob(str(self.data_dir / "module_*"))
        all_module_ids = [int(os.path.basename(m).split('_')[1]) for m in module_dirs if
                          os.path.isdir(m) and os.path.basename(m).split('_')[1].isdigit()]

        target_ids = set(all_module_ids)
        if self.module_id_whitelist:
            target_ids.intersection_update(self.module_id_whitelist)

        module_instances = [ModuleState(mid, self.data_dir, self.data_products, self.logger) for mid in target_ids]

        results = await asyncio.gather(*(mod.initialize() for mod in module_instances))

        active_dps_union = set()
        for is_active, module in zip(results, module_instances):
            if is_active:
                self.modules[module.module_id] = module
                active_dps_union.update(module.dp_configs.keys())

        if not self.modules: return False
        await self.dp_queue.put(active_dps_union)
        return True

    async def _file_watcher(self):
        """Producer: Watches for file changes and puts them on the queue."""
        watch_paths = list(set(m.run_path for m in self.modules.values() if m.run_path))
        if not watch_paths: return

        update_interval_ms = int(self.update_interval_seconds * 1000)
        async_watcher = awatch(
            *watch_paths,
            stop_event=self.stop_io,
            # debounce=,
            recursive=True,
            poll_delay_ms=update_interval_ms,
            force_polling=True
        )

        async for changes in async_watcher:
            await self.change_queue.put(changes)

    async def _processing_loop(self):
        """Consumer: Processes file changes from the queue and handles client deadlines."""
        last_daq_check = time.monotonic()
        while not self.stop_io.is_set():
            now = time.monotonic()

            wait_times = [(rs['config']['update_interval_seconds'] - (now - rs.get('last_update_t', 0)))
                          for rs in self.reader_states if rs['is_allocated']]
            timeout = min(wait_times) if wait_times else self.update_interval_seconds
            timeout = max(0.01, timeout)

            try:
                changes = await asyncio.wait_for(self.change_queue.get(), timeout=timeout)
                await self._process_changes(changes)
            except asyncio.TimeoutError:
                pass  # Expected when a client deadline is reached.

            now = time.monotonic()
            ready_readers, _, _ = self._get_ready_readers(now)
            if ready_readers:
                self._broadcast_data(ready_readers, now)

            if now - last_daq_check >= 10:
                if not await is_daq_active(self.simulate_daq, self.sim_cfg):
                    self.logger.warning("DAQ data flow stopped.")
                    self.stop_io.set()
                    return
                last_daq_check = now

    async def _process_changes(self, changes: set):
        """Processes file changes and updates the internal latest_data_cache."""
        for _, filepath_str in changes:
            filepath = Path(filepath_str)
            if not filepath.name.endswith('.pff'): continue

            for module in self.modules.values():
                if module.run_path and filepath.parent.absolute() == module.run_path.absolute():
                    for dp_config in module.dp_configs.values():
                        if dp_config.name in filepath.name:
                            await self._handle_file_change(filepath, module, dp_config)
                            break
                    break

    async def _handle_file_change(self, filepath: Path, module: ModuleState, dp_config: DataProductConfig):
        """Handles a file change event, including logic for file rotation."""
        # This is the core logic fix for file rotation.
        current_seqno = _parse_seqno(dp_config.current_filepath.name) if dp_config.current_filepath else -1
        new_seqno = _parse_seqno(filepath.name)

        # If the new file has a higher sequence number, it's a rollover.
        if new_seqno > current_seqno:
            self.logger.debug(f"Module {module.module_id}, DP {dp_config.name}: Rollover detected. "
                             f"Switching from seqno {current_seqno} to {new_seqno}.")
            dp_config.current_filepath = filepath
            dp_config.last_known_filesize = 0  # Reset size for the new file.

        # Only process if the change is on the currently active file.
        if filepath != dp_config.current_filepath:
            return

        try:
            current_size = os.path.getsize(filepath)
        except FileNotFoundError:
            dp_config.last_known_filesize = 0
            return

        if 0 < dp_config.frame_size <= (current_size - dp_config.last_known_filesize):
            dp_config.last_known_filesize = current_size
            header, img, frame_idx = self._fetch_latest_frame(filepath, dp_config)
            if header and img is not None:
                pano_image = PanoImage(
                    type=dp_config.pano_image_type,
                    header=ParseDict(header, Struct()),
                    image_array=img,
                    shape=dp_config.image_shape,
                    bytes_per_pixel=dp_config.bytes_per_pixel,
                    file=filepath.name,
                    frame_number=frame_idx,
                    module_id=module.module_id,
                )
                if dp_config.is_ph:
                    self.latest_data_cache[module.module_id]['ph'] = pano_image
                else:
                    self.latest_data_cache[module.module_id]['movie'] = pano_image

    def _fetch_latest_frame(self, filepath: Path, dp_config: DataProductConfig):
        """Reads the last complete frame from a PFF file."""
        try:
            with open(filepath, 'rb') as f:
                fsize = os.path.getsize(filepath)
                nframes = fsize // dp_config.frame_size
                if nframes > 0:
                    last_frame_idx = nframes - 1
                    f.seek(last_frame_idx * dp_config.frame_size)
                    header_str = pff.read_json(f)
                    img = pff.read_image(f, dp_config.image_shape[0], dp_config.bytes_per_pixel)
                    if header_str and img is not None:
                        return json.loads(header_str), img, last_frame_idx
        except (FileNotFoundError, ValueError) as e:
            self.logger.warning(f"Could not read latest frame from {filepath}: {e}")
        return None, None, -1

    def _get_ready_readers(self, now: float):
        """Identifies clients that are ready to receive data."""
        ready_list = []
        for rs in filter(lambda r: r['is_allocated'], self.reader_states):
            if rs['enqueue_timeouts'] >= self.max_reader_enqueue_timeouts: continue
            if (now - rs.get('last_update_t', 0)) >= rs['config']['update_interval_seconds']:
                ready_list.append(rs)
        return ready_list, 0, 0

    def _broadcast_data(self, ready_readers, now):
        """Puts the latest cached data onto the queues of ready clients."""
        for rs in ready_readers:
            try:
                for mid, data in self.latest_data_cache.items():
                    if rs['config']['module_ids'] and mid not in rs['config']['module_ids']: continue

                    ph_image = data.get('ph')
                    if rs['config']['stream_pulse_height_data'] and ph_image:
                        rs['queue'].put_nowait(ph_image)

                    movie_image = data.get('movie')
                    if rs['config']['stream_movie_data'] and movie_image:
                        rs['queue'].put_nowait(movie_image)

                rs['enqueue_timeouts'] = 0
            except asyncio.QueueFull:
                self.logger.warning(f"Reader queue full for client {rs['client_ip']}.")
                rs['enqueue_timeouts'] += 1
            rs['last_update_t'] = now


