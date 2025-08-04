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
from watchfiles import awatch

from .daq_data_pb2 import PanoImage
from .resources import get_dp_config, get_dp_name_from_props, is_daq_active
from .state import ReaderState, DataProductConfig
from panoseti_util import pff


def _parse_dp_name(filename: str) -> Optional[str]:
    """Extracts the data product name (e.g., 'img16') from a PFF filename."""
    match = re.search(r'\.dp_([a-zA-Z0-9]+)\.', filename)
    return match.group(1) if match else None


class ModuleState:
    """Manages the state and logic for a single PANOSETI module's data acquisition."""

    def __init__(self, module_id: int, data_dir: Path, logger: logging.Logger):
        self.module_id = module_id
        self.data_dir = data_dir
        self.logger = logger
        self.run_path: Optional[Path] = None
        self.dp_configs: Dict[str, DataProductConfig] = {}

    async def discover_and_initialize_from_fs(self, timeout: float = 2.0) -> bool:
        """Finds the active run directory and initializes all discoverable data products."""
        run_pattern = self.data_dir / f"module_{self.module_id}" / "obs_*"
        runs = await asyncio.to_thread(glob, str(run_pattern))
        if not runs:
            self.logger.warning(f'No run directory found for module {self.module_id} matching {run_pattern}')
            return False
        self.run_path = Path(sorted(runs, key=os.path.getmtime)[-1])
        self.logger.info(f"Module {self.module_id}: Found active run at {self.run_path}")

        pff_files = await asyncio.to_thread(glob, str(self.run_path / '*.pff'))
        discovered_dp_names = set(_parse_dp_name(Path(f).name) for f in pff_files if _parse_dp_name(Path(f).name))

        if not discovered_dp_names:
            return True  # No data products is not a fatal error here

        self.logger.info(f"Module {self.module_id}: Discovered data products: {discovered_dp_names}")
        init_tasks = [self.add_dp_from_fs(dp_name, timeout) for dp_name in discovered_dp_names]
        results = await asyncio.gather(*init_tasks)
        return any(results)

    async def add_dp_from_fs(self, dp_name: str, timeout: float = 1.0) -> bool:
        """Adds and initializes a data product configuration from the filesystem."""
        if dp_name in self.dp_configs: return True
        try:
            dp_configs = get_dp_config([dp_name])
            dp_config = dp_configs[dp_name]
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
            dp_configs = get_dp_config([dp_name])
            self.dp_configs[dp_name] = dp_configs[dp_name]
            self.logger.info(f"Module {self.module_id}: Added config for uploaded data product '{dp_name}'")
        except ValueError as e:
            self.logger.error(f"Module {self.module_id}: Could not get config for uploaded DP '{dp_name}': {e}")

    async def _initialize_dp(self, dp_config: DataProductConfig, timeout: float) -> bool:
        """Initializes a single data product by finding a valid data file and its frame size."""
        start_time = time.monotonic()
        dp_config.glob_pat = str(self.run_path / f'*{dp_config.name}*.pff')
        while time.monotonic() - start_time < timeout:
            files = await asyncio.to_thread(glob, dp_config.glob_pat)
            if not files:
                await asyncio.sleep(0.25)
                continue
            latest_file = Path(sorted(files, key=os.path.getmtime)[-1])
            try:
                if os.path.getsize(latest_file) >= dp_config.bytes_per_image:
                    with open(latest_file, 'rb') as f:
                        try:
                            dp_config.frame_size = pff.img_frame_size(f, dp_config.bytes_per_image)
                        except Exception as e:
                            self.logger.warning(f"Failed to get frame size for {dp_config.name} for module {self.module_id}: {e}")
                            return False
                    dp_config.current_filepath = latest_file
                    dp_config.last_known_filesize = await asyncio.to_thread(os.path.getsize, latest_file)
                    return True
            except (FileNotFoundError, ValueError) as e:
                self.logger.warning(f"Failed to initialize {dp_config.name} for module {self.module_id}: {e}")
                return False
            await asyncio.sleep(0.25)
        self.logger.warning(f"Timeout initializing data product {dp_config.name} for module {self.module_id}")
        return False


class HpIoManager:
    """Orchestrates dynamic filesystem monitoring and data broadcasting for PANOSETI DAQ."""

    def __init__(
            self,
            data_dir: Path,
            update_interval_seconds: float,
            simulate_daq: bool,
            reader_states: List[ReaderState],
            stop_io: asyncio.Event,
            valid: asyncio.Event,
            max_reader_enqueue_timeouts: int,
            active_data_products_queue: asyncio.Queue,
            upload_queue: asyncio.Queue,
            logger: logging.Logger,
            sim_cfg: Dict[str, Any],
    ):
        self.data_dir = data_dir
        self.update_interval_seconds = update_interval_seconds
        self.simulate_daq = simulate_daq
        self.reader_states = reader_states
        self.stop_io = stop_io
        self.valid = valid
        self.max_reader_enqueue_timeouts = max_reader_enqueue_timeouts
        self.active_data_products_queue = active_data_products_queue
        self.upload_queue = upload_queue
        self.logger = logger
        self.sim_cfg = sim_cfg

        self.modules: Dict[int, ModuleState] = {}
        self.latest_data_cache: Dict[int, Dict[str, PanoImage]] = defaultdict(lambda: {'ph': None, 'movie': None})
        self.change_queue = asyncio.Queue()

    async def run(self):
        """Main entry point to start the monitoring and broadcasting task."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()
        watcher_task = None
        processing_task = None
        try:
            if not await self._initialize_modules_from_fs():
                self.logger.warning("HpIoManager did not find any modules on filesystem at startup.")
            await self._update_active_data_products()
            self.valid.set()
            watcher_task = asyncio.create_task(self._file_watcher())
            processing_task = asyncio.create_task(self._processing_loop())
            await asyncio.gather(watcher_task, processing_task)
        except Exception as err:
            self.logger.error(f"HpIoManager task encountered a fatal exception: {err}", exc_info=True)
            if watcher_task: watcher_task.cancel()
            if processing_task: processing_task.cancel()
            raise
        finally:
            self.valid.clear()
            self.logger.info("Closing all open file handles...")
            for module in self.modules.values():
                for dp_config in module.dp_configs.values():
                    if dp_config.f:
                        await asyncio.to_thread(dp_config.f.close)
            self.logger.info("HpIoManager task exited.")

    async def _initialize_modules_from_fs(self) -> bool:
        """Initial discovery of modules and data products from the filesystem."""
        if not await is_daq_active(self.simulate_daq, self.sim_cfg, retries=2, delay=0.5):
            self.logger.warning("DAQ data flow not active, filesystem scan may be incomplete.")

        module_dirs = await asyncio.to_thread(glob, str(self.data_dir / "module_*"))
        all_module_ids = [int(os.path.basename(m).split('_')[1]) for m in module_dirs if
                          os.path.isdir(m) and os.path.basename(m).split('_')[1].isdigit()]

        if not all_module_ids: return False

        for mid in all_module_ids:
            if mid not in self.modules:
                module = ModuleState(mid, self.data_dir, self.logger)
                if await module.discover_and_initialize_from_fs():
                    self.modules[mid] = module
        return len(self.modules) > 0

    async def _update_active_data_products(self):
        """Collects all unique DP names from all modules and informs the task manager."""
        active_dps_union = set()
        for module in self.modules.values():
            active_dps_union.update(module.dp_configs.keys())
        await self.active_data_products_queue.put(active_dps_union)

    async def _file_watcher(self):
        """Watches for file changes and puts them on the queue."""
        if not await asyncio.to_thread(os.path.isdir, self.data_dir):
            self.logger.warning(f"Data directory {self.data_dir} does not exist. Filesystem watcher will not start.")
            return

        MIN_UPDATE_MS = 5
        update_interval_ms = max(int(self.update_interval_seconds * 1000), MIN_UPDATE_MS)
        async for changes in awatch(self.data_dir, stop_event=self.stop_io, debounce=update_interval_ms,
                                    recursive=True):
            await self.change_queue.put(changes)

    async def _processing_loop(self):
        """Processes events from filesystem and upload queue."""
        change_task = asyncio.create_task(self.change_queue.get())
        upload_task = asyncio.create_task(self.upload_queue.get())
        while not self.stop_io.is_set():
            now = time.monotonic()
            wait_times = [(rs.config['update_interval_seconds'] - (now - rs.last_update_t))
                          for rs in self.reader_states if rs.is_allocated]
            timeout = min(wait_times) if wait_times else self.update_interval_seconds
            timeout = max(0.01, timeout)

            done, _ = await asyncio.wait(
                {change_task, upload_task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if change_task in done:
                await self._process_changes(change_task.result())
                change_task = asyncio.create_task(self.change_queue.get())
            if upload_task in done:
                await self._handle_uploaded_image(upload_task.result())
                upload_task = asyncio.create_task(self.upload_queue.get())

            ready_readers, _, _ = self._get_ready_readers(now)
            if ready_readers:
                self._broadcast_data(ready_readers, now)

    async def _process_changes(self, changes: set):
        """Handles file changes, discovering new modules/DPs as needed."""
        for _, filepath_str in changes:
            filepath = Path(filepath_str)
            if not filepath.name.endswith('.pff'): continue

            match = re.search(r'module_(\d+)', str(filepath))
            if not match: continue
            module_id = int(match.group(1))

            dp_name = _parse_dp_name(filepath.name)
            if not dp_name: continue

            if module_id not in self.modules:
                self.logger.info(f"Discovered new module via filesystem: {module_id}")
                module = ModuleState(module_id, self.data_dir, self.logger)
                if await module.discover_and_initialize_from_fs():
                    self.modules[module_id] = module
                    await self._update_active_data_products()
                else:
                    continue

            module = self.modules[module_id]
            if dp_name not in module.dp_configs:
                if await module.add_dp_from_fs(dp_name):
                    await self._update_active_data_products()
                else:
                    continue

            if module.run_path and filepath.parent.absolute() == module.run_path.absolute():
                await self._handle_file_change(filepath, module, module.dp_configs[dp_name])

    async def _handle_uploaded_image(self, pano_image: PanoImage):
        """Processes a directly uploaded PanoImage, discovering modules/DPs as needed."""
        module_id = pano_image.module_id
        try:
            dp_name = get_dp_name_from_props(pano_image.type, list(pano_image.shape), pano_image.bytes_per_pixel)
        except ValueError as e:
            self.logger.warning(f"Could not identify uploaded image for module {module_id}: {e}")
            return

        if module_id not in self.modules:
            self.logger.info(f"Discovered new module via upload: {module_id}")
            self.modules[module_id] = ModuleState(module_id, self.data_dir, self.logger)

        module = self.modules[module_id]
        if dp_name not in module.dp_configs:
            module.add_dp_for_upload(dp_name)
            await self._update_active_data_products()

        is_ph = (pano_image.type == PanoImage.Type.PULSE_HEIGHT)
        cache_key = 'ph' if is_ph else 'movie'
        self.latest_data_cache[module_id][cache_key] = pano_image

    async def _handle_file_change(self, filepath: Path, module: ModuleState, dp_config: DataProductConfig):
        """Fetches the latest frame from a modified PFF file."""
        header, img, frame_idx = await self._fetch_latest_frame(filepath, dp_config)
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
            cache_key = 'ph' if dp_config.is_ph else 'movie'
            self.latest_data_cache[module.module_id][cache_key] = pano_image

    async def _fetch_latest_frame(self, filepath: Path, dp_config: DataProductConfig) -> Tuple[
        Optional[dict], Optional[list], int]:
        """Reads the last complete frame from a PFF file, managing the file handle."""
        try:
            if filepath != dp_config.current_filepath:
                if dp_config.f: await asyncio.to_thread(dp_config.f.close)
                dp_config.f = await asyncio.to_thread(open, filepath, 'rb')
                dp_config.current_filepath = filepath
                dp_config.last_known_filesize = 0
                dp_config.last_frame_idx = -1
            if dp_config.f is None:
                dp_config.f = await asyncio.to_thread(open, filepath, 'rb')
                dp_config.current_filepath = filepath

            current_size = (await asyncio.to_thread(os.fstat, dp_config.f.fileno())).st_size
            if current_size <= dp_config.last_known_filesize or dp_config.frame_size == 0:
                return None, None, -1

            nframes = current_size // dp_config.frame_size
            if nframes <= dp_config.last_frame_idx:
                return None, None, -1

            new_frame_idx = nframes - 1

            def _blocking_read(f, frame_idx):
                f.seek(frame_idx * dp_config.frame_size)
                header_str = pff.read_json(f)
                img = pff.read_image(f, dp_config.image_shape[0], dp_config.bytes_per_pixel)
                return (json.loads(header_str), img) if header_str and img is not None else (None, None)

            header, img = await asyncio.to_thread(_blocking_read, dp_config.f, new_frame_idx)
            if header and img is not None:
                dp_config.last_known_filesize = current_size
                dp_config.last_frame_idx = new_frame_idx
                return header, img, new_frame_idx
            return None, None, -1
        except (FileNotFoundError, ValueError) as e:
            self.logger.warning(f"Could not read latest frame from {filepath}: {e}")
            if dp_config.f:
                await asyncio.to_thread(dp_config.f.close)
            dp_config.f = None
            return None, None, -1

    def _get_ready_readers(self, now: float):
        """Identifies clients ready to receive data based on their update interval."""
        ready_list = []
        for rs in filter(lambda r: r.is_allocated, self.reader_states):
            if rs.enqueue_timeouts < self.max_reader_enqueue_timeouts and \
                    (now - rs.last_update_t) >= rs.config['update_interval_seconds']:
                ready_list.append(rs)
        return ready_list, 0, 0

    def _broadcast_data(self, ready_readers, now):
        """Puts the latest cached data onto the queues of ready clients."""
        for rs in ready_readers:
            try:
                data_sent = False
                for mid, data in self.latest_data_cache.items():
                    if rs.config['module_ids'] and mid not in rs.config['module_ids']: continue
                    ph_image = data.get('ph')
                    if rs.config['stream_pulse_height_data'] and ph_image:
                        rs.queue.put_nowait(ph_image)
                        data_sent = True
                    movie_image = data.get('movie')
                    if rs.config['stream_movie_data'] and movie_image:
                        rs.queue.put_nowait(movie_image)
                        data_sent = True
                if data_sent:
                    rs.enqueue_timeouts = 0
            except asyncio.QueueFull:
                rs.enqueue_timeouts += 1
                self.logger.warning(
                    f"Reader queue full for client {rs.client_ip}. Timeout count: {rs.enqueue_timeouts}")
            finally:
                rs.last_update_t = now
