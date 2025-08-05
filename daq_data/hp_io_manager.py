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
from .resources import get_dp_config, get_dp_name_from_props, is_daq_active
from .state import ReaderState, DataProductConfig
from panoseti_util import pff


def _parse_dp_name(filename: str, dp_name_re = re.compile(r'\.dp_([a-zA-Z0-9]+)\.')) -> Optional[str]:
    """Extracts the data product name (e.g., 'img16') from a PFF filename."""
    match = dp_name_re.search(filename)
    return match.group(1) if match else None



def _parse_seqno(filename: str, seqno_re=re.compile(r'\.seqno_(\d+)\.')) -> Optional[int]:
    """Extracts the seqno from a PFF filename."""
    match = seqno_re.search(filename)
    seqno = int(match.group(1)) if match else None
    return seqno


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
                    dp_config.last_seqno = _parse_seqno(latest_file.name)
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
            read_status_pipe_name: str,
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
        self.read_status_pipe_name = read_status_pipe_name
        self.sim_cfg = sim_cfg

        self.modules: Dict[int, ModuleState] = {}
        self.latest_data_cache: Dict[int, Dict[str, PanoImage]] = defaultdict(lambda: {'ph': None, 'movie': None})
        self.change_queue = asyncio.Queue()
        self.pipe_fds_to_close = []
        self._pipe_readers_installed = False

    async def run(self):
        """Main entry point to start the monitoring and broadcasting task."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()
        watcher_task = None
        processing_task = None
        loop = asyncio.get_running_loop()

        try:
            if not await self._initialize_modules_from_fs():
                self.logger.warning("HpIoManager did not find any modules on filesystem at startup.")

            use_pipe_watcher = False
            if self.modules:
                for module in self.modules.values():
                    if module.run_path:
                        pipe_path = module.run_path / self.read_status_pipe_name
                        if pipe_path.exists() and stat.S_ISFIFO(os.stat(pipe_path).st_mode):
                            self.logger.info(f"Found named pipe at {pipe_path}. Activating pipe-based watcher.")
                            use_pipe_watcher = True
                            break

            if use_pipe_watcher:
                watcher_task = asyncio.create_task(self.watch_status_pipe())
            else:
                self.logger.info("No named pipe found. Using filesystem polling watcher.")
                watcher_task = asyncio.create_task(self._file_watcher())

            await self._update_active_data_products()
            self.valid.set()

            processing_task = asyncio.create_task(self._processing_loop())

            await asyncio.gather(watcher_task, processing_task)

        except Exception as err:
            self.logger.error(f"HpIoManager task encountered a fatal exception: {err}", exc_info=True)
            if watcher_task: watcher_task.cancel()
            if processing_task: processing_task.cancel()
            raise
        finally:
            self.valid.clear()
            self.logger.info("Closing all open file and pipe handles...")

            # Clean up pipe readers from the event loop
            if self._pipe_readers_installed:
                for fd in self.pipe_fds_to_close:
                    loop.remove_reader(fd)
                self._pipe_readers_installed = False

            for fd in self.pipe_fds_to_close:
                try:
                    os.close(fd)
                except Exception as e:
                    self.logger.warning(f"Error closing pipe fd: {e}")

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
                                    recursive=True, force_polling=True, poll_delay_ms=update_interval_ms):
            await self.change_queue.put(changes)

    async def watch_status_pipe(self):
        """
        Watches for signals on a named pipe ('read_status_pipe') and finds the newest
        .pff file for each data product to queue for processing.
        """
        self.logger.info("Starting pipe-based watcher.")
        loop = asyncio.get_running_loop()
        pipes_to_watch = {}

        for mid, module in self.modules.items():
            if module.run_path:
                pipe_path = module.run_path / self.read_status_pipe_name
                if pipe_path.exists() and stat.S_ISFIFO(os.stat(pipe_path).st_mode):
                    try:
                        fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)
                        pipes_to_watch[fd] = module
                        self.pipe_fds_to_close.append(fd)
                    except Exception as e:
                        self.logger.error(f"Failed to open pipe for reading on module {mid}: {e}")

        if not pipes_to_watch:
            self.logger.warning("No valid pipes found to watch.")
            return

        data_ready = asyncio.Event()

        def _pipe_readable_callback():
            data_ready.set()

        for fd in pipes_to_watch.keys():
            loop.add_reader(fd, _pipe_readable_callback)
        self._pipe_readers_installed = True

        try:
            while not self.stop_io.is_set():
                await asyncio.wait_for(data_ready.wait(), timeout=1.0)

                # Drain all readable pipes
                for fd in pipes_to_watch.keys():
                    try:
                        while os.read(fd, 1024):
                            pass
                    except BlockingIOError:
                        continue  # Pipe is empty, continue to next fd

                all_changes = set()
                # For each module, find the latest .pff file for each data product.
                for module in self.modules.values():
                    if not module.run_path:
                        continue

                    # Get all pff files for the module
                    all_pff_file_paths = await asyncio.to_thread(glob, str(module.run_path / '*.pff'))
                    if not all_pff_file_paths:
                        continue

                    # Group files by data product, as sequence numbers are per-DP.
                    pff_files_by_dp = defaultdict(list)
                    for f_path in all_pff_file_paths:
                        # filename = Path(f_path).name
                        filename = f_path.split('/')[-1]
                        dp_name = _parse_dp_name(filename)
                        if dp_name:
                            pff_files_by_dp[dp_name].append(filename)

                    for dp_name, files in pff_files_by_dp.items():
                        if not files:
                            continue

                        # Use max() with _parse_seqno as the key to efficiently find the
                        # filename with the highest sequence number. The `or -1` handles
                        # malformed filenames gracefully.
                        newest_pff_file = max(files, key=lambda f: _parse_seqno(f) or -1)

                        # Add the full path of the newest file to the change set.
                        full_path = str(module.run_path / newest_pff_file)
                        all_changes.add((2, full_path))  # (2, f) is a "modified" event

                if all_changes:
                    await self.change_queue.put(all_changes)

                data_ready.clear()  # Reset for the next signal

        except asyncio.TimeoutError:
            pass  # It's okay to timeout, the loop will continue
        except asyncio.CancelledError:
            self.logger.info("Pipe watcher task cancelled.")
        finally:
            self.logger.info("Pipe watcher task exited.")
            # Cleanup of readers and fds is handled in the main run() method's finally block.

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
                # self.logger.debug(f"Broadcasting data to {ready_readers=}")
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

            try:
                if module.run_path and module.run_path.samefile(filepath.parent):
                    await self._handle_file_change(filepath, module, module.dp_configs[dp_name])
            except FileNotFoundError:
                # This can happen in a race condition where the directory is removed
                # after being detected but before samefile() is called.
                self.logger.warning(f"Path {filepath.parent} or {module.run_path} not found during check.")
                continue

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
        """
        Handles a file change, managing file handles and fetching the latest frame.
        This method is now responsible for opening/closing file handles to optimize _fetch_latest_frame.
        """
        # Check if the file has changed from the one we are currently tracking
        curr_seqno = _parse_seqno(filepath.name)
        if curr_seqno > dp_config.last_seqno or dp_config.f is None:
            self.logger.debug(f"New {curr_seqno=} data file detected for {dp_config.name} in module {module.module_id}: {filepath}")
            # Close the old file handle if it exists
            if dp_config.f:
                dp_config.f.close()

            # Open the new file and cache the handle and path
            try:
                dp_config.f = open(filepath, 'rb')
                dp_config.current_filepath = filepath
                dp_config.last_known_filesize = 0  # Reset for the new file
                dp_config.last_frame_idx = -1
                dp_config.last_seqno = curr_seqno
            except FileNotFoundError:
                self.logger.warning(f"File {filepath} disappeared before it could be opened.")
                dp_config.f = None
                dp_config.current_filepath = None
                dp_config.last_seqno = -1
                return

        # If we have a valid file handle, fetch the latest frame
        if dp_config.f:
            # self.logger.debug(f"File change detected for {dp_config.name} in module {module.module_id}: {filepath}")
            header, img, frame_idx = await self._fetch_latest_frame(dp_config)
            if header and img is not None:
                # self.logger.debug(f"Received latest frame for {dp_config.name} in module {module.module_id}: ")
                pano_image = PanoImage(
                    type=dp_config.pano_image_type,
                    header=ParseDict(header, Struct()),
                    image_array=img,
                    shape=dp_config.image_shape,
                    bytes_per_pixel=dp_config.bytes_per_pixel,
                    file=filepath.name,  # Use the original filepath name for the message
                    frame_number=frame_idx,
                    module_id=module.module_id,
                )

                cache_key = 'ph' if dp_config.is_ph else 'movie'
                self.latest_data_cache[module.module_id][cache_key] = pano_image

    async def _fetch_latest_frame(self, dp_config: DataProductConfig) -> Tuple[Optional[dict], Optional[bytes], int]:
        """
        Reads the last complete frame from a PFF file using a cached file handle.
        """
        try:
            # Get the current file size using the cached file descriptor
            f = dp_config.f
            current_size = os.fstat(f.fileno()).st_size

            # If the file hasn't grown or has no data, ignore.
            if current_size <= dp_config.last_known_filesize or dp_config.frame_size == 0:
                return None, None, -1

            nframes = current_size // dp_config.frame_size
            # If the number of frames hasn't increased, ignore.
            if nframes <= dp_config.last_frame_idx:
                return None, None, -1

            new_frame_idx = nframes - 1

            def _blocking_read(file_handle, frame_idx):
                """Blocking I/O operations to be run in a separate thread."""
                file_handle.seek(frame_idx * dp_config.frame_size)
                header_str = pff.read_json(file_handle)
                img = pff.read_image(file_handle, dp_config.image_shape[0], dp_config.bytes_per_pixel)
                return (json.loads(header_str), img) if header_str and img is not None else (None, None)

            header, img = await asyncio.to_thread(_blocking_read, f, new_frame_idx)

            if header and img is not None:
                # Update state only after a successful read
                dp_config.last_known_filesize = current_size
                dp_config.last_frame_idx = new_frame_idx
                return header, img, new_frame_idx

            return None, None, -1
        except (IOError, ValueError) as e:
            self.logger.warning(f"Could not read latest frame from {dp_config.current_filepath}: {e}")
            # Close the potentially corrupted file handle so it can be reopened on the next change
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
        """Broadcasts cached data to ready clients and clears the cache for that data."""
        for mid in list(self.latest_data_cache.keys()):
            data = self.latest_data_cache[mid]
            # self.logger.debug(f"Broadcasting data for module {mid}: {data}")

            ph_image = data.get('ph')
            movie_image = data.get('movie')

            # Broadcast pulse-height image if it exists
            if ph_image:
                for rs in ready_readers:
                    # Check if client wants this data type and module
                    if rs.config['stream_pulse_height_data'] and \
                            (not rs.config['module_ids'] or mid in rs.config['module_ids']):
                        try:
                            # self.logger.debug(f"Sending {ph_image=} to {rs.uid}")
                            rs.queue.put_nowait(ph_image)
                            rs.enqueue_timeouts = 0  # Reset on successful send
                        except asyncio.QueueFull:
                            rs.enqueue_timeouts += 1
                            self.logger.warning(
                                f"Reader queue full for client {rs.client_ip} (PH). Timeout count: {rs.enqueue_timeouts}")
                # Clear the cache for this image after broadcasting
                self.latest_data_cache[mid]['ph'] = None

            # Broadcast movie image if it exists
            if movie_image:
                for rs in ready_readers:
                    # Check if client wants this data type and module
                    if rs.config['stream_movie_data'] and \
                            (not rs.config['module_ids'] or mid in rs.config['module_ids']):
                        try:
                            # self.logger.debug(f"Sending {movie_image=} to {rs.uid}")
                            rs.queue.put_nowait(movie_image)
                            rs.enqueue_timeouts = 0  # Reset on successful send
                        except asyncio.QueueFull:
                            rs.enqueue_timeouts += 1
                            self.logger.warning(
                                f"Reader queue full for client {rs.client_ip} (Movie). Timeout count: {rs.enqueue_timeouts}")
                # Clear the cache for this image after broadcasting
                self.latest_data_cache[mid]['movie'] = None

        # Update all ready readers' timestamps after the broadcast attempt
        for rs in ready_readers:
            rs.last_update_t = now

