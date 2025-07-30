# In a new file, e.g., daq_data/hp_io_manager.py, or at the top of server.py

import asyncio
import json
import logging
import os
import time
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Event

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict
from watchfiles import awatch, Change

from .daq_data_pb2 import PanoImage
# from . import daq_data_pb2.PanoImage.Type as PanoImageType
from .resources import get_dp_cfg, is_daq_active
from panoseti_util import pff


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


class ModuleState:
    """Manages the state and logic for a single PANOSETI module's data acquisition."""

    def __init__(self, module_id: int, data_dir: Path, data_products: List[str], logger: logging.Logger):
        self.module_id = module_id
        self.data_dir = data_dir
        self.logger = logger
        self.run_path: Optional[Path] = None
        self.dp_configs: Dict[str, DataProductConfig] = get_dp_config_new(data_products)

    async def initialize(self, timeout: float = 2.0) -> bool:
        """Finds the active run directory and initializes data product configurations."""
        run_pattern = self.data_dir / f"module_{self.module_id}" / "obs_*"
        runs = glob(str(run_pattern))
        if not runs:
            self.logger.warning(f'No run directory found for module {self.module_id} matching {run_pattern}')
            return False

        self.run_path = Path(sorted(runs, key=os.path.getmtime)[-1])
        self.logger.info(f"Module {self.module_id}: Found active run directory {self.run_path}")

        init_tasks = [self._initialize_dp(dp, timeout) for dp in self.dp_configs.values()]
        results = await asyncio.gather(*init_tasks)

        # Prune inactive data products
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

            latest_file = sorted(files, key=os.path.getmtime)[-1]
            try:
                if os.path.getsize(latest_file) >= dp_config.bytes_per_image:
                    with open(latest_file, 'rb') as f:
                        dp_config.frame_size = pff.img_frame_size(f, dp_config.bytes_per_image)
                    dp_config.last_known_filesize = os.path.getsize(latest_file)
                    self.logger.debug(
                        f"Initialized {dp_config.name} for module {self.module_id} with file {latest_file}")
                    return True
            except (FileNotFoundError, ValueError) as e:
                self.logger.warning(f"Could not initialize {dp_config.name} for module {self.module_id}: {e}")
                return False
            await asyncio.sleep(0.5)

        self.logger.debug(f'Timeout initializing {dp_config.name} for module {self.module_id}')
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
            for mid, module in self.modules.items():
                self.logger.info(f"{module.dp_configs=}")

            # Start producer and consumer tasks concurrently
            watcher_task = asyncio.create_task(self._file_watcher())
            processing_task = asyncio.create_task(self._processing_loop())

            await asyncio.gather(watcher_task, processing_task)

        except Exception as err:
            self.logger.error(f"HpIoManager task encountered a fatal exception: {err}", exc_info=True)
            # Cancel tasks on error to ensure clean shutdown
            if 'watcher_task' in locals() and not watcher_task.done():
                watcher_task.cancel()
            if 'processing_task' in locals() and not processing_task.done():
                processing_task.cancel()
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

        init_tasks = []
        for mid in target_ids:
            module = ModuleState(mid, self.data_dir, self.data_products, self.logger)
            self.modules[mid] = module
            init_tasks.append(module.initialize())

        results = await asyncio.gather(*init_tasks)
        active_dps_union = set()
        for is_active, mid in zip(results, target_ids):
            if not is_active:
                del self.modules[mid]
            else:
                active_dps = self.modules[mid].dp_configs.keys()
                active_dps_union.update(active_dps)

        if not self.modules:
            return False

        await self.dp_queue.put(active_dps_union)
        return True

    async def _file_watcher(self):
        """Producer: Watches for file changes and puts them on the queue."""
        watch_paths = list(set(m.run_path for m in self.modules.values() if m.run_path))
        if not watch_paths:
            return

        update_interval_ms = int(self.update_interval_seconds * 1000)
        awatcher = awatch(
            *watch_paths,
            stop_event=self.stop_io,
            debounce=update_interval_ms,
            recursive=True,
            force_polling=True,
            poll_delay_ms=update_interval_ms
        )
        async for changes in awatcher:
            # self.logger.info(f"Received file change event: {changes=}")
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
                # This is expected. It means a client deadline has been reached.
                pass

            now = time.monotonic()
            ready_readers, _, _ = self._get_ready_readers(now)
            if ready_readers:
                self._broadcast_data(ready_readers, now)

            if now - last_daq_check >= 10:
                if not await is_daq_active(self.simulate_daq, self.sim_cfg):
                    self.logger.warning("DAQ data flow stopped.")
                    self.stop_io.set()  # Signal shutdown
                    return
                last_daq_check = now

    async def _process_changes(self, changes: set):
        """Processes file changes and updates the internal latest_data_cache."""
        for _, filepath_str in changes:
            filepath = Path(filepath_str)
            if not filepath.name.endswith('.pff'):
                continue

            for module in self.modules.values():
                if module.run_path and filepath.parent.absolute() == module.run_path.absolute():
                    # self.logger.info(f"{module.dp_configs=}")
                    for dp_config in module.dp_configs.values():
                        # self.logger.info(f"{dp_config.name=}: {filepath.name=}")
                        if dp_config.name in filepath.name:
                            await self._check_and_update_cache(filepath, module, dp_config)
                            break
                    break

    async def _check_and_update_cache(self, filepath: Path, module: ModuleState, dp_config: DataProductConfig):
        """Checks file size and updates the cache if a new frame is present."""
        try:
            current_size = os.path.getsize(filepath)
        except FileNotFoundError:
            dp_config.last_known_filesize = 0
            return

        if 0 < dp_config.frame_size <= (current_size - dp_config.last_known_filesize):
            dp_config.last_known_filesize = current_size
            header, img, frame_idx = self._fetch_latest_frame(filepath, dp_config)
            self.logger.info(f"found data: {header=}, {frame_idx=}")
            if header and img is not None:
                # self.logger.info(f"{filepath=}: {header=}")
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
        else:
            self.logger.info(f"no new data: {filepath=}")

    def _fetch_latest_frame(self, filepath: Path, dp_config: DataProductConfig):
        """Reads the last complete frame from a PFF file."""
        try:
            with open(filepath, 'rb') as f:
                fsize = os.path.getsize(filepath)
                # f.seek(0, os.SEEK_END)
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
        ready_list, _, _ = [], 0, 0
        for rs in filter(lambda r: r['is_allocated'], self.reader_states):
            if rs['enqueue_timeouts'] >= self.max_reader_enqueue_timeouts:
                continue
            if (now - rs.get('last_update_t', 0)) >= rs['config']['update_interval_seconds']:
                ready_list.append(rs)
        return ready_list, 0, 0

    def _broadcast_data(self, ready_readers, now):
        """Puts the latest cached data onto the queues of ready clients."""
        for rs in ready_readers:
            try:
                for mid, data in self.latest_data_cache.items():
                    # self.logger.info(f"{data=}")
                    if rs['config']['module_ids'] and mid not in rs['config']['module_ids']:
                        continue

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

def get_dp_config_new(dps: List[str]) -> Dict[str, DataProductConfig]:
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
        dp_cfg[dp] = DataProductConfig(
            name=dp,
            is_ph=is_ph,
            pano_image_type=pano_image_type,
            image_shape=image_shape,
            bytes_per_pixel=bytes_per_pixel,
            bytes_per_image=bytes_per_image,
        )
    return dp_cfg


async def hp_io_task_fn_old(
        data_dir: Path,
        data_products: List[str],
        update_interval_seconds: float,
        module_id_whitelist: List[int],
        simulate_daq: bool,
        reader_states: List[Dict],
        stop_io: Event,
        valid: Event,
        max_reader_enqueue_timeouts,
        dp_queue: asyncio.Queue,
        logger: logging.Logger,
        sim_cfg: Dict[str, Any],
        **kwargs
) -> None:

    """ Receive pulse-height and movie-mode data from hashpipe and broadcast it to all active reader queues.
    Requires DAQ software to be active to properly initialize.
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
    logger.info(f"Created a new hp_io task with the following options: {kwargs=}")
    valid.clear()  # indicate hashpipe io channel is currently invalid

    async def init_dp_state(d: Dict[str, Any], dp: str, run_path, curr_t, timeout):
        """initialize hp_io state for a specific combination of module and data product."""
        # Get the current number of pff files with type [dp]
        d['glob_pat'] = '%s/*%s*.pff' % (run_path, dp)


        # wait until hashpipe starts writing files
        nfiles = 0
        files = []
        while not stop_io.is_set() and nfiles == 0 and (time.monotonic() - curr_t < timeout):
            files = glob(d['glob_pat'])
            nfiles = len(files)
            if nfiles > 0:
                break
            await asyncio.sleep(0.5)

        # check if the environment is valid
        if stop_io.is_set():
            # raise EnvironmentError("stop_io event is set")
            return False
        elif time.monotonic() - curr_t >= timeout:
            logger.debug(f'no file of type {dp} in {d["glob_pat"]}')
            return False

        # wait until the filesize is large enough to read one image of type [dp]
        while not stop_io.is_set() and (time.monotonic() - curr_t < timeout):
            # Get the most recently modified file
            # if hashpipe is actively collecting data, the most recent file should contain data
            files = glob(d['glob_pat'])
            nfiles = len(files)
            file = sorted(files, key=os.path.getmtime)[-1]
            filepath = file
            if os.path.getsize(filepath) >= d['bytes_per_image']:
                break
            await asyncio.sleep(0.5)

        # check if the environment is valid
        if stop_io.is_set():
            return False
        elif time.monotonic() - curr_t >= timeout:
            logger.info(
                f"no file of type {dp} in {d['glob_pat']} is large enough to read one image of type {dp}")
            return False

        # this data product is actively being produced
        # read the first frame of the file to determine the frame_size (this size is constant for the entire run)
        f = open(filepath, 'rb')
        logger.debug(f"{dp=}: {filepath=}: {d['bytes_per_image']=}")
        frame_size = pff.img_frame_size(f, d['bytes_per_image'])
        d['frame_size'] = frame_size

        f.seek(0, os.SEEK_SET)
        d['f'] = f
        d['nfiles'] = nfiles
        d['filepath'] = filepath
        d['last_frame'] = -1
        return True

    async def init_module_state(module_state, module_id, timeout=2.0):
        """initialize hp_io state for a specific module.
        Returns True iff successfully initialized."""
        curr_t = time.monotonic()
        dp_cfg = module_state['dp_cfg']

        # check if a directory for [module_id] exists
        run_pattern = f"{data_dir}/module_{module_id}/obs_*"
        runs = glob(run_pattern)
        nruns = len(runs)
        if nruns == 0:
            raise FileNotFoundError(f'no run of module {module_id} in {run_pattern}')
        run_path = sorted(runs, key=os.path.getmtime)[-1]
        module_state['run_path'] = run_path

        logger.info(f"checking {run_path=}")
        # automatically detect the subset of [data_products] that are actively being produced
        init_dp_tasks = []
        for dp in dp_cfg:
            init_dp_tasks.append(init_dp_state(dp_cfg[dp], dp, run_path, curr_t, timeout))
        results = await asyncio.gather(*init_dp_tasks)
        # remove modules that fail to initialize
        for result, dp in zip(results, data_products):
            if not result:
                logger.debug(f"module_{module_id}: no active files for {dp=}")
                del dp_cfg[dp]
        return len(dp_cfg) > 0

    async def init_hp_io_state():
        hp_io_state = dict()
        # check if daq software is active
        daq_valid = False
        for i in range(2):
            if is_daq_active(simulate_daq, sim_cfg=sim_cfg):
                daq_valid = True
                break
            await asyncio.sleep(0.5)
        if not daq_valid:
            raise EnvironmentError("DAQ data flow not active.")
        # Automatically detect all existing module directories
        module_dir_pattern = f"{data_dir}/module_*"
        module_dirs = glob(module_dir_pattern)
        module_ids = []
        for m in module_dirs:
            if os.path.isdir(m):
                mid = os.path.basename(m).split('_')[1]
                try:
                    mid = int(mid)
                except ValueError:
                    raise ValueError(f"module_id {mid} from {os.path.abspath(m)} is not a valid integer. Please check if the module_id is ")
                mid = int(mid)
                module_ids.append(mid)
        # if client has specified any module_ids, only attempt to track those module directories.
        if len(module_id_whitelist) > 0:
            abs_data_dir = os.path.abspath(data_dir)
            if set(module_id_whitelist).issubset(set(module_ids)):
                logger.info(f"all whitelisted module_ids {module_id_whitelist} have directories in "
                            f"data_dir='{abs_data_dir=}'")
            else:
                logger.warning(f"all module_ids {module_id_whitelist} do not have directories in {abs_data_dir=}. "
                               f"the following whitelisted module_ids do not have directories: "
                               f"{set(module_id_whitelist) - set(module_ids)}")
            module_ids = set(module_ids).intersection(module_id_whitelist)
        # initialize dictionaries to track data snapshots for each module directory
        init_module_tasks = []
        for mid in module_ids:
            hp_io_state[mid] = dict()
            hp_io_state[mid]['dp_cfg'] = get_dp_cfg(data_products)
            init_module_tasks.append(init_module_state(hp_io_state[mid], mid))
        results = await asyncio.gather(*init_module_tasks)
        # remove modules that fail to initialize
        active_dps = set()
        for result, mid in zip(results, module_ids):
            if not result:
                logger.warning(f"module {mid} failed to initialize")
                del hp_io_state[mid]
            else:
                # report which module_dirs are active
                dps = list(hp_io_state[mid]['dp_cfg'].keys())
                active_dps.update(dps)
                logger.info(f"tracking dps={dps} in run_dir='{hp_io_state[mid]['run_path']}'")
        # check environment
        if len(hp_io_state) == 0:
            raise EnvironmentError("No active modules found.")
        elif stop_io.is_set():
            raise EnvironmentError("stop_io event is set")
        elif not is_daq_active(simulate_daq, sim_cfg=sim_cfg):
            raise EnvironmentError("DAQ data flow stopped.")
        # report to _start_hp_io_task which data products are being tracked
        await dp_queue.put(active_dps)
        return hp_io_state

    def fetch_data_product_main(d: Dict[str, Any]) -> Tuple[Dict[str, Any], Tuple[int, ...]] or Tuple[None, None]:
        """
        Check if there is new pff data of type [dp].
        If new data is present:
            1. Update dp_cfg[dp] accordingly.
            2. return a tuple of (pff header, pff image).
        Otherwise, return (None, None)
        Note: this function mutates dp_cfg.
        """
        f = d['f']
        nfiles = d['nfiles']
        filepath = d['filepath']
        last_frame = d['last_frame']
        try:
            # check if a newer file for this data product has been created
            files = glob(d['glob_pat'])
            if len(files) > nfiles:
                nfiles = len(files)
                f.close()
                file = sorted(files, key=os.path.getmtime)[-1]
                filepath = file
                f = open(filepath, 'rb')
                last_frame = -1
            fsize = f.seek(0, os.SEEK_END)
            nframes = int(fsize / d['frame_size'])
            # check if any new frames have been written to this file
            if nframes > last_frame + 1:
                # seek to the latest frame in the file
                last_frame = nframes - 1
                f.seek(last_frame * d['frame_size'], os.SEEK_SET)

                # parse pff header and image
                try:
                    header_str = pff.read_json(f)
                    img = pff.read_image(f, d['image_shape'][0], d['bytes_per_pixel'])
                    # the check below is necessary to handle the rare case where a pff file has
                    # reached the max size specified in data_config.json resulting in no data for the last frame.
                    if header_str and img:
                        header = json.loads(header_str)
                        return header, img
                except Exception as e:
                    logger.error(f"Failed to read pff header and image from file {filepath} with error: {e}")
                    return None, None
            return None, None
        finally:
            # always update dp_cfg upon exit.
            # important for ensuring we always close any newly opened file pointers
            d['f'] = f
            d['nfiles'] = nfiles
            d['filepath'] = filepath
            d['last_frame'] = last_frame

    def get_ready_readers(curr_t: float) -> Tuple[List[Dict[str, Any]], int, int]:
        """Returns reader state dicts of all active readers that are ready to receive data from the hp_io task.
        curr_t must be a float returned by time.monotonic().
        """
        ready_list = []
        ph_clients = movie_clients = 0
        allocated_reader_states = [rs for rs in reader_states if rs['is_allocated']]
        for rs in allocated_reader_states:
            if rs['enqueue_timeouts'] >= max_reader_enqueue_timeouts:
                logger.warning(f"client {rs['client_ip']} is too slow, cancelling this client")
                continue
            elif (curr_t - rs['last_update_t']) >= rs['config']['update_interval_seconds']:
                ph_clients += rs['config']['stream_pulse_height_data']
                movie_clients += rs['config']['stream_movie_data']
                ready_list.append(rs)
        return ready_list, ph_clients, movie_clients


    def fetch_latest_module_data(
            module_state: Dict[str, Any],
            ready_ph_clients: int,
            ready_movie_clients: int
    ) -> Tuple[List[PanoImage], List[PanoImage]]:
        latest_ph_data: List[PanoImage] = []
        latest_movie_data: List[PanoImage] = []
        # logger.info(f"{module_id=}: {dp_cfg=}")
        dp_cfg = module_state['dp_cfg']
        for dp in dp_cfg:
            d = dp_cfg[dp]
            # check if we have to compute the latest for this datatype right now
            if d['is_ph'] and ready_ph_clients == 0:
                continue
            elif not d['is_ph'] and ready_movie_clients == 0:
                continue
            header, img = fetch_data_product_main(d)
            if header and img:
                # create PanoImage message from the latest image
                pano_image = PanoImage(
                    type=d['pano_image_type'],
                    header=ParseDict(header, Struct()),
                    image_array=img,
                    shape=d['image_shape'],
                    bytes_per_pixel=d['bytes_per_pixel'],
                    file=os.path.basename(d['filepath']),
                    frame_number=d['last_frame'],
                    module_id=module_id,
                )
                if d['is_ph']:
                    latest_ph_data.append(pano_image)
                else:
                    latest_movie_data.append(pano_image)
        return latest_ph_data, latest_movie_data

    hp_io_state: Dict[int, Dict[str, Any]] = None
    try:
        hp_io_state = await init_hp_io_state()
        logger.debug(f"{hp_io_state=}")
        # signal the hp_io task is ready to service client requests for data preview
        valid.set()
        last_daq_active_check_t = time.monotonic()
        last_sleep_t = time.monotonic()
        while not stop_io.is_set():
            # check if any active readers are ready to receive data
            curr_t = time.monotonic()
            ready_readers, nph, nmovie = get_ready_readers(curr_t)
            if len(ready_readers) > 0:
                data_to_broadcast = {}
                for module_id in hp_io_state:
                    data_to_broadcast[module_id] = {'ph': None, 'movie': None}
                    latest_module_ph_data, latest_module_movie_data = fetch_latest_module_data(hp_io_state[module_id], nph, nmovie)
                    data_to_broadcast[module_id]['ph'] = latest_module_ph_data
                    data_to_broadcast[module_id]['movie'] = latest_module_movie_data
                # broadcast image data to all ready clients
                for rs in ready_readers:
                    rq: asyncio.Queue = rs['queue']
                    try:
                        for module_id in data_to_broadcast:
                            # If no module_ids are specified by the client, send data from all modules
                            # Otherwise, only broadcast data from modules specified by the user.
                            if len(rs['config']['module_ids']) > 0 and module_id not in rs['config']['module_ids']:
                                continue
                            if rs['config']['stream_pulse_height_data']:
                                for ph_data in data_to_broadcast[module_id]['ph']:
                                    rq.put_nowait(ph_data)
                            if rs['config']['stream_movie_data']:
                                for movie_data in data_to_broadcast[module_id]['movie']:
                                    rq.put_nowait(movie_data)
                        rs['enqueue_timeouts'] = 0
                    except asyncio.QueueFull:
                        logger.warning(f"hp_io task is unable to broadcast image data to reader {rs['client_ip']}")
                        rs['enqueue_timeouts'] += 1
                    rs['last_update_t'] = curr_t
            # Every 10 seconds, check if the DAQ software stopped
            curr_t = time.monotonic()
            if curr_t - last_daq_active_check_t >= 10:
                last_daq_active_check_t = curr_t
                if not is_daq_active(simulate_daq, sim_cfg=sim_cfg):
                    logger.warning("DAQ data flow stopped.")
                    return
            curr_t = time.monotonic()
            sleep_duration = max(update_interval_seconds - (curr_t - last_sleep_t), 0)
            last_sleep_t = curr_t
            # if sleep_duration < update_interval_seconds * 1e-2:
            #     logger.warning(f"hp_io task is falling behind: {sleep_duration=} and {update_interval_seconds=}")
            await asyncio.sleep(sleep_duration)
    except Exception as err:
        logger.error(f"hp_io task encountered a fatal exception! '{repr(err)}'")
        raise
    finally:
        valid.clear()
        logger.info("hp_io task exited")
        # close any open file pointers
        if hp_io_state is not None:
            for module_id, module_state in hp_io_state.items():
                if 'dp_cfg' in module_state:
                    dp_cfg = module_state['dp_cfg']
                    for dp in dp_cfg:
                        if 'f' in dp_cfg[dp]:
                            dp_cfg[dp]['f'].close()
