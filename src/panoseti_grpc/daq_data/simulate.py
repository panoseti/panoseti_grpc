"""
Manages the lifecycle of DAQ simulation tasks for the DaqData server.
Implements a Strategy pattern to handle different simulation modes.
"""
import abc
import asyncio
from io import BytesIO
import json
import logging
import os
import errno
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from importlib import resources

from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.generated.daq_data_pb2 import PanoImage
from panoseti_grpc.panoseti_util import pff

from .client import AioDaqDataClient
from .state import get_dp_config
from .resources import daq_data_anchor_package


class BaseSimulationStrategy(abc.ABC):
    """Abstract base class for a simulation strategy."""
    MAX_FILESYSTEM_SIM_FRAMES = 1_000

    def __init__(self, common_config: dict, strategy_config: dict, server_config: dict, logger: logging.Logger,
                 stop_event: asyncio.Event):
        self.logger = logger
        self.stop_event = stop_event
        self.common_config = common_config
        self.strategy_config = strategy_config
        self.server_config = server_config
        self.sim_created_resources = []
        self.movie_frames: List[bytes] = []
        self.ph_frames: List[bytes] = []

        self.frame_limit = self.strategy_config.get('frame_limit', -1)
        # Limit filesystem frame writing to preserve disk space
        if isinstance(self, FilesystemBaseStrategy):
            if not (0 <= self.frame_limit < self.MAX_FILESYSTEM_SIM_FRAMES):
                self.logger.warning(f"Overwriting filesystem simulation from "
                                    f"{self.frame_limit} to {self.MAX_FILESYSTEM_SIM_FRAMES} frames.")
                self.frame_limit = self.MAX_FILESYSTEM_SIM_FRAMES
            else:
                self.logger.info(f"Filesystem simulation will limit to {self.frame_limit} frames.")
        else:
            # Don't limit streaming because it only uses constant space
            if self.frame_limit < 0:
                self.frame_limit = float('inf')

    def _load_source_data(self):
        """Loads all PFF frames from source files into memory."""
        self.logger.info("Loading source data frames into memory for simulation.")
        source_cfg = self.common_config['source_data']
        dp_cfgs = get_dp_config([self.common_config['movie_type'], self.common_config['ph_type']])
        try:
            with resources.files(daq_data_anchor_package).joinpath(source_cfg['movie_pff_path']).open("rb") as f:
                dp_config = dp_cfgs[self.common_config['movie_type']]
                frame_size, nframes, _, _ = pff.img_info(f, dp_config.bytes_per_image)
                f.seek(0)
                for _ in range(nframes):
                    self.movie_frames.append(f.read(frame_size))
            #with open(source_cfg['ph_pff_path'], "rb") as f:
            with resources.files(daq_data_anchor_package).joinpath(source_cfg['ph_pff_path']).open("rb") as f:
                dp_config = dp_cfgs[self.common_config['ph_type']]
                frame_size, nframes, _, _ = pff.img_info(f, dp_config.bytes_per_image)
                f.seek(0)
                for _ in range(nframes):
                    self.ph_frames.append(f.read(frame_size))
            self.logger.info(f"Loaded {len(self.movie_frames)} movie and {len(self.ph_frames)} PH frames.")
        except FileNotFoundError as e:
            self.logger.error(f"Source PFF file not found: {e}. Cannot start simulation.")
        except Exception as e:
            self.logger.error(f"Error loading source data: {e}", exc_info=True)

    @abc.abstractmethod
    async def setup(self) -> bool:
        """Perform mode-specific setup (e.g., creating files, opening sockets)."""
        pass

    @abc.abstractmethod
    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        """Sends a single frame using the strategy's method (e.g., write to file, socket)."""
        pass

    @abc.abstractmethod
    async def cleanup(self):
        """Perform mode-specific cleanup."""
        pass

    async def run(self):
        """Main simulation loop. Assumes setup() and data loading have been completed."""
        self.logger.info(f"Starting simulation data loop with {self.__class__.__name__}")
        if not self.movie_frames or not self.ph_frames:
            self.logger.error("Source data not loaded, cannot run simulation loop.")
            return

        try:
            fnum = 0
            while not self.stop_event.is_set():
                if fnum >= self.frame_limit:
                    self.logger.warning(f"Frame limit reached ({self.frame_limit}), stopping simulation.")
                    break
                movie_frame = self.movie_frames[fnum % len(self.movie_frames)]
                ph_frame = self.ph_frames[fnum % len(self.ph_frames)]
                for mid in self.common_config['sim_module_ids']:
                    await self.send_frame(movie_frame, self.common_config['movie_type'], mid, fnum)
                    await self.send_frame(ph_frame, self.common_config['ph_type'], mid, fnum)
                fnum += 1
                await asyncio.sleep(self.common_config.get('update_interval_seconds', 0.1))
        except asyncio.CancelledError:
            self.logger.info(f"Simulation data loop for '{self.__class__.__name__}' cancelled.")
        except Exception as e:
            self.logger.error(f"Error in simulation loop for '{self.__class__.__name__}': {e}", exc_info=True)
        finally:
            self.logger.info(f"Simulation data loop for '{self.__class__.__name__}' finished.")


class FilesystemBaseStrategy(BaseSimulationStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pff_handles = {}
        self.module_states = {}

    async def setup(self) -> bool:
        """Shared setup logic for creating directories and rollover state."""
        fs_cfg = self.common_config['filesystem_cfg']
        try:
            for mid in self.common_config['sim_module_ids']:
                sim_run_dir = Path(fs_cfg['sim_data_dir']) / fs_cfg['sim_run_dir_template'].format(module_id=mid)
                os.makedirs(sim_run_dir, exist_ok=True)
                active_file = sim_run_dir / fs_cfg['daq_active_file'].format(module_id=mid)
                active_file.touch()
                self.sim_created_resources.append(str(active_file))
                self.module_states[mid] = {'seqno': 0, 'frames_written': 0}
                self.pff_handles[mid] = {}
                await self._rollover_pff_files(mid)
            return True
        except Exception as e:
            self.logger.error(f"Failed to setup filesystem simulation: {e}", exc_info=True)
            return False

    async def _rollover_pff_files(self, mid: int):
        state = self.module_states[mid]
        self.logger.info(f"Module {mid}: Rolling over to seqno {state['seqno']}.")
        if self.pff_handles.get(mid):
            for handle in self.pff_handles[mid].values():
                handle.close()
        fs_cfg = self.common_config['filesystem_cfg']
        base_path = Path(fs_cfg['sim_data_dir']) / fs_cfg['sim_run_dir_template'].format(module_id=mid)
        movie_path = base_path / f"sim.dp_{self.common_config['movie_type']}.module_{mid}.seqno_{state['seqno']}.pff"
        ph_path = base_path / f"sim.dp_{self.common_config['ph_type']}.module_{mid}.seqno_{state['seqno']}.pff"
        self.pff_handles[mid]['movie_f'] = open(movie_path, 'wb')
        self.pff_handles[mid]['ph_f'] = open(ph_path, 'wb')
        # self.logger.info(f"Module {mid}: Created new movie and PH files: {movie_path}, {ph_path}")
        self.sim_created_resources.extend([str(movie_path), str(ph_path)])
        state['frames_written'] = 0

    async def _write_frame_to_file(self, frame_data: bytes, data_product_type: str, module_id: int):
        state = self.module_states[module_id]
        key = 'movie_f' if 'img' in data_product_type else 'ph_f'
        if key == 'movie_f':
            if state['frames_written'] >= self.strategy_config.get('frames_per_pff', 1000):
                state['seqno'] += 1
                await self._rollover_pff_files(module_id)
            state['frames_written'] += 1
        handle = self.pff_handles[module_id][key]
        # self.logger.debug(f"Module {module_id}: Writing frame to {key} at seqno {state['seqno']}")
        handle.write(frame_data)
        handle.flush()

    async def cleanup(self):
        for mid, handles in self.pff_handles.items():
            for handle in handles.values():
                if not handle.closed:
                    handle.close()
        for fpath in self.sim_created_resources:
            try:
                if os.path.exists(fpath):
                    os.unlink(fpath)
            except Exception as e:
                self.logger.warning(f"Failed to clean up sim resource {fpath}: {e}")

class FilesystemPollStrategy(FilesystemBaseStrategy):
    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        # self.logger.debug(f"Module {module_id}: ABOUT TO WRITE  frame {frame_num} to file.")
        await self._write_frame_to_file(frame_data, data_product_type, module_id)
        # self.logger.debug(f"Module {module_id}: WROTE frame {frame_num} to file.")


class FilesystemPipeStrategy(FilesystemBaseStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipe_fds = {}
        self.read_status_pipe_name = self.server_config['read_status_pipe_name']

    async def setup(self) -> bool:
        self.logger.info("Setting up initial files and pipes for filesystem pipe simulation.")
        if not await super().setup():
            return False
        try:
            fs_cfg = self.common_config['filesystem_cfg']
            for mid in self.common_config['sim_module_ids']:
                sim_run_dir = Path(fs_cfg['sim_data_dir']) / fs_cfg['sim_run_dir_template'].format(module_id=mid)
                pipe_path = sim_run_dir / self.read_status_pipe_name
                if not os.path.exists(pipe_path):
                    os.mkfifo(pipe_path)
                self.sim_created_resources.append(str(pipe_path))
            return True
        except Exception as e:
            self.logger.error(f"Failed to create named pipes for simulation: {e}", exc_info=True)
            return False

    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        await self._write_frame_to_file(frame_data, data_product_type, module_id)
        try:
            fs_cfg = self.common_config['filesystem_cfg']
            if module_id not in self.pipe_fds:
                sim_run_dir = Path(fs_cfg['sim_data_dir']) / fs_cfg['sim_run_dir_template'].format(module_id=module_id)
                pipe_path = sim_run_dir / self.read_status_pipe_name
                self.pipe_fds[module_id] = os.open(pipe_path, os.O_WRONLY | os.O_NONBLOCK)
            msg = data_product_type.encode().ljust(10)
            os.write(self.pipe_fds[module_id], msg)
        except OSError as e:
            if e.errno == errno.ENXIO:
                if self.pipe_fds.get(module_id):
                    os.close(self.pipe_fds.pop(module_id))
            else:
                self.logger.warning(f"OSError writing to pipe for module {module_id}: {e}")
        except Exception as e:
            self.logger.warning(f"Error writing to pipe for module {module_id}: {e}")

    async def cleanup(self):
        await super().cleanup()
        for fd in self.pipe_fds.values():
            os.close(fd)


class UdsStrategy(BaseSimulationStrategy):
    """Simulates DAQ by connecting to UDS sockets and sending PFF frames (Client Role)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Dict: {dp_name: writer}
        self._writers: Dict[str, asyncio.StreamWriter] = {}

    async def setup(self, num_retries=5, retry_delay=0.5) -> bool:
        """Connects to the UDS sockets created by the main server."""
        self.logger.info("Setting up UDS simulation (Client Role).")
        uds_cfg = self.server_config.get("acquisition_methods", {}).get("uds", {})
        socket_template = uds_cfg.get("socket_path_template")
        if not socket_template:
            self.logger.error("UDS simulation requires 'socket_path_template'.")
            return False

        data_products = self.strategy_config.get('data_products', [])

        for i in range(num_retries):
            all_connected = True
            for dp_name in data_products:
                if dp_name in self._writers: continue  # Already connected

                socket_path = socket_template.format(dp_name=dp_name)
                try:
                    _, writer = await asyncio.open_unix_connection(socket_path)
                    self._writers[dp_name] = writer
                    self.logger.info(f"UDS sim: Connected to {socket_path}")
                except (ConnectionRefusedError, FileNotFoundError):
                    self.logger.warning(f"UDS sim (attempt {i + 1}/{num_retries}): Could not connect to {socket_path}.")
                    all_connected = False
                    break  # Break inner loop to retry all after a delay

            if all_connected:
                self.logger.info("UDS simulation connected to all target sockets.")
                return True

            await asyncio.sleep(retry_delay)

        self.logger.error("UDS sim failed to connect to all sockets.")
        return False

    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        """Sends [2-byte module_id][PFF frame] to the correct socket."""
        writer = self._writers.get(data_product_type)
        if not writer or writer.is_closing():
            if frame_num % 100 == 0:
                self.logger.debug(f"No active writer for {data_product_type}. Dropping frame.")
            return

        try:
            # 1. Pack the module_id into 2 bytes (big-endian)
            module_id_bytes = module_id.to_bytes(2, 'big')

            # 2. Write the prefix and the PFF frame
            writer.write(module_id_bytes)
            writer.write(frame_data)
            await writer.drain()
        except (BrokenPipeError, ConnectionResetError) as e:
            self.logger.warning(f"UDS sim connection lost for {data_product_type}: {e}.")
            self._writers.pop(data_product_type, None)  # Remove bad writer

    async def cleanup(self):
        self.logger.info("Closing all UDS simulation connections...")
        for writer in self._writers.values():
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()

class RpcStrategy(BaseSimulationStrategy):
    """Simulates DAQ by sending data via the UploadImages RPC."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client: AioDaqDataClient
        self._upload_queue = asyncio.Queue(maxsize=100)
        self._upload_task = None

    async def setup(self):
        self.logger.info("Setting up client for RPC simulation.")
        uds_addr = self.server_config.get("unix_domain_socket")
        if not uds_addr:
            self.logger.error("RPC simulation requires a 'unix_domain_socket' in server config.")
            self.stop_event.set()
            return False

        # Assuming client can use the UDS address directly
        daq_config = {'daq_nodes': [{'ip_addr': uds_addr}]}
        self.client = AioDaqDataClient(daq_config, network_config=None)
        await self.client.__aenter__()

        if not await self.client.ping(uds_addr):
            self.logger.error(f"RPC sim: Could not ping server at {uds_addr}. Aborting.")
            self.stop_event.set()
            return False

        self._upload_task = asyncio.create_task(
            self.client.upload_images([uds_addr], self._image_generator())
        )
        self.logger.info("RPC upload stream started.")
        return True

    async def _image_generator(self):
        """Async generator that yields images from the internal queue."""
        while not self.stop_event.is_set():
            try:
                pano_image = await asyncio.wait_for(self._upload_queue.get(), timeout=1.0)
                yield pano_image
                self._upload_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        # We need to parse the frame to create a PanoImage object
        dp_configs = get_dp_config([data_product_type])
        dp = dp_configs[data_product_type]

        frame_io = BytesIO(frame_data)
        # parse the JSON header
        header_str = pff.read_json(frame_io)
        if not header_str:
            self.logger.warning("Could not read JSON header from simulated frame data.")
            return
        header = json.loads(header_str)
        
        # Parse the image data
        img_array = pff.read_image(frame_io, dp.image_shape[0], dp.bytes_per_pixel)
        if img_array is None:
            self.logger.warning("Could not read image data from simulated frame data.")
            return

        # star_char_idx = len(frame_data) - dp.bytes_per_image - 1
        # header = json.load(BytesIO(frame_data[:star_char_idx]))
        # img_array = pff.read_image(BytesIO(frame_data[star_char_idx:]), dp.image_shape[0], dp.bytes_per_pixel)

        pano_image = PanoImage(
            type=dp.pano_image_type,
            header=ParseDict(header, Struct()),
            image_array=img_array,
            shape=dp.image_shape,
            bytes_per_pixel=dp.bytes_per_pixel,
            file=f"rpc_sim_{data_product_type}.pff",
            frame_number=frame_num,
            module_id=module_id
        )
        
        await self._upload_queue.put(pano_image)

    async def cleanup(self):
        self.logger.info("Cleaning up RPC simulation client.")
        if self._upload_task and not self._upload_task.done():
            self._upload_task.cancel()
            await asyncio.gather(self._upload_task, return_exceptions=True)
        if self.client:
            await self.client.__aexit__(None, None, None)


class SimulationManager:
    """Manages the lifecycle of a DAQ simulation task."""
    def __init__(self, server_cfg: dict, logger: logging.Logger):
        self.server_cfg = server_cfg
        self.logger = logger
        self.sim_task: Optional[asyncio.Task] = None
        self.strategy: Optional[BaseSimulationStrategy] = None
        self._sim_stop_event = asyncio.Event()

    def _get_strategy_class(self, mode: str):
        strategy_map = {
            "filesystem_poll": FilesystemPollStrategy,
            "filesystem_pipe": FilesystemPipeStrategy,
            "uds": UdsStrategy,
            "rpc": RpcStrategy, 
        }
        return strategy_map.get(mode)

    async def setup_environment(self) -> bool:
        """Sets up the simulation environment (files, pipes) but does not start the data loop."""
        sim_cfg = self.server_cfg.get('simulate_daq_cfg')
        if not sim_cfg:
            self.logger.error("`simulate_daq_cfg` not found in server configuration.")
            return False
        
        mode = sim_cfg.get("simulation_mode")
        StrategyClass = self._get_strategy_class(mode)
        if not StrategyClass:
            self.logger.error(f"Unknown or unsupported simulation mode: {mode}")
            return False

        self.logger.info(f"Setting up environment for '{mode}' simulation using {StrategyClass}.")
        strategy_config = sim_cfg.get('strategies', {}).get(mode, {})
        self.strategy: Union[FilesystemPollStrategy, FilesystemPipeStrategy, UdsStrategy, RpcStrategy] \
            = StrategyClass(sim_cfg, strategy_config, self.server_cfg, self.logger, self._sim_stop_event)

        self.strategy._load_source_data()
        if not await self.strategy.setup():
            self.logger.error("Simulation environment setup failed.")
            return False
        
        return True

    async def start_simulation_loop(self) -> bool:
        """Starts the main data generation loop for the simulation."""
        if not self.strategy:
            self.logger.error("Simulation strategy not initialized. Cannot start loop.")
            return False

        self.logger.info(f"Attempting to start simulation loop in '{self.strategy.__class__.__name__}' mode.")
        self._sim_stop_event.clear()
        self.sim_task = asyncio.create_task(self.strategy.run())

        # Wait briefly to see if the task fails or finishes immediately.
        await asyncio.sleep(0.2)

        if self.sim_task.done():
            try:
                # Check for an exception. If there isn't one, the task finished
                # cleanly, which is a valid outcome for short-running simulations.
                self.sim_task.result()
                self.logger.info(
                    f"Simulation task for mode '{self.strategy.__class__.__name__}' completed its run cleanly.")
            except Exception as e:
                self.logger.error(
                    f"Simulation task for mode '{self.strategy.__class__.__name__}' exited immediately with an error: {e}",
                    exc_info=True
                )
                self.sim_task = None
                return False

        self.logger.info("Simulation loop started successfully.")
        return True

    async def stop_simulation_loop(self):
        """Stops the data generation loop task."""
        if not self.sim_task or self.sim_task.done():
            return
        self.logger.info("Stopping simulation loop...")
        self._sim_stop_event.set()
        try:
            await asyncio.wait_for(self.sim_task, timeout=2.0)
            self.logger.info("Simulation loop stopped gracefully.")
        except asyncio.TimeoutError:
            self.logger.warning("Simulation loop did not stop gracefully. Cancelling.")
            self.sim_task.cancel()
        finally:
            self.sim_task = None

    async def cleanup_environment(self):
        """Cleans up any resources created by the simulation strategy."""
        if self.strategy:
            self.logger.info("Cleaning up simulation environment...")
            await self.strategy.cleanup()
            self.strategy = None

    def data_flow_valid(self) -> Optional[bool]:
        return self.sim_task and not self.sim_task.done()