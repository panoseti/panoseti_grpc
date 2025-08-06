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
import stat
from pathlib import Path
from typing import Any, Dict, List

from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from .client import AioDaqDataClient
from .daq_data_pb2 import PanoImage
from .resources import get_dp_config
from panoseti_util import pff


class BaseSimulationStrategy(abc.ABC):
    """Abstract base class for a simulation strategy."""

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

    def _load_source_data(self):
        """Loads all PFF frames from source files into memory. This is shared logic."""
        self.logger.info("Loading source data frames into memory for simulation.")
        source_cfg = self.common_config['source_data']
        dp_cfgs = get_dp_config([self.common_config['movie_type'], self.common_config['ph_type']])

        try:
            # Load movie frames
            with open(source_cfg['movie_pff_path'], "rb") as f:
                dp_config = dp_cfgs[self.common_config['movie_type']]
                frame_size, nframes, _, _ = pff.img_info(f, dp_config.bytes_per_image)
                f.seek(0)
                for _ in range(nframes):
                    self.movie_frames.append(f.read(frame_size))

            # Load pulse-height frames
            with open(source_cfg['ph_pff_path'], "rb") as f:
                dp_config = dp_cfgs[self.common_config['ph_type']]
                frame_size, nframes, _, _ = pff.img_info(f, dp_config.bytes_per_image)
                f.seek(0)
                for _ in range(nframes):
                    self.ph_frames.append(f.read(frame_size))

            self.logger.info(f"Loaded {len(self.movie_frames)} movie and {len(self.ph_frames)} PH frames.")
        except FileNotFoundError as e:
            self.logger.error(f"Source PFF file not found: {e}. Cannot start simulation.")
            self.movie_frames, self.ph_frames = [], []
        except Exception as e:
            self.logger.error(f"Error loading source data: {e}", exc_info=True)
            self.movie_frames, self.ph_frames = [], []


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
        """Main simulation loop. This is shared logic."""
        self.logger.info(f"Starting simulation with '{self.__class__.__name__}'.")
        self._load_source_data()

        if not self.movie_frames or not self.ph_frames:
            self.logger.error("Source data not loaded, cannot run simulation.")
            return

        if not await self.setup():
            self.logger.error("Simulation setup failed. Aborting run.")
            await self.cleanup()
            return

        fnum = 0
        try:
            while not self.stop_event.is_set():
                movie_frame = self.movie_frames[fnum % len(self.movie_frames)]
                ph_frame = self.ph_frames[fnum % len(self.ph_frames)]

                for mid in self.common_config['sim_module_ids']:
                    await self.send_frame(movie_frame, self.common_config['movie_type'], mid, fnum)
                    await self.send_frame(ph_frame, self.common_config['ph_type'], mid, fnum)

                fnum += 1
                await asyncio.sleep(self.common_config.get('update_interval_seconds', 0.1))

        except asyncio.CancelledError:
            self.logger.info(f"Simulation strategy '{self.__class__.__name__}' cancelled.")
        except Exception as e:
            self.logger.error(f"Error in simulation loop for '{self.__class__.__name__}': {e}", exc_info=True)
        finally:
            await self.cleanup()
            self.logger.info(f"Simulation strategy '{self.__class__.__name__}' finished.")


class FilesystemStrategy(BaseSimulationStrategy):
    """Simulates DAQ by writing to files and signaling via a named pipe."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipe_fds = {}
        self.pff_handles = {}
        self.module_states = {}
        self.read_status_pipe_name = self.server_config['read_status_pipe_name']

    async def setup(self) -> bool:
        self.logger.info("Setting up initial files and pipes for filesystem simulation.")
        fs_cfg = self.strategy_config
        try:
            for mid in self.common_config['sim_module_ids']:
                sim_run_dir = Path(fs_cfg['sim_data_dir']) / fs_cfg['sim_run_dir_template'].format(module_id=mid)
                os.makedirs(sim_run_dir, exist_ok=True)
                
                active_file = sim_run_dir / fs_cfg['daq_active_file'].format(module_id=mid)
                active_file.touch()
                self.sim_created_resources.append(str(active_file))

                pipe_path = sim_run_dir / self.read_status_pipe_name
                if not os.path.exists(pipe_path):
                    os.mkfifo(pipe_path)
                self.sim_created_resources.append(str(pipe_path))

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

        if self.pff_handles[mid]:
            for handle in self.pff_handles[mid].values():
                handle.close()

        base_path = Path(self.strategy_config['sim_data_dir']) / self.strategy_config['sim_run_dir_template'].format(module_id=mid)
        movie_path = base_path / f"sim.dp_{self.common_config['movie_type']}.module_{mid}.seqno_{state['seqno']}.pff"
        ph_path = base_path / f"sim.dp_{self.common_config['ph_type']}.module_{mid}.seqno_{state['seqno']}.pff"

        self.pff_handles[mid]['movie_f'] = open(movie_path, 'wb')
        self.pff_handles[mid]['ph_f'] = open(ph_path, 'wb')
        
        self.sim_created_resources.extend([str(movie_path), str(ph_path)])
        state['frames_written'] = 0

    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        state = self.module_states[module_id]
        
        if 'img' in data_product_type:
            key = 'movie_f'
        else:
            key = 'ph_f'
            # Only increment frames_written for one of the data types to count pairs
            if state['frames_written'] >= self.strategy_config.get('frames_per_pff', 1000):
                state['seqno'] += 1
                await self._rollover_pff_files(module_id)
            state['frames_written'] += 1
        
        handle = self.pff_handles[module_id][key]
        handle.write(frame_data)
        handle.flush()

        try:
            if module_id not in self.pipe_fds:
                sim_run_dir = Path(self.strategy_config['sim_data_dir']) / self.strategy_config['sim_run_dir_template'].format(module_id=module_id)
                pipe_path = sim_run_dir / self.read_status_pipe_name
                self.pipe_fds[module_id] = os.open(pipe_path, os.O_WRONLY | os.O_NONBLOCK)
            os.write(self.pipe_fds[module_id], b'1')
        except OSError:
            self.logger.debug(f"Pipe for module {module_id} not open for reading yet. Skipping signal.")
        except Exception as e:
            self.logger.warning(f"Error writing to pipe for module {module_id}: {e}")

    async def cleanup(self):
        self.logger.info("Cleaning up filesystem simulation resources...")
        for mid, handles in self.pff_handles.items():
            for handle in handles.values():
                if not handle.closed:
                    handle.close()
        for fd in self.pipe_fds.values():
            os.close(fd)
        
        for fpath in self.sim_created_resources:
            try:
                if os.path.exists(fpath):
                    os.unlink(fpath)
            except Exception as e:
                self.logger.warning(f"Failed to clean up sim resource {fpath}: {e}")


class UdsStrategy(BaseSimulationStrategy):
    """Simulates DAQ by writing PFF frames to Unix Domain Sockets."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._writers: Dict[str, asyncio.StreamWriter] = {}

    async def setup(self) -> bool:
        self.logger.info("Connecting to UDS sockets for simulation.")
        await asyncio.sleep(0.5)
        for dp_name in self.strategy_config['data_products']:
            socket_path = f"/tmp/hashpipe_grpc_{dp_name}.sock"
            try:
                _, writer = await asyncio.open_unix_connection(socket_path)
                self._writers[dp_name] = writer
                self.logger.info(f"UDS sim: Connected to {socket_path}")
            except (ConnectionRefusedError, FileNotFoundError):
                self.logger.error(f"UDS sim: Could not connect to {socket_path}. Is server running?")
                return False
            except Exception as e:
                self.logger.error(f"UDS sim: Unexpected error connecting to {socket_path}: {e}")
                return False
        return True

    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        if data_product_type in self._writers:
            writer = self._writers[data_product_type]
            try:
                if writer.is_closing():
                    self.logger.warning(f"UDS writer for {data_product_type} is closed. Stopping simulation.")
                    self.stop_event.set()
                    return
                writer.write(frame_data)
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError) as e:
                self.logger.warning(f"UDS sim connection lost for {data_product_type}: {e}. Stopping.")
                self.stop_event.set()

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

        # In a real scenario, this would be a more robust parse
        # For simulation, we assume a static header
        header = {"simulated": True, "frame": frame_num}
        img = pff.read_image(BytesIO(frame_data), dp.image_shape[0], dp.bytes_per_pixel)#list(frame_data[frame_data.find(b'*') + 1:]) # Simplified parsing

        pano_image = PanoImage(
            type=dp.pano_image_type,
            header=ParseDict(header, Struct()),
            image_array=img,
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
        self.sim_task: asyncio.Task = None
        self._sim_stop_event = asyncio.Event()

    async def start(self) -> bool:
        await self.stop()
        self._sim_stop_event.clear()

        sim_cfg = self.server_cfg.get('simulate_daq_cfg')
        if not sim_cfg:
            self.logger.error("`simulate_daq_cfg` not found in server configuration.")
            return False

        mode = sim_cfg.get("simulation_mode")
        self.logger.info(f"Attempting to start simulation in '{mode}' mode.")

        strategy_map = {
            "filesystem": FilesystemStrategy,
            "uds": UdsStrategy,
            "rpc": RpcStrategy,
        }

        if mode not in strategy_map:
            self.logger.error(f"Unknown simulation mode: {mode}")
            return False

        StrategyClass = strategy_map[mode]
        strategy_config = sim_cfg.get('strategies', {}).get(mode, {})
        strategy = StrategyClass(sim_cfg, strategy_config, self.server_cfg, self.logger, self._sim_stop_event)

        self.sim_task = asyncio.create_task(strategy.run())
        await asyncio.sleep(0.1)
        
        if self.sim_task.done():
            self.logger.error(f"Simulation task for mode '{mode}' exited immediately after starting.")
            try:
                await self.sim_task # Await to raise any exceptions from startup
            except Exception as e:
                self.logger.error(f"Exception from failed simulation task: {e}", exc_info=True)
            self.sim_task = None
            return False

        self.logger.info(f"Simulation in '{mode}' mode started successfully.")
        return True

    async def stop(self):
        if not self.sim_task or self.sim_task.done():
            return

        self.logger.info("Stopping simulation task...")
        self._sim_stop_event.set()
        try:
            await asyncio.wait_for(self.sim_task, timeout=5.0)
            self.logger.info("Simulation task stopped gracefully.")
        except asyncio.TimeoutError:
            self.logger.warning("Simulation task did not stop gracefully. Cancelling.")
            self.sim_task.cancel()
        except Exception as e:
            self.logger.error(f"Exception while stopping simulation task: {e}", exc_info=True)
        finally:
            self.sim_task = None

    def data_flow_valid(self) -> bool:
        return self.sim_task and not self.sim_task.done()