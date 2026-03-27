"""
Manages the lifecycle of DAQ simulation tasks for the DaqData server.
Only the UDS (Unix Domain Socket) simulation mode is supported.
"""
from __future__ import annotations
import abc
import asyncio
from importlib import resources
import logging

from panoseti_grpc.panoseti_util import pff

from .config import DaqDataServerConfig, SimulateDaqConfig, UdsSimStrategyConfig
from .state import get_dp_config
from .resources import daq_data_anchor_package

class BaseSimulationStrategy(abc.ABC):
    """Abstract base class for a simulation strategy."""

    def __init__(self, common_config: SimulateDaqConfig, strategy_config: UdsSimStrategyConfig,
                 server_cfg: DaqDataServerConfig, logger: logging.Logger, stop_event: asyncio.Event):
        self.logger = logger
        self.stop_event = stop_event
        self.common_config = common_config
        self.strategy_config = strategy_config
        self.server_cfg = server_cfg
        self.sim_created_resources = []
        self.movie_frames: list[bytes] = []
        self.ph_frames: list[bytes] = []

        self.frame_limit = float('inf') if strategy_config.frame_limit < 0 else strategy_config.frame_limit

    def _load_source_data(self):
        """Loads all PFF frames from source files into memory."""
        self.logger.info("Loading source data frames into memory for simulation.")
        source_cfg = self.common_config.source_data
        dp_cfgs = get_dp_config([self.common_config.movie_type, self.common_config.ph_type])
        try:
            with resources.files(daq_data_anchor_package).joinpath(source_cfg.movie_pff_path).open("rb") as f:
                dp_config = dp_cfgs[self.common_config.movie_type]
                frame_size, nframes, _, _ = pff.img_info(f, dp_config.bytes_per_image)
                f.seek(0)
                for _ in range(nframes):
                    self.movie_frames.append(f.read(frame_size))
            with resources.files(daq_data_anchor_package).joinpath(source_cfg.ph_pff_path).open("rb") as f:
                dp_config = dp_cfgs[self.common_config.ph_type]
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
        """Perform mode-specific setup."""
        pass

    @abc.abstractmethod
    async def send_frame(self, frame_data: bytes, data_product_type: str, module_id: int, frame_num: int):
        """Sends a single frame using the strategy's method."""
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
                for mid in self.common_config.sim_module_ids:
                    await self.send_frame(movie_frame, self.common_config.movie_type, mid, fnum)
                    await self.send_frame(ph_frame, self.common_config.ph_type, mid, fnum)
                fnum += 1
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            self.logger.info(f"Simulation data loop for '{self.__class__.__name__}' cancelled.")
        except Exception as e:
            self.logger.error(f"Error in simulation loop for '{self.__class__.__name__}': {e}", exc_info=True)
        finally:
            self.logger.info(f"Simulation data loop for '{self.__class__.__name__}' finished.")

class UdsStrategy(BaseSimulationStrategy):
    """Simulates DAQ by connecting to UDS sockets and sending PFF frames (Client Role)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._writers: dict[str, asyncio.StreamWriter] = {}

    async def setup(self, num_retries=5, retry_delay=0.5) -> bool:
        """Connects to the UDS sockets created by the main server."""
        self.logger.info("Setting up UDS simulation (Client Role).")
        uds_cfg = self.server_cfg.acquisition_methods.uds
        socket_template = uds_cfg.socket_path_template
        data_products = self.strategy_config.data_products

        for i in range(num_retries):
            all_connected = True
            for dp_name in data_products:
                if dp_name in self._writers:
                    continue  # Already connected

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
            module_id_bytes = module_id.to_bytes(2, 'big')
            writer.write(module_id_bytes)
            writer.write(frame_data)
            await writer.drain()
        except (BrokenPipeError, ConnectionResetError) as e:
            self.logger.warning(f"UDS sim connection lost for {data_product_type}: {e}.")
            self._writers.pop(data_product_type, None)

    async def cleanup(self):
        self.logger.info("Closing all UDS simulation connections...")
        for writer in self._writers.values():
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()

class SimulationManager:
    """Manages the lifecycle of a DAQ simulation task."""
    def __init__(self, server_cfg: DaqDataServerConfig, logger: logging.Logger):
        self.server_cfg = server_cfg
        self.logger = logger
        self.sim_task: asyncio.Task | None = None
        self.strategy: BaseSimulationStrategy | None = None
        self._sim_stop_event = asyncio.Event()

    async def setup_environment(self) -> bool:
        """Sets up the simulation environment but does not start the data loop."""
        sim_cfg = self.server_cfg.simulate_daq_cfg
        if not sim_cfg:
            self.logger.error("`simulate_daq_cfg` not found in server configuration.")
            return False

        if sim_cfg.simulation_mode != "uds":
            self.logger.error(f"Unsupported simulation mode: '{sim_cfg.simulation_mode}'. Only 'uds' is supported.")
            return False

        self.logger.info("Setting up environment for 'uds' simulation.")
        strategy_config = sim_cfg.strategies.get('uds', UdsSimStrategyConfig())
        self.strategy = UdsStrategy(sim_cfg, strategy_config, self.server_cfg, self.logger, self._sim_stop_event)

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

        self.logger.info("Attempting to start simulation loop in 'uds' mode.")
        self._sim_stop_event.clear()
        self.sim_task = asyncio.create_task(self.strategy.run())

        # Wait briefly to see if the task fails or finishes immediately.
        await asyncio.sleep(0.2)

        if self.sim_task.done():
            try:
                self.sim_task.result()
                self.logger.info("Simulation task completed its run cleanly.")
            except Exception as e:
                self.logger.error(
                    f"Simulation task exited immediately with an error: {e}", exc_info=True
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

    def data_flow_valid(self) -> bool | None:
        return self.sim_task and not self.sim_task.done()
