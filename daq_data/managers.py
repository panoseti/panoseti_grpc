"""Classes for managing DaqData server state."""
import uuid
import asyncio
from contextlib import asynccontextmanager
from typing import List, Dict, Optional
import logging
import grpc

from .hp_io_manager import HpIoManager
from .state import ReaderState, DataProductState
from .simulate import SimulationManager

class HpIoTaskManager:
    """Manages the lifecycle of the HpIoManager background task."""

    def __init__(self, logger: logging.Logger, server_cfg: dict, reader_states: List[ReaderState]):
        self.logger = logger
        self.server_cfg = server_cfg
        self.reader_states = reader_states
        self.hp_io_task: Optional[asyncio.Task] = None
        self.hp_io_manager: Optional[HpIoManager] = None
        self.hp_io_valid_event = asyncio.Event()
        self.active_data_products = set()
        self.hp_io_cfg: Dict = {}
        self.stop_event = asyncio.Event()
        self.simulation_manager = SimulationManager(server_cfg, logger)

    def is_valid(self, verbose: bool = True) -> bool:
        """Checks if the hp_io task is running and considered valid."""
        if self.hp_io_task and not self.hp_io_task.done() and self.hp_io_valid_event.is_set():
            return True
        if verbose:
            if not self.hp_io_task: self.logger.warning("hp_io task is uninitialized")
            elif self.hp_io_task.done(): self.logger.warning("hp_io task is not alive")
            elif not self.hp_io_valid_event.is_set(): self.logger.warning("hp_io task is alive but not valid")
        return False

    async def start(self, hp_io_cfg: dict) -> bool:
        """Creates a new hp_io task. Stops any existing task first."""
        await self.stop()

        is_sim = hp_io_cfg.get('simulate_daq', False)
        sim_setup_task = None
        sim_cfg = self.server_cfg.get('simulate_daq_cfg', {})
        if is_sim:
            # Setup the simulation environment before HpIoManager starts.
            sim_setup_task = asyncio.create_task(self.simulation_manager.setup_environment())
            # Override data_dir to point to the simulation directory.
            sim_data_dir = sim_cfg.get('filesystem_cfg', {}).get('sim_data_dir')
            if sim_data_dir:
                self.logger.info(f"Simulation mode detected. Overriding data_dir to '{sim_data_dir}'.")
                hp_io_cfg['data_dir'] = sim_data_dir
                # hp_io_cfg['module_ids'] = tuple(sim_cfg.get('sim_module_ids', []))
        # Create the HpIoManager with the provided configuration
        self.hp_io_cfg = hp_io_cfg
        temp_server_cfg = self.server_cfg.copy()
        temp_server_cfg['hp_io_cfg'] = hp_io_cfg
        active_data_products_queue = asyncio.Queue()

        self.hp_io_manager = HpIoManager(temp_server_cfg, self.reader_states, self.stop_event,
                                         self.hp_io_valid_event, active_data_products_queue, self.logger)
        self.hp_io_task = asyncio.create_task(self.hp_io_manager.run())
        try:
            if sim_setup_task and 'filesystem' in sim_cfg.get("simulation_mode", ""):
                # Filesystem simulations need to be started BEFORE hp_io
                if not await sim_setup_task:
                    self.logger.critical("Failed to set up filesystem simulation environment. Aborting start.")
                    await self.simulation_manager.cleanup_environment()
                    await self.stop()
                    return False
                if not await self.simulation_manager.start_simulation_loop():
                    self.logger.error("Failed to start filesystem simulation loop after IO manager was ready.")
                    await self.stop()
                    return False

            await asyncio.wait_for(self.hp_io_valid_event.wait(), timeout=10.0)
            self.active_data_products = await active_data_products_queue.get()
            self.logger.info(f"hp_io task initialized with active_data_products={self.active_data_products}")

            if sim_setup_task and 'filesystem' not in sim_cfg.get("simulation_mode", ""):
                # Unix-domain simulations need to be started AFTER hp_io
                if not await sim_setup_task:
                    self.logger.critical("Failed to set up UDS simulation environment. Aborting start.")
                    await self.simulation_manager.cleanup_environment()
                    await self.stop()
                    return False
                if not await self.simulation_manager.start_simulation_loop():
                    self.logger.error("Failed to start UDS simulation loop after IO manager was ready.")
                    await self.stop()
                    return False
        except asyncio.TimeoutError:
            self.logger.error("Timeout waiting for hp_io task to become valid.")
            await self.stop()
            return False
        return self.is_valid(verbose=True)

    async def stop(self):
        """Stops the hp_io task and any associated simulation task gracefully."""
        # Stop the simulation data loop first
        await self.simulation_manager.stop_simulation_loop()

        if self.hp_io_task and not self.hp_io_task.done():
            self.logger.info("Stopping hp_io task...")
            self.stop_event.set()
            try:
                await asyncio.wait_for(self.hp_io_task, timeout=2.0)
                self.logger.info("Successfully terminated hp_io task.")
            except asyncio.TimeoutError:
                self.logger.warning("Timeout stopping hp_io task. Cancelling.")
                self.hp_io_task.cancel()
            except Exception as e:
                self.logger.error(f"Exception while stopping hp_io task: {e}", exc_info=True)
        
        # Clean up simulation resources after everything has stopped
        await self.simulation_manager.cleanup_environment()

        self.hp_io_task = None
        self.hp_io_manager = None
        self.active_data_products = set()
        self.hp_io_valid_event.clear()
        self.stop_event.clear()


class ClientManager:
    """Manages client connections, state, and access control for server resources."""
    def __init__(self, logger: logging.Logger, server_cfg: dict):
        self.logger = logger
        self.max_clients = server_cfg['max_concurrent_rpcs']
        self._cancel_readers_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        self._readers: List[ReaderState] = [
            ReaderState(
                queue=asyncio.Queue(maxsize=server_cfg['max_read_queue_size']),
                cancel_reader_event=self._cancel_readers_event,
                shutdown_event=self._shutdown_event,
            ) for _ in range(self.max_clients)
        ]
        self._active_readers = 0
        self._writer_active = False
        self._uploader_active = False

        # Locks to synchronize access to shared resources
        self._writer_lock = asyncio.Lock()
        self._uploader_lock = asyncio.Lock()
        self._readers_lock = asyncio.Lock() # Protects the _readers list and _active_readers count

    @property
    def reader_states(self) -> List[ReaderState]: return self._readers

    @property
    def cancel_readers_event(self): return self._cancel_readers_event

    @property
    def shutdown_event(self): return self._shutdown_event

    async def cancel_all_readers(self):
        """Signals all active reader streams to terminate."""
        self.logger.warning("Cancelling all active and waiting reader RPCs.")
        self._cancel_readers_event.set()

    def signal_shutdown(self):
        self._shutdown_event.set()

    @asynccontextmanager
    async def get_writer_access(self, context, force: bool = False):
        """A context manager to safely acquire exclusive 'writer' access."""
        uid = uuid.uuid4()
        
        # acquire writer lock **
        await self._writer_lock.acquire()
        try:
            self.logger.debug(f"Writer ({uid}) acquired writer lock.")
            # check for active readers and handle cancellation
            # async with self._readers_lock:
            if self._active_readers > 0:
                if not force:
                    active_ips = [rs.client_ip for rs in self._readers if rs.is_allocated]
                    msg = f"Cannot modify server state: {self._active_readers} clients are streaming: {active_ips}"
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
                else:
                    self.logger.warning("Forcing write access by cancelling all active readers.")
            await self.cancel_all_readers()
            yield uid
        finally:
            self._cancel_readers_event.clear()
            self._writer_lock.release()
            self.logger.debug(f"Writer ({uid}) released writer lock.")

    @asynccontextmanager
    async def get_reader_access(self, context, hp_io_task_manager: HpIoTaskManager):
        """A context manager to safely acquire a 'reader' slot."""
        uid = uuid.uuid4()
        
        # check for writer lock before proceeding **
        if self._writer_lock.locked() or self._cancel_readers_event.is_set():
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Server is being configured, please try again soon.")
            
        async with self._readers_lock:
            # Perform other pre-condition checks
            if self.shutdown_event.is_set():
                await context.abort(grpc.StatusCode.CANCELLED, "Server is shutting down.")
            if not hp_io_task_manager.is_valid():
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "hp_io task is not valid.")
            if self._active_readers >= self.max_clients:
                await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, f"Max clients ({self.max_clients}) reached.")
            
            rs_to_allocate = next((rs for rs in self._readers if not rs.is_allocated), None)
            if rs_to_allocate is None:
                await context.abort(grpc.StatusCode.INTERNAL, "Internal server error: no reader slots available.")
                return

            rs_to_allocate.is_allocated = True
            rs_to_allocate.client_ip = context.peer()
            rs_to_allocate.uid = uid
            self._active_readers += 1
            self.logger.info(f"Reader slot allocated for ({uid}). Active readers: {self._active_readers}")

        try:
            yield rs_to_allocate
        finally:
            async with self._readers_lock:
                if rs_to_allocate.is_allocated:
                    rs_to_allocate.reset()
                    self._active_readers -= 1
                    self.logger.info(f"Reader slot released for ({uid}). Active readers: {self._active_readers}")

    @asynccontextmanager
    async def get_uploader_access(self, context):
        """A context manager to safely acquire 'uploader' access."""
        uid = uuid.uuid4()

        # acquire the dedicated uploader lock and check writer lock **
        # if self._writer_lock.locked():
        #      await context.abort(grpc.StatusCode.ABORTED, "A writer is active, cannot accept uploads.")

        await self._uploader_lock.acquire()
        try:
            self.logger.debug(f"Uploader ({uid}) acquired uploader lock.")
            yield uid
        finally:
            self._uploader_lock.release()
            self.logger.debug(f"Uploader ({uid}) released uploader lock.")
