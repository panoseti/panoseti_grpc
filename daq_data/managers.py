"""Classes for managing DaqData server state."""
import uuid
import os
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import List, Dict
import time
import logging
import urllib.parse
from pathlib import Path
import grpc

from .resources import ReaderState
from .hp_io_manager import HpIoManager
from .sim_thread import daq_sim_thread_fn
from .state import ReaderState, DataProductConfig
from .simulate import SimulationManager

class HpIoTaskManager:
    """Manages the lifecycle of the HpIoManager background task."""

    def __init__(self, logger: logging.Logger, server_cfg: dict, reader_states: List[ReaderState]):
        self.logger = logger
        self.server_cfg = server_cfg
        self.reader_states = reader_states
        self.hp_io_task: asyncio.Task = None
        self.hp_io_manager: HpIoManager = None
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
        self.hp_io_cfg = hp_io_cfg
        
        # Pass the specific hp_io_cfg to the server_cfg for the manager to use
        temp_server_cfg = self.server_cfg.copy()
        temp_server_cfg['hp_io_cfg'] = hp_io_cfg

        active_data_products_queue = asyncio.Queue()
        self.hp_io_manager = HpIoManager(temp_server_cfg, self.reader_states, self.stop_event,
                                         self.hp_io_valid_event, active_data_products_queue, self.logger)
        self.hp_io_task = asyncio.create_task(self.hp_io_manager.run())

        try:
            # Wait for the IO task to start its data sources and become valid.
            await asyncio.wait_for(self.hp_io_valid_event.wait(), timeout=5.0)
            
            # Now that IO task is ready, get the active data products.
            self.active_data_products = await active_data_products_queue.get()
            self.logger.info(f"hp_io task initialized with active_data_products={self.active_data_products}")

        except asyncio.TimeoutError:
            self.logger.error("Timeout waiting for hp_io task to become valid.")
            await self.stop()
            return False

        if hp_io_cfg['simulate_daq']:
            if not await self.simulation_manager.start():
                self.logger.error("Failed to start simulation manager after IO manager was ready.")
                await self.stop() # Stop the IO manager if simulation fails to start
                return False
            await asyncio.sleep(0.2) # Brief pause to ensure sim has started
        
        # Final validation
        acq_config = self.server_cfg.get("acquisition_methods", {})
        is_fs_mode = acq_config.get("filesystem_poll", {}).get("enabled") or acq_config.get("filesystem_pipe", {}).get("enabled")
        if is_fs_mode and not self.active_data_products and not acq_config.get("uds", {}).get("enabled"):
            self.logger.error("hp_io task failed validation: no active data products found in filesystem mode and UDS is disabled.")
            await self.stop()
            return False
            
        return self.is_valid(verbose=True)

    async def stop(self):
        """Stops the hp_io task and any associated simulation task gracefully."""
        if self.simulation_manager.data_flow_valid():
            await self.simulation_manager.stop()

        if not self.hp_io_task: return
        if not self.hp_io_task.done():
            self.logger.info("Stopping hp_io task...")
            self.stop_event.set()
            try:
                await asyncio.wait_for(self.hp_io_task, timeout=5.0)
                self.logger.info("Successfully terminated hp_io task.")
            except asyncio.TimeoutError:
                self.logger.warning("Timeout stopping hp_io task. Cancelling.")
                self.hp_io_task.cancel()
            except Exception as e:
                self.logger.error(f"Exception while stopping hp_io task: {e}", exc_info=True)
        
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
        self._lock = asyncio.Lock()
        self._readers: List[ReaderState] = [
            ReaderState(
                queue=asyncio.Queue(maxsize=server_cfg['max_read_queue_size']),
                cancel_reader_event=self._cancel_readers_event,
                shutdown_event=self._shutdown_event,
            )
            for _ in range(self.max_clients)
        ]
        self._active_readers = 0
        self._writer_active = False
        self._uploader_active = False

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
        async with self._lock:
            for rs in self._readers:
                if rs.is_allocated:
                    try:
                        rs.queue.put_nowait("shutdown")
                    except asyncio.QueueFull:
                        pass # The reader will time out or see the event anyway

    def signal_shutdown(self):
        self._shutdown_event.set()

    @asynccontextmanager
    async def get_writer_access(self, context, hp_io_task_manager: HpIoTaskManager, force: bool = False):
        """A context manager to safely acquire exclusive 'writer' access."""
        uid = uuid.uuid4()
        peer = urllib.parse.unquote(context.peer())
        async with self._lock:
            self.logger.debug(f"Writer ({uid}) from '{peer=}' trying to acquire lock. Active readers: {self._active_readers}")
            if self._writer_active:
                 await context.abort(grpc.StatusCode.ABORTED, f"Another writer is already active.")

            if self._active_readers > 0:
                if not force:
                    active_client_ips = [rs.client_ip for rs in self._readers if rs.is_allocated]
                    emsg = (f"Cannot modify server state: {self._active_readers} clients are streaming. "
                            f"Set force=True or stop clients: {active_client_ips}")
                    self.logger.warning(emsg)
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
                else:
                    self.logger.warning("Forcing write access by cancelling all active readers.")
                    await self.cancel_all_readers()

            self._writer_active = True
            self.logger.debug(f"Writer ({uid}) lock acquired.")

        try:
            yield uid
        finally:
            async with self._lock:
                self._writer_active = False
                self._cancel_readers_event.clear() # Allow new readers
                self.logger.debug(f"Writer ({uid}) lock released.")

    @asynccontextmanager
    async def get_reader_access(self, context, hp_io_task_manager: HpIoTaskManager):
        """A context manager to safely acquire a 'reader' slot."""
        uid = uuid.uuid4()
        peer = urllib.parse.unquote(context.peer())
        async with self._lock:
            self.logger.debug(f"Reader {uid} from '{peer}' trying to acquire lock.")
            # Perform all pre-condition checks inside the lock
            if self._shutdown_event.is_set():
                await context.abort(grpc.StatusCode.CANCELLED, "Server is shutting down.")
            if self._writer_active or self._cancel_readers_event.is_set():
                await context.abort(grpc.StatusCode.UNAVAILABLE, "Server is being configured, please try again soon.")
            if not hp_io_task_manager.is_valid():
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                                    "hp_io task is not initialized or has become invalid.")
            if self._active_readers >= self.max_clients:
                await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
                                    f"Max number of clients ({self.max_clients}) reached.")

            rs_to_allocate = next((rs for rs in self._readers if not rs.is_allocated), None)
            if rs_to_allocate is None:
                self.logger.critical("Mismatch between active_readers count and allocated slots!")
                await context.abort(grpc.StatusCode.INTERNAL, "Internal server error: no reader slots available.")
                return

            # Allocation successful
            self._active_readers += 1
            rs_to_allocate.is_allocated = True
            rs_to_allocate.client_ip = peer
            rs_to_allocate.uid = uid
            self.logger.info( f"Reader slot allocated for ({uid}) from "
                              f"'{rs_to_allocate.client_ip}'. Active readers: {self._active_readers}")
        try:
            yield rs_to_allocate
        finally:
            async with self._lock:
                if rs_to_allocate and rs_to_allocate.is_allocated:
                    self.logger.info(
                        f"Releasing reader slot for ({uid}) from '{rs_to_allocate.client_ip}'. "
                        f"Active readers: {self._active_readers - 1}")
                    rs_to_allocate.reset()
                    self._active_readers -= 1

    @asynccontextmanager
    async def get_uploader_access(self, context, hp_io_task_manager: HpIoTaskManager):
        """A context manager to safely acquire 'uploader' access."""
        uid = uuid.uuid4()
        peer = urllib.parse.unquote(context.peer())
        async with self._lock:
            self.logger.debug(f"Uploader ({uid}) from '{peer}' trying to acquire lock. Active readers: {self._active_readers}")
            simulate_upload = hp_io_task_manager.hp_io_cfg.get('simulate_daq', False)
            simulate_upload = True
            if not simulate_upload and self._writer_active:
                await context.abort(grpc.StatusCode.ABORTED, "A writer is active.")
            if self._uploader_active: await context.abort(grpc.StatusCode.ABORTED, "An uploader is already active.")
            self._uploader_active = True
        try:
            yield uid
        finally:
            async with self._lock:
                self._uploader_active = False
