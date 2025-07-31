"""Classes for managing DaqData server state."""

import os
import asyncio
from contextlib import asynccontextmanager

from dataclasses import dataclass, field
from typing import List, Dict
import time
import logging
import urllib.parse

from pathlib import Path
from threading import Thread, Event

import grpc

from .resources import ReaderState
from .hp_io_manager import HpIoManager
from .sim_thread import daq_sim_thread_fn
from .state import ReaderState, DataProductConfig

class HpIoTaskManager:
    """Manages the lifecycle of the HpIoManager background task."""

    def __init__(self, logger: logging.Logger, server_cfg: dict, reader_states: List[ReaderState]):
        self.logger = logger
        self.server_cfg = server_cfg
        self.reader_states = reader_states  # HpIoManager needs direct access to this

        self.hp_io_task: asyncio.Task or None = None
        self.daq_sim_thread: Thread or None = None

        self.stop_event = Event()
        self.hp_io_valid_event = asyncio.Event()
        self.daq_sim_thread_valid_event = Event()

        self.active_data_products = set()
        self.hp_io_cfg = {}

    def is_valid(self, verbose: bool = True) -> bool:
        """Checks if the hp_io task is running and considered valid."""
        if self.hp_io_task and not self.hp_io_task.done() and self.hp_io_valid_event.is_set():
            return True

        if verbose:
            if self.hp_io_task is None:
                self.logger.warning("hp_io task is uninitialized")
            elif self.hp_io_task.done():
                self.logger.warning("hp_io task is not alive")
            elif not self.hp_io_valid_event.is_set():
                self.logger.warning("hp_io task is alive but not valid")
        return False

    async def start(self, hp_io_cfg: dict) -> bool:
        """Creates a new hp_io task. Stops any existing task first."""
        await self.stop()  # Ensure any previous task is stopped
        self.stop_event.clear()

        hp_io_update_interval = max(
            hp_io_cfg['update_interval_seconds'],
            self.server_cfg['min_hp_io_update_interval_seconds']
        )

        simulate_daq_cfg = self.server_cfg['simulate_daq_cfg']
        data_dir = hp_io_cfg['data_dir']

        if hp_io_cfg['simulate_daq']:
            data_dir = simulate_daq_cfg['files']['data_dir']
            abs_data_dir = os.path.abspath(data_dir)
            self.logger.info(f"Starting simulated DAQ flow to {abs_data_dir=}")
            self.daq_sim_thread = Thread(
                target=daq_sim_thread_fn,
                args=(simulate_daq_cfg.copy(), hp_io_update_interval / 2, self.stop_event,
                      self.daq_sim_thread_valid_event, self.logger)
            )
            self.daq_sim_thread.start()
            await asyncio.to_thread(self.daq_sim_thread_valid_event.wait)
        else:
            if not await asyncio.to_thread(os.path.exists, data_dir):
                self.logger.error(f"Cannot start HpIo task: data_dir '{data_dir}' does not exist.")
                return False
            self.daq_sim_thread = None

        active_data_products_queue = asyncio.Queue()
        self.hp_io_task = asyncio.create_task(
            HpIoManager(
                Path(data_dir),
                self.server_cfg['valid_data_products'],
                hp_io_update_interval,
                hp_io_cfg['module_ids'],
                hp_io_cfg['simulate_daq'],
                self.reader_states,
                self.stop_event,
                self.hp_io_valid_event,
                self.server_cfg['max_reader_enqueue_timeouts'],
                active_data_products_queue,
                self.logger,
                simulate_daq_cfg
            ).run()
        )

        try:
            init_tasks = [self.hp_io_valid_event.wait(), active_data_products_queue.get()]
            _, self.active_data_products = await asyncio.wait_for(asyncio.gather(*init_tasks), timeout=10)
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout waiting for hp_io task to become valid.")
            await self.stop()
            return False

        if not self.is_valid(verbose=False):
            self.logger.error("hp_io task failed to become valid after startup.")
            await self.stop()
            return False

        self.logger.info(f"hp_io task is valid with active_data_products={self.active_data_products}")
        self.hp_io_cfg = hp_io_cfg
        return True

    async def stop(self):
        """Stops the hp_io task and any associated simulation thread gracefully."""
        if not self.hp_io_task and not self.daq_sim_thread:
            self.logger.debug("No hp_io task or DAQ sim thread to stop.")
            return

        self.stop_event.set()
        if self.hp_io_task and not self.hp_io_task.done():
            self.logger.info("Stopping hp_io task...")
            try:
                await self.hp_io_task
                self.logger.info("Successfully terminated hp_io task.")
            except Exception as e:
                self.logger.critical(f"Exception while stopping hp_io task: {e}", exc_info=True)

        if self.daq_sim_thread and self.daq_sim_thread.is_alive():
            self.logger.info("Stopping DAQ simulation thread...")
            await asyncio.to_thread(self.daq_sim_thread.join)
            self.logger.info("Successfully terminated DAQ simulation thread.")

        self.hp_io_task = None
        self.daq_sim_thread = None
        self.active_data_products = set()
        self.hp_io_valid_event.clear()


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


    @property
    def reader_states(self) -> List[ReaderState]:
        """Returns the list of reader states."""
        return self._readers

    async def cancel_all_readers(self):
        """Signals all active reader streams to terminate."""
        self.logger.warning("Cancelling all active and waiting reader RPCs.")
        self._cancel_readers_event.set()
        async with self._lock:
            for rs in self._readers:
                if rs.is_allocated:
                    try:
                        # Put a special message to unblock any waiting queue.get()
                        rs.queue.put_nowait("shutdown")
                    except asyncio.QueueFull:
                        pass  # The reader will time out or see the event anyway

    def signal_shutdown(self):
        """Signals that the server is shutting down."""
        self._shutdown_event.set()

    @asynccontextmanager
    async def get_writer_access(self, context, hp_io_task_manager: HpIoTaskManager, force: bool = False):
        """A context manager to safely acquire exclusive 'writer' access."""
        async with self._lock:
            self.logger.debug(f"Writer trying to acquire lock. Active readers: {self._active_readers}")
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
                    # The readers will release their slots shortly, but we hold the lock.

            self._writer_active = True
            self.logger.debug("Writer lock acquired.")

        try:
            yield
        finally:
            async with self._lock:
                self._cancel_readers_event.clear()  # Allow new readers
                self._writer_active = False
                self.logger.debug("Writer lock released.")

    @asynccontextmanager
    async def get_reader_access(self, context, hp_io_task_manager: HpIoTaskManager):
        """A context manager to safely acquire a 'reader' slot."""
        allocated_reader_state: ReaderState or None = None
        async with self._lock:
            self.logger.debug("Reader trying to acquire lock.")
            # --- Perform all pre-condition checks inside the lock ---
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

            # Find and allocate a free reader slot
            for rs in self._readers:
                if not rs.is_allocated:
                    allocated_reader_state = rs
                    break

            if allocated_reader_state is None:
                # Should not happen if _active_readers count is correct, but is a good safeguard.
                self.logger.critical("Mismatch between active_readers count and allocated slots!")
                await context.abort(grpc.StatusCode.INTERNAL, "Internal server error: no reader slots available.")

            # --- Allocation successful ---
            self._active_readers += 1
            allocated_reader_state.is_allocated = True
            allocated_reader_state.client_ip = urllib.parse.unquote(context.peer())
            self.logger.info(
                f"Reader slot allocated for {allocated_reader_state.client_ip}. Active readers: {self._active_readers}")

        try:
            yield allocated_reader_state
        finally:
            async with self._lock:
                if allocated_reader_state and allocated_reader_state.is_allocated:
                    self.logger.info(
                        f"Releasing reader slot for {allocated_reader_state.client_ip}. Active readers: {self._active_readers - 1}")
                    allocated_reader_state.reset()
                    self._active_readers -= 1
