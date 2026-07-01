"""Classes for managing DaqData server state."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import grpc

from .config import DaqDataServerConfig
from .hp_io_manager import HpIoManager
from .simulate import SimulationManager
from .state import ReaderState


class HpIoTaskManager:
    """Manages the lifecycle of the HpIoManager background task."""

    def __init__(
        self, logger: logging.Logger, server_cfg: DaqDataServerConfig, reader_states: list[ReaderState]
    ) -> None:
        self.logger = logger
        self.server_cfg = server_cfg
        self.reader_states = reader_states
        self.hp_io_task: asyncio.Task[None] | None = None
        self.hp_io_manager: HpIoManager | None = None
        self.hp_io_valid_event = asyncio.Event()
        self.active_data_products: set[str] = set()
        self.hp_io_cfg: dict[str, Any] = {}
        self.stop_event = asyncio.Event()
        self.simulation_manager = SimulationManager(server_cfg, logger)

    def is_valid(self, verbose: bool = True) -> bool:
        """Checks if the hp_io task is running and considered valid."""
        if self.hp_io_task and not self.hp_io_task.done() and self.hp_io_valid_event.is_set():
            return True
        if verbose:
            if not self.hp_io_task:
                self.logger.warning("hp_io task is uninitialized")
            elif self.hp_io_task.done():
                self.logger.warning("hp_io task is not alive")
            elif not self.hp_io_valid_event.is_set():
                self.logger.warning("hp_io task is alive but not valid")
        return False

    async def start(self, hp_io_cfg: dict[str, Any]) -> bool:
        """Creates a new hp_io task. Stops any existing task first."""
        await self.stop()

        is_sim = bool(hp_io_cfg.get("simulate_daq", False))
        sim_setup_task = None
        if is_sim:
            sim_setup_task = asyncio.create_task(self.simulation_manager.setup_environment())

        self.hp_io_cfg = hp_io_cfg
        active_data_products_queue: asyncio.Queue[set[str]] = asyncio.Queue()

        self.hp_io_manager = HpIoManager(
            self.server_cfg,
            hp_io_cfg,
            self.reader_states,
            self.stop_event,
            self.hp_io_valid_event,
            active_data_products_queue,
            self.logger,
        )
        self.hp_io_task = asyncio.create_task(self.hp_io_manager.run())
        try:
            await asyncio.wait_for(self.hp_io_valid_event.wait(), timeout=10.0)
            self.active_data_products = await active_data_products_queue.get()
            self.logger.info(f"hp_io task initialized with active_data_products={self.active_data_products}")

            if sim_setup_task:
                # UDS simulation must be started AFTER hp_io (sockets must exist first)
                if not await sim_setup_task:
                    self.logger.critical("Failed to set up UDS simulation environment. Aborting start.")
                    await self.simulation_manager.cleanup_environment()
                    await self.stop()
                    return False
                if not await self.simulation_manager.start_simulation_loop():
                    self.logger.error("Failed to start UDS simulation loop after IO manager was ready.")
                    await self.stop()
                    return False
        except TimeoutError:
            self.logger.error("Timeout waiting for hp_io task to become valid.")
            await self.stop()
            return False
        return self.is_valid(verbose=True)

    async def stop(self) -> None:
        """Stops the hp_io task and any associated simulation task gracefully."""
        await self.simulation_manager.stop_simulation_loop()

        if self.hp_io_task and not self.hp_io_task.done():
            self.logger.info("Stopping hp_io task...")
            self.stop_event.set()
            try:
                await asyncio.wait_for(self.hp_io_task, timeout=2.0)
                self.logger.info("Successfully terminated hp_io task.")
            except TimeoutError:
                self.logger.warning("Timeout stopping hp_io task. Cancelling.")
                self.hp_io_task.cancel()
            except Exception as e:
                self.logger.error(f"Exception while stopping hp_io task: {e}", exc_info=True)

        await self.simulation_manager.cleanup_environment()

        self.hp_io_task = None
        self.hp_io_manager = None
        self.active_data_products = set()
        self.hp_io_valid_event.clear()
        self.stop_event.clear()


class ClientManager:
    """Manages client connections, state, and access control for server resources."""

    def __init__(self, logger: logging.Logger, server_cfg: DaqDataServerConfig) -> None:
        self.logger = logger
        self.max_clients = server_cfg.max_concurrent_rpcs
        self._cancel_readers_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        self._readers: list[ReaderState] = [
            ReaderState(
                cancel_reader_event=self._cancel_readers_event,
                shutdown_event=self._shutdown_event,
            )
            for _ in range(self.max_clients)
        ]
        self._active_readers = 0
        self._writer_lock = asyncio.Lock()
        self._readers_lock = asyncio.Lock()

    @property
    def reader_states(self) -> list[ReaderState]:
        return self._readers

    @property
    def cancel_readers_event(self) -> asyncio.Event:
        return self._cancel_readers_event

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    async def cancel_all_readers(self) -> None:
        """Signals all active reader streams to terminate."""
        self.logger.warning("Cancelling all active and waiting reader RPCs.")
        self._cancel_readers_event.set()

    def signal_shutdown(self) -> None:
        self._shutdown_event.set()

    @asynccontextmanager
    async def get_writer_access(
        self, context: grpc.aio.ServicerContext, force: bool = False
    ) -> AsyncIterator[uuid.UUID]:
        """A context manager to safely acquire exclusive 'writer' access."""
        uid = uuid.uuid4()
        async with self._writer_lock:
            self.logger.debug(f"Writer ({uid}) acquired writer lock.")
            try:
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
                self.logger.debug(f"Writer ({uid}) released writer lock.")

    @asynccontextmanager
    async def get_reader_access(
        self, context: grpc.aio.ServicerContext, hp_io_task_manager: HpIoTaskManager
    ) -> AsyncIterator[ReaderState]:
        """A context manager to safely acquire a 'reader' slot."""
        uid = uuid.uuid4()

        if self._writer_lock.locked() or self._cancel_readers_event.is_set():
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Server is being configured, please try again soon.")

        async with self._readers_lock:
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
