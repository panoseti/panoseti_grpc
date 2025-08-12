import asyncio
import logging
import re
from contextlib import asynccontextmanager
import inspect

import grpc
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict
from pyubx2 import UBXReader, UBX_PROTOCOL
from serial import Serial

from ublox_control import (
    ublox_control_pb2,
    ublox_control_pb2_grpc,
)
from ublox_control.init_f9t_tests import is_os_posix, check_client_f9t_cfg_keys, poll_nav_messages
from ublox_control.resources import F9T_BAUDRATE, run_all_tests


class F9tIoManager:
    """
    Manages low-level I/O with the ZED-F9T device using asyncio.
    It uses asyncio.to_thread to run the blocking pyserial code without
    halting the main event loop.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._io_task = None
        self._stop_event = asyncio.Event()
        self._valid_event = asyncio.Event()
        self.loop = asyncio.get_running_loop()

    async def start(self, f9t_cfg, read_queues, read_queue_freemap, send_queue):
        """Stops any existing I/O task and starts a new one."""
        await self.stop()
        self._stop_event.clear()

        self._io_task = asyncio.create_task(
            self._io_loop(f9t_cfg, read_queues, read_queue_freemap, send_queue)
        )
        try:
            await asyncio.wait_for(self._valid_event.wait(), timeout=2)
            return True
        except asyncio.TimeoutError:
            self.logger.error("Timeout waiting for F9T I/O task to become valid.")
            await self.stop()
            return False

    async def stop(self):
        """Signals the I/O task to stop and waits for it to terminate."""
        if not self._io_task or self._io_task.done():
            return True

        self._stop_event.set()
        try:
            await asyncio.wait_for(self._io_task, timeout=2)
        except asyncio.TimeoutError:
            self.logger.warning("Timeout waiting for I/O task to stop. Cancelling.")
            self._io_task.cancel()
        except asyncio.CancelledError:
            pass  # Task was cancelled, which is fine.

        return self._io_task.done()

    def is_valid(self):
        """Checks if the I/O task is running and has a valid connection."""
        return self._io_task and not self._io_task.done() and self._valid_event.is_set()

    def _blocking_io(self, f9t_cfg, read_queues, read_queue_freemap, send_queue):
        """This function contains the blocking serial I/O and runs in a separate thread."""
        try:
            with Serial(f9t_cfg['device'], F9T_BAUDRATE, timeout=f9t_cfg['timeout']) as stream:
                ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
                self.logger.info("F9T blocking I/O thread successfully connected to the chip.")
                self.loop.call_soon_threadsafe(self._valid_event.set)

                while not self._stop_event.is_set():
                    # Blocking read from the device
                    raw_data, parsed_data = ubr.read()
                    if parsed_data:
                        # Distribute to active reader queues in a thread-safe way
                        for i, is_allocated in enumerate(read_queue_freemap):
                            if is_allocated:
                                try:
                                    # Queues are not thread-safe, so we must use the loop
                                    self.loop.call_soon_threadsafe(read_queues[i].put_nowait, parsed_data)
                                except asyncio.QueueFull:
                                    self.logger.warning(f"Reader queue {i} is full. Dropping packet.")

                    # Non-blocking check for outbound messages
                    while not send_queue.empty():
                        msg = send_queue.get_nowait()
                        stream.write(msg.serialize())
                        send_queue.task_done()
        except Exception as e:
            self.logger.critical(f"F9T blocking I/O thread encountered a fatal error: {e}")
        finally:
            self.loop.call_soon_threadsafe(self._valid_event.clear)
            self.logger.info("F9T blocking I/O thread has stopped.")

    async def _io_loop(self, f9t_cfg, read_queues, read_queue_freemap, send_queue):
        """The async wrapper for the blocking I/O function."""
        self.logger.info(f"Starting F9T I/O task for device {f9t_cfg['device']}")
        self._valid_event.clear()

        await asyncio.to_thread(
            self._blocking_io, f9t_cfg, read_queues, read_queue_freemap, send_queue
        )

        self.logger.info("F9T I/O task has stopped.")


class ClientManager:
    """Manages client connections, resource allocation, and access control using asyncio locks."""

    def __init__(self, logger: logging.Logger, max_readers: int):
        self.logger = logger
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._max_readers = max_readers

        # State variables for reader-writer lock
        self._writer_active = False
        self._writers_waiting = 0
        self._readers_active = 0

        # Resource pool for client data queues
        self._read_queues = [asyncio.Queue(maxsize=200) for _ in range(max_readers)]
        self._read_queue_freemap = [False] * max_readers

    @property
    def read_queues(self):
        return self._read_queues

    @property
    def read_queue_freemap(self):
        return self._read_queue_freemap

    @asynccontextmanager
    async def get_writer_access(self, context):
        """An async context manager to acquire exclusive write access."""
        async with self._lock:
            self._writers_waiting += 1
            try:
                await asyncio.wait_for(
                    self._condition.wait_for(lambda: not (self._writer_active or self._readers_active > 0)),
                    timeout=5
                )
            except asyncio.TimeoutError:
                self._writers_waiting -= 1
                context.abort(grpc.StatusCode.ABORTED, "Timeout waiting for writer access. Readers are active.")
            self._writers_waiting -= 1
            self._writer_active = True

        try:
            yield
        finally:
            async with self._lock:
                self._writer_active = False
                self._condition.notify_all()

    @asynccontextmanager
    async def get_reader_access(self, context):
        """An async context manager to acquire shared read access and a dedicated data queue."""
        reader_id = -1
        async with self._lock:
            try:
                await asyncio.wait_for(
                    self._condition.wait_for(lambda: not (
                                self._writer_active or self._writers_waiting > 0 or self._readers_active >= self._max_readers)),
                    timeout=5
                )
            except asyncio.TimeoutError:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Timeout waiting for reader access.")

            try:
                reader_id = self._read_queue_freemap.index(False)
                self._read_queue_freemap[reader_id] = True
            except ValueError:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "No available reader slots.")

            self._readers_active += 1

        try:
            yield self._read_queues[reader_id]
        finally:
            async with self._lock:
                self._readers_active -= 1
                if reader_id != -1:
                    self._read_queue_freemap[reader_id] = False
                    # Clear the queue to prevent old data from being sent to the next client
                    while not self._read_queues[reader_id].empty():
                        self._read_queues[reader_id].get_nowait()
                if self._writers_waiting > 0 and self._readers_active == 0:
                    self._condition.notify_all()


class F9tManager:
    """Orchestrates the server's primary operations, acting as a task supervisor."""

    def __init__(self, io_manager: F9tIoManager, client_manager: ClientManager, server_cfg, logger: logging.Logger):
        self.io_manager = io_manager
        self.client_manager = client_manager
        self.server_cfg = server_cfg
        self.logger = logger

        self._f9t_cfg = {}
        self._is_initialized = False
        self._send_queue = asyncio.Queue()

    def is_running(self):
        return self.io_manager.is_valid()

    async def initialize_f9t(self, request, context):
        """Handles the logic for the InitF9t RPC."""
        client_f9t_cfg = MessageToDict(request.f9t_cfg, preserving_proto_field_name=True)
        self.logger.info(
            f"Received InitF9t transaction for device {client_f9t_cfg['device']}, "
            f"timeout {client_f9t_cfg['timeout']}s, "
            f"cfg_key_settings {client_f9t_cfg['cfg_key_settings']}"
        )

        all_pass, test_results = await run_all_tests(
            test_fn_list=[is_os_posix, check_client_f9t_cfg_keys],
            args_list=[[], [['device', 'chip_name', 'chip_uid', 'timeout', 'cfg_key_settings'], client_f9t_cfg.keys()]]
        )
        if not all_pass:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Client F9T configuration failed validation.")
        self.logger.info("Client F9T config validation passed.")

        if not await self.io_manager.start(client_f9t_cfg, self.client_manager.read_queues,
                                           self.client_manager.read_queue_freemap, self._send_queue):
            context.abort(grpc.StatusCode.INTERNAL, "Failed to start and validate F9T I/O task.")

        init_pass, init_tests = await run_all_tests(
            test_fn_list=[poll_nav_messages], args_list=[[self._send_queue]]
        )
        if not init_pass:
            await self.io_manager.stop()
            context.abort(grpc.StatusCode.ABORTED, "Post-initialization tests failed.")

        self._f9t_cfg = client_f9t_cfg
        self._is_initialized = True
        self.logger.info("InitF9t transaction completed successfully.")

        return ublox_control_pb2.InitF9tResponse(
            init_status=ublox_control_pb2.InitF9tResponse.InitStatus.SUCCESS,
            message="InitF9t transaction successful",
            f9t_cfg=ParseDict(self._f9t_cfg, Struct()),
            test_results=test_results + init_tests,
        )

    async def stream_data(self, request, context, reader_queue: asyncio.Queue):
        """Handles the logic for the CaptureUblox RPC stream."""
        patterns = [re.compile(p) for p in request.patterns]

        while context.is_active() and self.is_running():
            try:
                parsed_data = await asyncio.wait_for(reader_queue.get(), timeout=5.0)

                packet_name = parsed_data.identity
                if not patterns or any(p.search(packet_name) for p in patterns):
                    timestamp = ublox_control_pb2.Timestamp()
                    timestamp.GetCurrentTime()
                    yield ublox_control_pb2.CaptureUbloxResponse(
                        type=ublox_control_pb2.CaptureUbloxResponse.Type.DATA,
                        name=packet_name,
                        timestamp=timestamp
                    )
            except asyncio.TimeoutError:
                if not self.is_running():
                    self.logger.warning("I/O task stopped while client was streaming.")
                    break

        if not context.is_active():
            self.logger.info("Client disconnected from stream.")

