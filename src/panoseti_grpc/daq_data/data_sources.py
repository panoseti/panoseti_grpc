"""
Defines data source classes for the HpIoManager.
Only the Unix Domain Socket (UDS) data source is supported.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import os
import socket
import stat
import struct
from io import BytesIO
from json import loads
from typing import Any, cast

from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.generated.daq_data_pb2 import PanoImage
from panoseti_grpc.panoseti_util import pff

from .state import DataProductState, get_dp_config


class BaseDataSource(abc.ABC):
    """Abstract base class for a data acquisition source."""

    def __init__(
        self,
        config: dict[str, Any],
        logger: logging.Logger,
        data_queue: asyncio.Queue[PanoImage],
        stop_event: asyncio.Event,
    ) -> None:
        self.config = config
        self.logger = logger
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.ready_event = asyncio.Event()

    @abc.abstractmethod
    async def run(self) -> None:
        """The main entry point to start watching for and producing data."""
        pass


class UdsDataSource(BaseDataSource):
    """Acquires data from a Unix Domain Socket. Acts as the UDS SERVER.

    Wire format per frame (written atomically by Hashpipe via writev):
        [2 bytes: big-endian module_id] [JSON header] \\n\\n* [binary image bytes]
    """

    SOCKET_BUFFER_SIZE = 2048 * 100

    def __init__(
        self,
        config: dict[str, Any],
        logger: logging.Logger,
        data_queue: asyncio.Queue[PanoImage],
        stop_event: asyncio.Event,
    ) -> None:
        super().__init__(config, logger, data_queue, stop_event)
        self.dp_name: str = self.config["dp_name"]

        socket_path_template = self.config.get("socket_path_template")
        if not socket_path_template:
            raise ValueError("UdsDataSource requires a 'socket_path_template'")

        self.socket_path = socket_path_template.format(dp_name=self.dp_name)
        self.dp_config: DataProductState = get_dp_config([self.dp_name])[self.dp_name]
        self.server: asyncio.AbstractServer | None = None
        self.read_timeout: float = config.get("read_timeout", 60.0)
        self.client_handler_tasks: set[asyncio.Task[None]] = set()

    async def _client_connection_wrapper(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Wraps the client handler to track its task lifecycle."""
        task = asyncio.create_task(self._handle_client(reader, writer))
        self.client_handler_tasks.add(task)

        def on_task_done(t: asyncio.Task[None]) -> None:
            self.client_handler_tasks.discard(t)
            client_info = writer.get_extra_info("peername")
            self.logger.info(f"Client handler task for {client_info} on {self.socket_path} has finished.")

        task.add_done_callback(on_task_done)

    async def run(self) -> None:
        """Creates, binds, and listens on the UDS socket file."""
        self.logger.info(f"Starting UDS receiver for '{self.dp_name}' on {self.socket_path}")

        try:
            # Remove stale socket files from previous runs.
            if os.path.exists(self.socket_path):
                s = os.stat(self.socket_path)
                if stat.S_ISSOCK(s.st_mode):
                    self.logger.warning(f"Removing stale socket file: {self.socket_path}")
                    os.unlink(self.socket_path)
                else:
                    self.logger.error(f"A non-socket file exists at {self.socket_path}. Manual intervention required.")
                    return
        except OSError as e:
            self.logger.error(f"Error removing stale socket file {self.socket_path}: {e}. Cannot start.")
            return

        server_sock = None
        try:
            server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

            # Larger receive buffer detects disconnects faster.
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.SOCKET_BUFFER_SIZE)
            self.logger.info(f"Set UDS receive buffer to {self.SOCKET_BUFFER_SIZE} for {self.socket_path}")

            # SO_LINGER(1,0): force RST on close so Hashpipe detects the disconnect immediately.
            linger_struct = struct.pack("ii", 1, 0)
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger_struct)

            server_sock.bind(self.socket_path)
            try:
                os.chmod(self.socket_path, 0o600)  # owner-only: only Hashpipe (same user) connects
            except OSError as e:
                self.logger.warning(f"Could not set socket permissions on {self.socket_path}: {e}")

            server_sock.listen(5)  # allow a small queue in case of rapid reconnects
            server_sock.setblocking(False)

            self.server = await asyncio.start_unix_server(self._client_connection_wrapper, sock=server_sock)
            self.ready_event.set()
            await self.stop_event.wait()

        except OSError as e:
            self.logger.error(f"UDS receiver for {self.socket_path} failed to bind or start: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"UDS receiver for {self.socket_path} failed unexpectedly: {e}", exc_info=True)
        finally:
            self.logger.info(f"Shutting down UDS receiver for {self.socket_path}...")

            if self.client_handler_tasks:
                self.logger.warning(
                    f"Cancelling {len(self.client_handler_tasks)} outstanding client "
                    f"handler tasks for {self.socket_path}."
                )
                tasks_to_cancel = list(self.client_handler_tasks)
                for task in tasks_to_cancel:
                    task.cancel()
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

            if self.server:
                self.server.close()
                await self.server.wait_closed()

            if server_sock:
                server_sock.close()

            if os.path.exists(self.socket_path):
                try:
                    os.unlink(self.socket_path)
                except OSError:
                    pass
            self.ready_event.clear()

    async def _read_one_frame(
        self,
        reader: asyncio.StreamReader,
        header_size: int | None,
    ) -> tuple[int, bytes, bytes, int]:
        """Reads one complete PFF frame from the stream. Returns (module_id, header_bytes, img_data, header_size).

        Batches all three reads inside a single wait_for to reduce Task creation overhead.
        """

        async def _reads() -> tuple[int, bytes, bytes, int]:
            nonlocal header_size
            module_id_bytes = await reader.readexactly(2)
            module_id = int.from_bytes(module_id_bytes, "big")

            if header_size is None:
                header_with_sep = await reader.readuntil(b"\n\n")
                header_size = len(header_with_sep)
                self.logger.info(f"Discovered header size of {header_size} bytes for {self.socket_path}")
            else:
                header_with_sep = await reader.readexactly(header_size)

            # '1 +' accounts for the '*' byte at the start of the image block
            img_data = await reader.readexactly(1 + self.dp_config.bytes_per_image)
            return module_id, header_with_sep, img_data, header_size

        return await asyncio.wait_for(_reads(), self.read_timeout)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handles a Hashpipe client connection, parsing [module_id][PFF frame] messages."""
        client_info = writer.get_extra_info("peername")
        self.logger.info(f"New client connection on {self.socket_path} from {client_info}")
        frame_count = 0
        header_size: int | None = None  # Discovered from first frame; fixed thereafter

        try:
            while not self.stop_event.is_set():
                module_id, header_with_sep, img_data, header_size = await self._read_one_frame(reader, header_size)

                # Parse JSON header (strip '\n\n' separator) and decode image
                header = loads(header_with_sep[:-2].decode())
                img_array = pff.read_image(
                    BytesIO(img_data),
                    self.dp_config.image_shape[0],
                    self.dp_config.bytes_per_pixel,
                )
                pano_image = PanoImage(
                    type=cast(Any, self.dp_config.pano_image_type),
                    header=ParseDict(header, Struct()),
                    image_array=img_array,
                    shape=list(self.dp_config.image_shape),
                    bytes_per_pixel=self.dp_config.bytes_per_pixel,
                    file=f"uds_{self.dp_name}",
                    frame_number=frame_count,
                    module_id=module_id,
                )
                await self.data_queue.put(pano_image)
                frame_count += 1

        except TimeoutError:
            # Hashpipe closes its connection after 15 s of idle (UDS_CONNECTION_TIMEOUT_US).
            # This is expected during observation gaps; log and exit so the server re-accepts.
            self.logger.info(
                f"Read timeout on {self.socket_path} (>{self.read_timeout}s idle). "
                "Closing connection; Hashpipe will reconnect on next frame."
            )
        except (asyncio.IncompleteReadError, ConnectionResetError):
            self.logger.info(f"Client {client_info} disconnected from {self.socket_path}.")
        except asyncio.CancelledError:
            self.logger.info(f"Client handler for {self.socket_path} was cancelled.")
        finally:
            writer.close()
            await writer.wait_closed()
