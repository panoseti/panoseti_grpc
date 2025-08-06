import asyncio
import os
import logging
import json
from io import BytesIO

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

from .daq_data_pb2 import PanoImage
from .resources import get_dp_config
from panoseti_util import pff


class UdsReceiver:
    """Manages reading PFF data from a single Unix Domain Socket."""

    def __init__(self, dp_name: str, module_id: int, upload_queue: asyncio.Queue, shutdown_event: asyncio.Event, logger: logging.Logger):
        self.dp_name = dp_name
        self.module_id = module_id
        self.upload_queue = upload_queue
        self._shutdown_event = shutdown_event
        self.logger = logger

        # Derive socket path from data product name
        self.socket_path = f"/tmp/hashpipe_grpc_{dp_name}.sock"
        # Get data product configuration
        self.dp_config = get_dp_config([dp_name])[dp_name]
        self.server: asyncio.AbstractServer = None

    async def run(self):
        """Starts the UDS server and listens for connections."""
        self.logger.info(f"Starting UDS receiver for {self.dp_name} on {self.socket_path}")

        # Clean up old socket file if it exists
        if os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except OSError as e:
                self.logger.error(f"Error removing existing socket file {self.socket_path}: {e}")
                return

        try:
            self.server = await asyncio.start_unix_server(self._handle_client, path=self.socket_path)
            self.logger.info(f"UDS server for {self.dp_name} is listening on {self.socket_path}")
            await self._shutdown_event.wait()
            self.logger.info(f"Stopping UDS receiver for {self.dp_name}.")
        except Exception as e:
            self.logger.error(f"UDS receiver for {self.dp_name} failed: {e}", exc_info=True)
        finally:
            if self.server:
                self.server.close()
                await self.server.wait_closed()
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
            self.logger.info(f"UDS receiver for {self.dp_name} has stopped.")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Callback to handle a new client connection."""
        peer = writer.get_extra_info('peername')
        self.logger.info(f"New connection on {self.dp_name} socket from {peer or 'unknown'}")
        frame_count = 0

        try:
            # The pff functions are blocking, so run them in a thread
            def _get_header_size(data_chunk) -> tuple[dict or None, int] :
                try:
                    f = BytesIO(data_chunk)
                    header_str = pff.read_json(f)
                    header_size = f.tell()
                    header = json.loads(header_str)
                    return header, header_size
                except Exception:
                    return None, -1

            def _blocking_pff_read(data_chunk) -> tuple[str, bytes]:
                f = BytesIO(data_chunk)
                header_str = pff.read_json(f)
                img = pff.read_image(f, self.dp_config.image_shape[0], self.dp_config.bytes_per_pixel)
                if header_str and img is not None:
                    return json.loads(header_str), img
                return None, None

            # First, read a chunk to parse the header
            # Ensure we initially read less than a complete PFF frame
            frame_size = self.dp_config.bytes_per_image
            initial_chunk = await reader.read(frame_size)
            if len(initial_chunk) < frame_size:
                self.logger.error(f"Initial chunk on {self.dp_name} stream is too short."
                                  f"Expected {frame_size} bytes, got {len(initial_chunk)}. Closing connection.")
                return

            header, header_size = await asyncio.to_thread(_get_header_size, initial_chunk)
            if header is None:
                self.logger.error(f"Could not parse JSON header on initial PFF frame of type {self.dp_name}.")
                return

            # Compute the size of each image frame based on the first frame
            frame_size = header_size + 1 + self.dp_config.bytes_per_image  # the `+ 1` is for the special '*' prefix char
            self.dp_config.frame_size = frame_size

            # Read the remaining bytes for the image
            img_buffer = initial_chunk[header_size + 1:]
            bytes_to_read = self.dp_config.bytes_per_image - len(img_buffer)
            try:
                if bytes_to_read > 0:
                    img_buffer += await reader.readexactly(bytes_to_read)
            except asyncio.IncompleteReadError:
                self.logger.error(f"Incomplete image array on initial PFF frame for {self.dp_name}.")
                raise

            self.logger.info(f"Parsed {self.dp_name} frame sizes: "
                             f"{frame_size=} = ({header_size=}) + (bytes_per_image={self.dp_config.bytes_per_image}) + 1")

            # After determining the constant frame size, we can exactly read complete PFF frames of this data type.
            while not self._shutdown_event.is_set():
                pff_frame_bytes = await reader.readexactly(frame_size)
                header, img = await asyncio.to_thread(_blocking_pff_read, pff_frame_bytes)
                if not header:
                    self.logger.warning(f"Could not parse JSON header on {self.dp_name} stream.")
                    continue
                elif not img:
                    self.logger.warning(f"Could not parse image array on {self.dp_name} stream.")
                    continue

                pano_image = PanoImage(
                    type=self.dp_config.pano_image_type,
                    header=ParseDict(header, Struct()),
                    image_array=img,
                    shape=self.dp_config.image_shape,
                    bytes_per_pixel=self.dp_config.bytes_per_pixel,
                    file=f"start_NONE.uds_{self.dp_name}",
                    frame_number=frame_count,
                    module_id=self.module_id,
                )

                try:
                    self.upload_queue.put_nowait(pano_image)
                    frame_count += 1
                except asyncio.QueueFull:
                    if frame_count % 1000 == 0:
                        self.logger.warning(f"Upload queue is full. Dropping frame from UDS {self.dp_name}.")

        except asyncio.IncompleteReadError:
            self.logger.info(f"Client disconnected from {self.dp_name} socket.")
        except asyncio.CancelledError:
            self.logger.info(f"Client handler for {self.dp_name} was cancelled.")
        except Exception as e:
            self.logger.error(f"Error in UDS client handler for {self.dp_name}: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()
            self.logger.info(f"Connection closed on {self.dp_name} socket.")
