"""
DAQ Data v2 Forwarder.
Reads real-time science data from Hashpipe UDS sockets and pushes it
to the centralized DaqDataV2 aggregator on the Headnode.
"""

import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from io import BytesIO
from json import loads

import grpc
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.generated import daq_data_v2_pb2, daq_data_v2_pb2_grpc
from panoseti_grpc.panoseti_util import pff
from panoseti_grpc.telemetry.logger import get_logger

# Define Data Products for v2
DATA_PRODUCTS = {
    "img8": {"shape": (32, 32), "bpp": 1, "is_ph": False},
    "img16": {"shape": (32, 32), "bpp": 2, "is_ph": False},
    "ph256": {"shape": (16, 16), "bpp": 2, "is_ph": True},
    "ph1024": {"shape": (32, 32), "bpp": 2, "is_ph": True},
}


class Forwarder:
    def __init__(
        self,
        headnode_target: str,
        socket_path_template: str,
        data_products: list[str],
        logger: logging.Logger,
    ):
        self.headnode_target = headnode_target
        self.socket_path_template = socket_path_template
        self.data_products = data_products
        self.logger = logger
        self.queue: asyncio.Queue[daq_data_v2_pb2.PanoImage] = asyncio.Queue(maxsize=100)
        self.stop_event = asyncio.Event()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, dp_name: str):
        """Handles a single client connection on a UDS socket."""
        client_info = writer.get_extra_info("peername")
        self.logger.info(f"New UDS client connection for {dp_name} from {client_info}")

        dp_cfg = DATA_PRODUCTS[dp_name]
        bytes_per_image = dp_cfg["shape"][0] * dp_cfg["shape"][1] * dp_cfg["bpp"]
        pano_type = (
            daq_data_v2_pb2.PanoImage.Type.PULSE_HEIGHT if dp_cfg["is_ph"] else daq_data_v2_pb2.PanoImage.Type.MOVIE
        )

        header_size = None
        frame_count = 0

        try:
            while not self.stop_event.is_set():
                # Read module_id (2 bytes)
                module_id_bytes = await reader.readexactly(2)
                module_id = int.from_bytes(module_id_bytes, "big")

                # Read JSON header
                if header_size is None:
                    header_with_sep = await reader.readuntil(b"\n\n")
                    header_size = len(header_with_sep)
                    self.logger.info(f"Discovered header size {header_size} for {dp_name}")
                else:
                    header_with_sep = await reader.readexactly(header_size)

                header = loads(header_with_sep[:-2].decode())

                # Read image data (1 byte '*' separator + pixels)
                img_data = await reader.readexactly(1 + bytes_per_image)
                img_array = pff.read_image(
                    BytesIO(img_data),
                    dp_cfg["shape"][0],
                    dp_cfg["bpp"],
                )

                pano_image = daq_data_v2_pb2.PanoImage(
                    type=pano_type,
                    header=ParseDict(header, Struct()),
                    image_array=list(img_array),
                    shape=list(dp_cfg["shape"]),
                    bytes_per_pixel=dp_cfg["bpp"],
                    file=f"uds_{dp_name}",
                    frame_number=frame_count,
                    module_id=module_id,
                )

                try:
                    self.queue.put_nowait(pano_image)
                    if frame_count % 100 == 0:
                        self.logger.info(f"Forwarder pushed frame {frame_count} for {dp_name} to queue")
                except asyncio.QueueFull:
                    # Circular buffer: evict oldest frame to make room for the newest
                    try:
                        self.queue.get_nowait()
                        self.queue.put_nowait(pano_image)
                        if frame_count % 100 == 0:
                            self.logger.warning(
                                f"Queue full for {dp_name}. Evicted oldest frame to keep low-latency stream."
                            )
                    except asyncio.QueueEmpty, asyncio.QueueFull:
                        pass  # Handle rare race conditions where queue state changes between calls

                frame_count += 1
        except asyncio.IncompleteReadError:
            self.logger.warning(f"UDS {dp_name} client {client_info} disconnected")
        except Exception as e:
            self.logger.error(f"Error handling UDS {dp_name} client: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _read_uds(self, dp_name: str):
        """Starts a UDS server for a single data product."""
        socket_path = self.socket_path_template.format(dp_name=dp_name)

        # Clean up stale socket
        import anyio

        if await anyio.Path(socket_path).exists():
            await anyio.Path(socket_path).unlink()

        self.logger.info(f"Starting UDS server for {dp_name} on {socket_path}")
        server = await asyncio.start_unix_server(lambda r, w: self._handle_client(r, w, dp_name), path=socket_path)

        async with server:
            await self.stop_event.wait()

    async def _push_to_headnode(self):
        """Streams images from the queue to the Headnode."""

        async def request_generator() -> AsyncIterator[daq_data_v2_pb2.UploadImageRequest]:
            self.logger.info("Request generator started")
            while not self.stop_event.is_set():
                try:
                    pano_image = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    self.logger.info(
                        f"Yielding frame {pano_image.frame_number} module {pano_image.module_id} to aggregator"
                    )
                    try:
                        req = daq_data_v2_pb2.UploadImageRequest(pano_image=pano_image)
                        yield req
                    except Exception as e:
                        self.logger.error(f"Error yielding request: {e}")
                except TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"Error in request generator: {e}", exc_info=True)
            self.logger.info("Request generator stopped")

        while not self.stop_event.is_set():
            try:
                self.logger.info(f"Connecting to Headnode: {self.headnode_target}")
                async with grpc.aio.insecure_channel(self.headnode_target) as channel:
                    stub = daq_data_v2_pb2_grpc.DaqDataV2Stub(channel)
                    self.logger.info("Calling UploadImages RPC...")
                    await stub.UploadImages(request_generator())
                    self.logger.info("UploadImages RPC completed")
            except grpc.aio.AioRpcError as e:
                self.logger.error(f"gRPC error pushing to headnode: {e}")
                await asyncio.sleep(2.0)
            except Exception as e:
                self.logger.error(f"Unexpected error pushing to headnode: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    async def run(self):
        """Starts all UDS readers and the push task."""
        tasks = [asyncio.create_task(self._read_uds(dp)) for dp in self.data_products]
        tasks.append(asyncio.create_task(self._push_to_headnode()))

        await self.stop_event.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--headnode", default="localhost:50051")
    parser.add_argument("--socket-template", default="/tmp/hashpipe_grpc.dp_{dp_name}.sock")
    parser.add_argument("--data-products", nargs="+", default=["img16", "ph256"])
    args = parser.parse_args()

    logger = get_logger("daq_data_v2.forwarder")
    forwarder = Forwarder(args.headnode, args.socket_template, args.data_products, logger)

    def stop():
        forwarder.stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop)

    await forwarder.run()


if __name__ == "__main__":
    asyncio.run(main())
