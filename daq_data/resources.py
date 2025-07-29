"""
Common functions for the DaqData clients and servers
"""
from pathlib import Path
import logging
from typing import List, Callable, Tuple, Any, Dict, AsyncIterator
import numpy as np
from pandas import to_datetime, Timestamp
import datetime
import decimal

# rich formatting
from rich import print
from rich.logging import RichHandler
from rich.pretty import pprint, Pretty
from rich.console import Console

## gRPC imports
import grpc

# gRPC reflection service: allows clients to discover available RPCs
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
    ProtoReflectionDescriptorDatabase,
)
# Standard gRPC protobuf types
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import timestamp_pb2

# protoc-generated marshalling / demarshalling code
# from .daq_data_pb2
# import daq_data_pb2_grpc
from daq_data import daq_data_pb2
from daq_data.daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest
from panoseti_util import pff

CFG_DIR = Path('daq_data/config')

def get_dp_cfg(dps):
    """Returns a dictionary of static properties for the given data products."""
    dp_cfg = {}
    for dp in dps:
        if dp == 'img16' or dp == 'ph1024':
            image_shape = [32, 32]
            bytes_per_pixel = 2
        elif dp == 'img8':
            image_shape = [32, 32]
            bytes_per_pixel = 1
        elif dp == 'ph256':
            image_shape = [16, 16]
            bytes_per_pixel = 2
        else:
            raise Exception("bad data product %s" % dp)
        bytes_per_image = bytes_per_pixel * image_shape[0] * image_shape[1]
        is_ph = 'ph' in dp
        # Get type enum for PanoImage message
        if is_ph:
            pano_image_type = PanoImage.Type.PULSE_HEIGHT
        else:
            pano_image_type = PanoImage.Type.MOVIE

        dp_cfg[dp] = {
            "image_shape": image_shape,
            "bytes_per_pixel": bytes_per_pixel,
            "bytes_per_image": bytes_per_image,
            "is_ph": is_ph,
            "pano_image_type": pano_image_type,
        }
    return dp_cfg


def make_rich_logger(name, level=logging.WARNING):
    LOG_FORMAT = (
        "[tid=%(thread)d] [%(funcName)s()] %(message)s "
    )

    rich_handler = RichHandler(
        level=logging.DEBUG,  # Set handler specific level
        show_time=True,
        show_level=True,
        show_path=True,
        enable_link_path=True,
        rich_tracebacks=True,  # Enable rich tracebacks for exceptions
        tracebacks_theme="monokai",  # Optional: Choose a traceback theme
    )

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt="%H:%M:%S",
        # datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[rich_handler]
    )
    return logging.getLogger(name)


# def reflect_services(channel: grpc.Channel) -> str:
#     """Prints all available RPCs for a DaqData service represented by [channel]."""
#     def format_rpc_service(method):
#         name = method.name
#         input_type = method.input_type.name
#         output_type = method.output_type.name
#         stream_fmt = '[magenta]stream[/magenta] '
#         client_stream = stream_fmt if method.client_streaming else ""
#         server_stream = stream_fmt if method.server_streaming else ""
#         return f"rpc {name}({client_stream}{input_type}) returns ({server_stream}{output_type})"
#     reflection_db = ProtoReflectionDescriptorDatabase(channel)
#     services = reflection_db.get_services()
#     msg = f"found services: {services}"
#
#     desc_pool = DescriptorPool(reflection_db)
#     service_desc = desc_pool.FindServiceByName("daqdata.DaqData")
#     msg += f"found [yellow]DaqData[/yellow] service with name: [yellow]{service_desc.full_name}[/yellow]"
#     for method in service_desc.methods:
#         msg += f"\n\tfound: {format_rpc_service(method)}"
#     return msg

def parse_pano_timestamps(pano_image: PanoImage) -> Dict[str, Any]:
    """Parse PanoImage header to get nanosecond-precision timestamps."""
    h = MessageToDict(pano_image.header)
    td = {}
    # Add nanosecond-precision Pandas Timestamp from panoseti packet timing
    if pano_image.shape == [16, 16]:
        td['wr_unix_timestamp'] = pff.wr_to_unix_decimal(h['pkt_tai'], h['pkt_nsec'], h['tv_sec'])
    elif pano_image.shape == [32, 32]:
        h_q0 = h['quabo_0']
        td['wr_unix_timestamp'] = pff.wr_to_unix_decimal(h_q0['pkt_tai'], h_q0['pkt_nsec'], h_q0['tv_sec'])
    nanoseconds_since_epoch = int(td['wr_unix_timestamp'] * decimal.Decimal('1e9'))
    td['pandas_unix_timestamp'] = to_datetime(nanoseconds_since_epoch, unit='ns')
    return td

def parse_pano_image(pano_image: daq_data_pb2.PanoImage) -> Dict[str, Any]:
    """Unpacks a PanoImage message into its components"""
    parsed_pano_image = MessageToDict(pano_image, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)
    pano_timestamps = parse_pano_timestamps(pano_image)
    parsed_pano_image['header'].update(pano_timestamps)
    pano_type = parsed_pano_image['type']
    image_array = np.array(pano_image.image_array).reshape(pano_image.shape)
    bytes_per_pixel = pano_image.bytes_per_pixel
    if bytes_per_pixel == 1:
        image_array = image_array.astype(np.uint8)
    elif bytes_per_pixel == 2:
        if pano_type == 'MOVIE':
            image_array = image_array.astype(np.uint16)
        elif pano_type == 'PULSE_HEIGHT':
            image_array = image_array.astype(np.int16)
    else:
        raise ValueError(f"unsupported bytes_per_pixel: {bytes_per_pixel}")

    parsed_pano_image['image_array'] = image_array
    return parsed_pano_image

def format_stream_images_response(stream_images_response: StreamImagesResponse) -> str:
    parsed_pano_image = parse_pano_image(stream_images_response.pano_image)
    module_id = parsed_pano_image['module_id']
    pano_type = parsed_pano_image['type']
    header = parsed_pano_image['header']
    img = parsed_pano_image['image_array']
    frame_number = parsed_pano_image['frame_number']
    file = parsed_pano_image['file']
    name = stream_images_response.name
    message = stream_images_response.message
    server_timestamp = stream_images_response.timestamp.ToDatetime().isoformat()
    return f"{name=} {server_timestamp=} {file} (f#{frame_number}) {pano_type=} "
