"""
from __future__ import annotations
Common functions for the DaqData clients and servers
"""
from __future__ import annotations
import asyncio
import os
from pathlib import Path
import logging
from typing import Any
import numpy as np
from pandas import to_datetime
from datetime import datetime
import decimal
import re

import importlib.resources as resources
import json

# rich formatting
from rich import print

## gRPC imports
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict

# protoc-generated marshalling / demarshalling code
from panoseti_grpc.generated import daq_data_pb2
from panoseti_grpc.generated.daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest
from panoseti_grpc.panoseti_util import pff, control_utils

# Shared PANOSETI logger (console + rotating file + optional gRPC telemetry)
from panoseti_grpc.telemetry.logger import get_logger

CFG_DIR = Path('daq_data/config')

daq_data_anchor_package = "panoseti_grpc"

def load_package_json(package, fname):
    """Define the resource path relative to the package root
    Args:
        - package: refers to a path in the package (e.g., 'package.daq_data.config' )
        - fname:  refers to the file within the package (e.g., 'hp_io_config_simulate.json')

    """
    resource_path = resources.files(package).joinpath(fname)

    # 2. Use 'as_file' to obtain a path-like object that can be opened
    #    This is necessary because the resource might be inside a zip file
    with resource_path.open('r') as f:
        data = json.load(f)

    return data

def _parse_dp_name(filename: str, dp_name_re = re.compile(r'\.dp_([a-zA-Z0-9]+)\.')) -> str:
    """Extracts the data product name (e.g., 'img16') from a PFF filename."""
    match = dp_name_re.search(filename)
    if not match:
        raise ValueError(f"Could not parse data product name from filename: {filename}")
    return match.group(1)

def _parse_seqno(filename: str, seqno_re=re.compile(r'\.seqno_(\d+)\.')) -> int:
    """Extracts the seqno from a PFF filename."""
    match = seqno_re.search(filename)
    seqno = int(match.group(1)) if match else 0
    return seqno

def get_dp_name_from_props(pano_type, shape: list, bytes_per_pixel: int) -> str:
    """Derives the data product name from PanoImage properties by iterating DataProduct members."""
    from .state import DataProduct
    # Normalise pano_type to a PanoImage.Type int value
    if isinstance(pano_type, str):
        pano_type = PanoImage.Type.Value(pano_type)
    is_ph = (pano_type == PanoImage.Type.PULSE_HEIGHT)
    shape_tuple = tuple(shape)
    for dp in DataProduct:
        if dp.image_shape == shape_tuple and dp.bytes_per_pixel == bytes_per_pixel and dp.is_ph == is_ph:
            return dp.value
    raise ValueError(
        f"Unknown data product for properties: type={PanoImage.Type.Name(pano_type)}, "
        f"shape={shape_tuple}, bpp={bytes_per_pixel}"
    )

def pkt_to_unix_decimal(tv_sec, tv_usec):
    tv_sec = decimal.Decimal(str(tv_sec))
    tv_usec = decimal.Decimal(str(tv_usec))
    usec_factor = decimal.Decimal(str(1e6))
    return tv_sec + (tv_usec / usec_factor)

def parse_pano_timestamps(pano_image: PanoImage, do_wr=False) -> dict[str, Any]:
    """Parse PanoImage header to get nanosecond-precision timestamps."""
    h = MessageToDict(pano_image.header)
    td = {}
    # Add nanosecond-precision Pandas Timestamp from panoseti packet timing
    if pano_image.shape == [16, 16]:
        td['wr_unix_timestamp'] = pff.wr_to_unix_decimal(h['pkt_tai'], h['pkt_nsec'], h['tv_sec'])
        td['pkt_unix_timestamp'] = pkt_to_unix_decimal(h['tv_sec'], h['tv_usec'])
    elif pano_image.shape == [32, 32]:
        h_q0 = h['quabo_0']
        td['wr_unix_timestamp'] = pff.wr_to_unix_decimal(h_q0['pkt_tai'], h_q0['pkt_nsec'], h_q0['tv_sec'])
        td['pkt_unix_timestamp'] = pkt_to_unix_decimal(h_q0['tv_sec'], h_q0['tv_usec'])
    if do_wr:
        nanoseconds_since_epoch = int(td['wr_unix_timestamp'] * decimal.Decimal('1e9'))
    else:
        nanoseconds_since_epoch = int(td['pkt_unix_timestamp'] * decimal.Decimal('1e9'))
    td['pandas_unix_timestamp'] = to_datetime(nanoseconds_since_epoch, unit='ns')
    return td

def parse_pano_image(pano_image: daq_data_pb2.PanoImage) -> dict[str, Any]:
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

def is_daq_active_sync(simulate_daq, sim_cfg=None):
    """Returns True iff the data stream from hashpipe or simulated hashpipe is active."""
    if simulate_daq:
        return True  # UDS simulation is always considered active
    return control_utils.is_hashpipe_running()

async def is_daq_active(simulate_daq, sim_cfg=None, retries=1, delay: float = 0.5):
    """Returns True iff the data stream from hashpipe or simulated hashpipe is active."""
    for i in range(retries):
        if is_daq_active_sync(simulate_daq, sim_cfg):
            return True
        await asyncio.sleep(delay)
    return False
