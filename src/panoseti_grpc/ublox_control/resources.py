"""
Common functions for gRPC UbloxControl service.
"""
import os
import sys
import json5
import logging
import datetime
from typing import List, Callable, Tuple, Any, Dict
from contextlib import contextmanager
from pathlib import Path
import redis
import inspect

import importlib.resources as resources

from rich import print
# from rich.markup import escape
from rich.logging import RichHandler
from rich.pretty import pprint

import datetime
from unittest import TestResult

from serial import Serial
from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, POLL_LAYER_RAM, TXN_COMMIT, TXN_NONE

from panoseti_grpc.generated import (
    ublox_control_pb2,
    ublox_control_pb2_grpc,
)

# message enums
from panoseti_grpc.generated.ublox_control_pb2 import InitF9tResponse, CaptureUbloxRequest, CaptureUbloxResponse



""" Config globals"""
F9T_BAUDRATE = 38400

CFG_DIR = Path('ublox_control/config')
ublox_control_config_file = 'ublox_control_server_config.json'
ublox_control_anchor_package = "panoseti_grpc"

# Configuration for metadata capture from the u-blox ZED-F9T timing chip
default_f9t_cfg_file = "f9t_config.json5"

f9t_cfg_path = CFG_DIR / default_f9t_cfg_file

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
        data = json5.load(f)

    return data

try:
    default_f9t_cfg = load_package_json(ublox_control_anchor_package, f9t_cfg_path)
    # with open(f9t_cfg_path) as f:
    #     default_f9t_cfg = json5.load(f)
except FileNotFoundError:
    print(f"could not find {os.path.abspath(f9t_cfg_path)}")
    sys.exit(1)


def make_rich_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Sets up a logger with a RichHandler for console output and a FileHandler
    for writing logs to a file. Also silences noisy third-party loggers.

    Args:
        name (str): The name for the logger (e.g., 'daq_data').
        level (int): The logging level (e.g., logging.INFO).

    Returns:
        logging.Logger: A configured logger instance.
    """
    # logging.getLogger("watchfiles").setLevel(logging.WARNING)

    # define log directory and file path
    log_dir = Path("ublox_control/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir / f"{name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.log"

    # Get the logger and set its level.
    # We use getLogger(name) to get our specific application logger.
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent messages from being passed to the root logger
    logger.propagate = False

    # Configure and add the RichHandler for console output
    # This handler is for pretty-printing logs to the terminal.
    console_handler = RichHandler(
        level=level,
        show_time=True,
        show_level=True,
        show_path=False, # Set to False for cleaner output
        rich_tracebacks=True,
        tracebacks_theme="monokai",
    )
    console_handler.setFormatter(logging.Formatter("[%(funcName)s()] %(message)s", datefmt="%H:%M:%S"))

    # Configure and add the FileHandler for file output
    # This handler writes logs to the file defined above.
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # Add handlers to the logger, but only if they haven't been added already
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


""" Redis utility functions """
def get_f9t_redis_key(chip_name, chip_uid, prot_msg):
    """
    Returns the hashset key for the given prot_msg and chip
    @param chip_uid: the unique chip ID returned by the `UBX-SEC-UNIQID` message. Must be a 10-digit hex integer.
    @param prot_msg: u-blox protocol message name (e.g. `UBX-TIM-TP`) as specified in the ZED-F9T data sheet.
    @param chip_name: chip name. For now, this will always be `ZED-F9T`, but in the future we may want to record data for other u-blox chip types.
    @return: Redis hash set key in the following format "UBLOX_{chip_name}_{chip_uid}_{data_type}", where each field is uppercase.
    """
    # Verify the chip_uid is a 10-digit hex number
    chip_uid_emsg = f"chip_uid must be a 10-digit hex integer. Got {chip_uid=}"
    try:
        if len(chip_uid) != 10:
            raise ValueError(chip_uid_emsg)
        int(chip_uid, 16)   # verifies chip_uid is a valid hex integer
    except ValueError or AssertionError:
        raise ValueError(chip_uid_emsg)
    return f"UBLOX_{chip_name.upper()}_{chip_uid.upper()}_{prot_msg.upper()}"


""" Testing utils """

# async def run_all_tests(
#     test_fn_list: List[Callable[..., Tuple[bool, str]]],
#     args_list: List[List[Any]],
# ) -> Tuple[bool, type(ublox_control_pb2.TestCase.TestResult)]:
#     """
#     Runs each test function in [test_functions], now supporting async functions.
#     """
#     assert len(test_fn_list) == len(args_list), "test_fn_list must have the same length as args_list"
#     def get_test_name(test_fn):
#         return f"%s.%s" % (test_fn.__module__, test_fn.__name__)
#
#     all_pass = True
#     test_results = []
#     for test_fn, args in zip(test_fn_list, args_list):
#         if inspect.iscoroutinefunction(test_fn):
#             test_result, message = await test_fn(*args)
#         else:
#             test_result, message = test_fn(*args)
#
#         all_pass &= test_result
#         test_result = ublox_control_pb2.TestCase(
#             name=get_test_name(test_fn),
#             result=TestCase.TestResult.PASS if test_result else TestCase.TestResult.FAIL,
#             message=message
#         )
#         test_results.append(test_result)
#     return all_pass, test_results
#

def test_redis_connection(host, port=6379, socket_timeout=1, logger=None) -> Tuple[bool, str]:
    """
    Test Redis connection with specified connection parameters.
        1. Connect to Redis.
        2. Perform a series of pipelined write operations to a test hashset.
        3. Verify whether these writes were successful.
    Returns number of failed operations. (0 = test passed, 1+ = test failed.)
    """
    failures = 0

    try:
        # print(f"Connecting to {host}:{port}")
        if logger: logger.debug(f"Connecting to {host}:{port}")
        r = redis.Redis(host=host, port=port, db=0, socket_timeout=socket_timeout)
        if not r.ping():
            # raise FileNotFoundError(f'Cannot connect to {host}:{port}')
            if logger: logger.error(f"Cannot connect to {host}:{port}")
            return False, f'Cannot connect to {host}:{port}'

        timestamp = datetime.datetime.now().isoformat()
        # Create a redis pipeline to efficiently send key updates.
        pipe = r.pipeline()

        # Queue updates to a test hash: write current timestamp to 10 test keys
        for i in range(20):
            field = f't{i}'
            value = datetime.datetime.now().isoformat()
            pipe.hset('TEST', field, value)

        # Execute the pipeline and get results
        results = pipe.execute(raise_on_error=False)

        # Check if each operation succeeded
        success = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                success.append('0')
                failures += 1
                print(f"Command {i} failed: {result=}")
                if logger: logger.debug(f"Command {i} failed: {result=}")
            else:
                success.append('1')
        # print(f'[{timestamp}]: success = [{" ".join(success)}]')
        if logger:
            if all(success):
                logger.debug(f'success = [{" ".join(success)}]')

    except Exception as e:
        # Fail safely by reporting a failure in case of any exceptions
        return False, f"Error: {e}"
    test_result = (failures == 0)
    return test_result, f"{failures=}"


# def get_experiment_dir(start_timestamp, device):
#     device_name = device.split('/')[-1]
#     return f'{packet_data_dir}/start_{start_timestamp}.device_{device_name}'

""" ----- F9T I/O functions ---- """

# def get_f9t_unique_id(device):
#     """
#     Poll the unique ID of the f9t chip.
#     We need to write a custom poll command because the pyubx2 library doesn't implement this cfg message.
#     """
#     # UBX-SEC-UNIQID poll message (class 0x27, id 0x03)
#     UBX_UNIQID_POLL = bytes([0xB5, 0x62, 0x27, 0x03, 0x00, 0x00, 0x2A, 0x8F])
#     with Serial(device, F9T_BAUDRATE, timeout=2) as stream:
#         ubr = UBXReader(stream)
#         # Flush any existing input
#         stream.reset_input_buffer()
#         print("Sending UBX-SEC-UNIQID poll...")
#         stream.write(UBX_UNIQID_POLL)
#         stream.flush()
#         # Wait for and parse the response
#         start_time = time.time()
#         while True:
#             if time.time() - start_time > 5:
#                 print("Timeout waiting for response.")
#                 break
#             raw_data, parsed_data = ubr.read()
#             if parsed_data and parsed_data.identity == 'SEC-UNIQID':
#                 # The unique ID is in parsed_data.uniqueId (should be bytes)
#                 unique_id = parsed_data.uniqueId.hex()
#                 print(f"Unique ID: {unique_id}")
#                 return unique_id
#             # # Look for UBX-SEC-UNIQID response (class 0x27, id 0x03)
#             # if raw_data and raw_data[2] == 0x27 and raw_data[3] == 0x03:
#             #     # Payload is at raw_data[6:-2], uniqueId is bytes 4:36 of payload
#             #     payload = raw_data[6:-2]
#             #     if len(payload) >= 36:
#             #         unique_id = payload[4:36].hex()
#             #         print(f"ZED-F9T Unique ID: {unique_id}")
#             #     else:
#             #         print("Received payload too short.")
#             #     break

def poll_f9t_config(device, cfg=default_f9t_cfg):
    """
    Poll the current configuration settings for each cfg_key specified in the cfg dict.
    On startup, should be 0 by default.
    """
    layer = POLL_LAYER_RAM
    position = 0
    ubx_cfg = cfg['protocol']['ubx']

    msg = UBXMessage.config_poll(layer, position, keys=ubx_cfg['cfg_keys'])
    print('Polling configuration:')
    with Serial(device, F9T_BAUDRATE, timeout=ubx_cfg['timeout (s)']) as stream:
        stream.write(msg.serialize())
        ubr_poll_status = UBXReader(stream, protfilter=UBX_PROTOCOL)
        raw_data, parsed_data = ubr_poll_status.read()
        if parsed_data is not None:
            print('\t', parsed_data)


def set_f9t_config(device, cfg=default_f9t_cfg):
    """Tell chip to start sending metadata packets for each cfg_key"""
    layer = SET_LAYER_RAM
    transaction = TXN_NONE
    timeout = cfg['timeout (s)']
    ubx_cfg = cfg['protocol']['ubx']

    # Tell chip to start sending metadata packets for each cfg_key. Note: Unspecified keys are initialized to 0.
    cfgData = [(cfg_key, 1) for cfg_key in ubx_cfg['cfg_keys']]  # 1 = start sending packets of type cfg_key.
    msg = UBXMessage.config_set(layer, transaction, cfgData)

    with Serial(device, F9T_BAUDRATE, timeout=timeout) as stream:
        print('Updating configuration:')
        stream.write(msg.serialize())
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        for i in range(1):
            raw_data, parsed_data = ubr.read()
            if parsed_data is not None:
                print('\t', parsed_data)


def ubx_to_dict(msg: UBXMessage) -> Dict[str, Any]:
    """
    Converts the parsed attributes of a UBXMessage to a dictionary suitable for JSON.
    It filters out internal attributes (starting with '_') and methods.
    Byte strings are converted to human-readable format.
    """
    if not isinstance(msg, UBXMessage):
        return {}

    d = {}
    # Iterate over public attributes
    for attr in filter(lambda a: not a.startswith('_'), dir(msg)):
        value = getattr(msg, attr)
        # Exclude methods and other non-serializable types
        if callable(value):
            continue

        # Convert bytes to a readable string format
        if isinstance(value, bytes):
            # Attempt to decode as UTF-8, fallback to hex representation
            try:
                d[attr] = value.decode('utf-8').strip('\x00')
            except UnicodeDecodeError:
                d[attr] = value.hex()
        # Ensure other values are JSON-serializable
        elif isinstance(value, (int, float, str, bool, list, dict, type(None))):
            d[attr] = value
        # Skip other types that can't be easily serialized

    return d
