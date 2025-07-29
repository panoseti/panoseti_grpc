#!/usr/bin/env python3
import signal
import argparse
import logging
import json
import os.path
import sys
from rich import print
from pathlib import Path

import grpc
from google.protobuf.json_format import MessageToDict

from daq_data import (
    daq_data_pb2,
    daq_data_pb2_grpc
)
from .daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest
from .daq_data_client import DaqDataClient
from .daq_data_visualizations import PulseHeightDistribution, PanoImagePreviewer

CFG_DIR = Path('daq_data/config')

def run_pulse_height_distribution(
    ddc: DaqDataClient,
    host: str,
    plot_update_interval: float,
    module_ids: tuple[int],
    durations_seconds=(5, 10, 30),
):
    """Streams pulse-height images and updates max pixel distribution histograms."""
    ph_dist = PulseHeightDistribution(durations_seconds, module_ids, plot_update_interval)
    # pulse-height image streaming only
    stream_images_responses = ddc.stream_images(
        host,
        stream_movie_data=False,
        stream_pulse_height_data=True,
        update_interval_seconds=-1,
        module_ids=module_ids,
        parse_pano_images=True
    )

    for parsed_pano_image in stream_images_responses:
        ph_dist.update(parsed_pano_image)


def run_pano_image_preview(
        ddc: DaqDataClient,
        host: str,
        stream_movie_data: bool,
        stream_pulse_height_data: bool,
        update_interval_seconds: float,
        module_ids: tuple[int],
        wait_for_ready: bool = False,
):
    """Streams PanoImages from an active observing run."""
    # Create visualizer
    previewer = PanoImagePreviewer(stream_movie_data, stream_pulse_height_data, module_ids)
    # Make the RPC call
    stream_images_responses = ddc.stream_images(
        host,
        stream_movie_data=stream_movie_data,
        stream_pulse_height_data=stream_pulse_height_data,
        update_interval_seconds=update_interval_seconds,
        module_ids=module_ids,
        parse_pano_images=True,
        wait_for_ready=wait_for_ready,
    )
    # Process responses
    for parsed_pano_image in stream_images_responses:
        previewer.update(parsed_pano_image)

def run_demo_api(args):
    with open(args.daq_config_path, "r") as f:
        daq_config = json.load(f)
    # get hp_io_cfg
    hp_io_cfg = None
    do_init_hp_io = False
    if args.init_sim or args.cfg_path is not None:
        do_init_hp_io = True
        if args.init_sim:
            hp_io_cfg_path = f'{CFG_DIR}/hp_io_config_simulate.json'
        elif args.cfg_path is not None:
            hp_io_cfg_path = f'{args.cfg_path}'
        else:
            hp_io_cfg_path = None

        # try to open the config file
        if hp_io_cfg_path is not None and not os.path.exists(hp_io_cfg_path):
            raise FileNotFoundError(f"Config file not found: '{os.path.abspath(hp_io_cfg_path)}'")
        else:
            with open(hp_io_cfg_path, "r") as f:
                hp_io_cfg = json.load(f)

    do_list_hosts = args.list_hosts
    do_reflect_services = args.reflect_services
    host = args.host
    do_ping = args.ping
    module_ids = args.module_ids

    # parse args for plotting
    do_plot = args.plot_view or args.plot_phdist
    if args.plot_phdist:
        if len(module_ids) == 0:
            print("no module_ids specified, using data from all modules to make ph distribution")
        elif len(module_ids) > 1:
            print("more than one module_id specified to make ph distribution")
    # parse log level
    log_level = args.log_level
    if log_level == 'debug':
        log_level = logging.DEBUG
    elif log_level == 'info':
        log_level = logging.INFO
    elif log_level == 'warning':
        log_level = logging.WARNING
    elif log_level == 'error':
        log_level = logging.ERROR
    elif log_level == 'critical':
        log_level = logging.CRITICAL

    try:
        with DaqDataClient(daq_config, log_level=log_level) as ddc:

            if do_ping:
                if host is None:
                    raise ValueError("host must be specified for ping")
                if ddc.ping(host):
                    print(f"PING {host=}: [green] success [/green]")
                else:
                    print(f"PING {host=}: [red] failed [/red]")

            valid_daq_hosts = ddc.get_valid_daq_hosts()

            if do_list_hosts:
                print(f"Valid DAQ hosts: {valid_daq_hosts}")

            if do_reflect_services:
                print("-------------- ReflectServices --------------")
                if host is not None and host not in valid_daq_hosts:
                    raise ValueError(f"Invalid host: {host}. Valid hosts: {valid_daq_hosts}")
                services = ddc.reflect_services(host)
                print(services)

            if do_init_hp_io:
                print("-------------- InitHpIo --------------")
                # check host
                if host is not None and host not in valid_daq_hosts:
                    raise ValueError(f"Invalid host: {host}. Valid hosts: {valid_daq_hosts}")
                success = ddc.init_hp_io(host, hp_io_cfg, timeout=15.0)

            if do_plot:
                print("-------------- StreamImages --------------")
                # check host
                if host is not None and host not in valid_daq_hosts:
                    raise ValueError(f"Invalid host: {host}. Valid hosts: {valid_daq_hosts}")
                if args.plot_view:
                    run_pano_image_preview(
                        ddc,
                        host,
                        stream_movie_data=True,
                        stream_pulse_height_data=True,
                        update_interval_seconds=2.0,  # np.random.uniform(1.0, 1.0),
                        module_ids=module_ids,
                        wait_for_ready=True,
                    )

                elif args.plot_phdist:
                    run_pulse_height_distribution(
                        ddc,
                        host,
                        plot_update_interval=1.0,
                        durations_seconds=(10, 60, 600),
                        module_ids=module_ids,
                    )
                else:
                    raise ValueError("Invalid plot")
    except grpc.RpcError as rpc_error:
        print(f"{type(rpc_error)}\n{repr(rpc_error)}")


def signal_handler(signum, frame):
    print(f"Signal {signum} received, exiting...")
    sys.exit(0)

if __name__ == "__main__":
    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
        signal.signal(sig, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "daq_config_path",
        help="path to daq_config.json file for the current observing run",
    )

    parser.add_argument(
        "--host",
        help="DaqData server hostname or IP address.",
    )

    parser.add_argument(
        "--ping",
        help="ping the specified host",
        action="store_true",
    )

    parser.add_argument(
        "--list-hosts",
        help="list available DAQ node hosts",
        action="store_true",
    )

    parser.add_argument(
        "--reflect-services",
        help="list available gRPC services on the DAQ node",
        action="store_true",
    )

    parser.add_argument(
        "--init",
        help="initialize the hp_io thread with CFG_PATH='/path/to/hp_io_config.json'",
        type=str,
        dest="cfg_path"
    )

    parser.add_argument(
        "--init-sim",
        help="initialize the hp_io thread to track a simulated run directory",
        action="store_true",
    )

    parser.add_argument(
        "--plot-view",
        help="whether to create a live data previewer",
        action="store_true",
    )

    parser.add_argument(
        "--plot-phdist",
        help="whether to create a live pulse-height distribution for the specified module id",
        action="store_true",
    )

    parser.add_argument(
        "--module-ids",
        help="whitelist for the module ids to stream data from. If empty, data from all available modules are returned.",
        nargs="*",
        type=int,
        default=[],
    )

    default_log_level = 'info'
    parser.add_argument(
        "--log-level",
        help=f"set the log level for the DaqDataClient logger. Default: '{default_log_level}'",
        choices=["debug", "info", "warning", "error", "critical"],
        default=default_log_level
    )

    # run(host="10.0.0.60")
    args = parser.parse_args()
    # run_demo_grpc(args)
    run_demo_api(args)
