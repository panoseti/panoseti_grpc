#!/usr/bin/env python3
import time
import asyncio
import signal
import argparse
import logging
import json
import os.path
import sys
from rich import print
from rich.pretty import pprint
from pathlib import Path

from google.protobuf.json_format import MessageToDict

from panoseti_grpc.generated import (
    daq_data_pb2,
    daq_data_pb2_grpc
)
from panoseti_grpc.generated.daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest
from .resources import make_rich_logger
from .client import DaqDataClient, AioDaqDataClient
from .plot import PulseHeightDistribution, PanoImagePreviewer

CFG_DIR = Path('daq_data/config')

async def run_pulse_height_distribution(
    addc: AioDaqDataClient,
    host: str,
    plot_update_interval: float,
    module_ids: tuple[int],
    shutdown_event: asyncio.Event,
    durations_seconds=(5, 10, 30),
):
    """Streams pulse-height images and updates max pixel distribution histograms."""
    if len(module_ids) == 0:
        print("no module_ids specified, using data from all modules to make ph distribution")
    elif len(module_ids) > 1:
        print("more than one module_id specified to make ph distribution")
    # pulse-height image streaming only
    stream_images_responses = await addc.stream_images(
        host,
        stream_movie_data=False,
        stream_pulse_height_data=True,
        update_interval_seconds=-1,
        module_ids=module_ids,
        parse_pano_images=True
    )
    ph_dist = PulseHeightDistribution(durations_seconds, module_ids, plot_update_interval)
    async for parsed_pano_image in stream_images_responses:
        if shutdown_event.is_set():
            break
        ph_dist.update(parsed_pano_image)


async def run_pano_image_preview(
    addc: AioDaqDataClient,
    host: str,
    stream_movie_data: bool,
    stream_pulse_height_data: bool,
    update_interval_seconds: float,
    plot_update_interval: float,
    module_ids: tuple[int],
    shutdown_event: asyncio.Event,
    wait_for_ready: bool = False,
):
    """Streams PanoImages from an active observing run."""
    # Make the RPC call
    stream_images_responses = await addc.stream_images(
        host,
        stream_movie_data=stream_movie_data,
        stream_pulse_height_data=stream_pulse_height_data,
        update_interval_seconds=update_interval_seconds,
        module_ids=module_ids,
        parse_pano_images=True,
        wait_for_ready=wait_for_ready,
    )
    previewer = PanoImagePreviewer(stream_movie_data, stream_pulse_height_data, module_ids, plot_update_interval=plot_update_interval)
    async for parsed_pano_image in stream_images_responses:
        if shutdown_event.is_set():
            break
        previewer.update(parsed_pano_image)

async def run_speed_test(
    addc: AioDaqDataClient,
    host: str,
    module_ids: tuple[int],
    shutdown_event: asyncio.Event,
):
        # Stream images of all types as fast as possible
        stream_images_responses = await addc.stream_images(
            host,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=-1,
            module_ids=module_ids,
            parse_pano_images=True
        )
        # ph_dist = PulseHeightDistribution(durations_seconds, module_ids, plot_update_interval)
        npackets = 0
        ref_t = time.monotonic()
        logger = make_rich_logger("daq_data.cli", level=logging.INFO)
        async for _ in stream_images_responses:
            if shutdown_event.is_set():
                break
            curr_t = time.monotonic()
            npackets += 1
            delta_t = curr_t - ref_t
            if delta_t > 1:
                logger.info(f"{npackets:>6} pkts / {delta_t:6.5f}s = {npackets / delta_t: 10.5f} (pkts/s)")
                npackets = 0
                ref_t = curr_t


async def do_ping_fn(addc, host):
    if host is None:
        raise ValueError("--host must be specified for --ping")
    if await addc.ping(host):
        print(f"PING {host=}: [green] success [/green]")
    else:
        print(f"PING {host=}: [red] failed [/red]")

async def do_list_hosts_fn(addc):
    print(f"DAQ host status (True = valid, False = invalid):")
    host_status_dict = await addc.get_daq_host_status()
    pprint(host_status_dict, expand_all=True)

async def do_reflect_services_fn(addc: AioDaqDataClient, host):
    print("-------------- ReflectServices --------------")
    try:
        services = await addc.reflect_services(host)
        print(services)
    except ValueError as e:
        print(f"Error reflecting services: {e}")

async def do_init_fn(addc, host, hp_io_cfg, timeout=15.0):
    print("-------------- InitHpIo --------------")
    if host is None:
        print(f"Initializing hp_io thread on all hosts")
    else:
        print(f"Initializing hp_io thread on {host=}")
    await addc.init_hp_io(host, hp_io_cfg, timeout=timeout)

def parse_log_level(log_level):
    if log_level == 'debug':
        return logging.DEBUG
    elif log_level == 'info':
        return logging.INFO
    elif log_level == 'warning':
        return logging.WARNING
    elif log_level == 'error':
        return logging.ERROR
    elif log_level == 'critical':
        return logging.CRITICAL
    else:
        raise ValueError(f"Invalid log level: {log_level}")

def load_hp_io_cfg(args):
    hp_io_cfg = None
    do_init = False
    if args.init_sim or args.cfg_path is not None:
        do_init = True
        if args.init_sim:
            hp_io_cfg_path = CFG_DIR / 'hp_io_config_simulate.json'
        elif args.cfg_path is not None:
            hp_io_cfg_path = f'{args.cfg_path}'
        else:
            hp_io_cfg_path = None

        # try to open the config file
        if hp_io_cfg_path is None:
            raise ValueError("Either --init-sim or --init must be specified with a valid config path")
        elif not hp_io_cfg_path and not os.path.exists(hp_io_cfg_path):
            raise FileNotFoundError(f"Config file not found: '{os.path.abspath(hp_io_cfg_path)}'")
        else:
            with open(hp_io_cfg_path, "r") as f:
                hp_io_cfg = json.load(f)
    return hp_io_cfg, do_init

async def run_demo_api(args):
    # Graceful Shutdown Setup
    shutdown_event = asyncio.Event()

    def _signal_handler(*_):
        logging.getLogger("daq_data.client").info(
            "Shutdown signal received, closing client stream..."
        )
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    do_ping = args.ping
    do_list_hosts = args.list_hosts
    do_reflect_services = args.reflect_services
    hp_io_cfg, do_init = load_hp_io_cfg(args)
    do_plot = args.plot_view or args.plot_phdist or args.plot_speed

    refresh_period = args.refresh_period
    host = args.host
    module_ids = args.module_ids
    log_level = parse_log_level(args.log_level)
    async with AioDaqDataClient(args.daq_config_path, args.net_config_path, stop_event=shutdown_event, log_level=log_level) as addc:
        if do_ping:
            await do_ping_fn(addc, host)

        if do_list_hosts:
            await do_list_hosts_fn(addc)

        if do_reflect_services:
            await do_reflect_services_fn(addc, host)

        if do_init:
            # concurrently send init commands to all DAQ nodes
            print(f"Initializing hp_io thread with config: {hp_io_cfg}")
            await do_init_fn(addc, host, hp_io_cfg)

        if do_plot:
            valid_daq_hosts = await addc.get_valid_daq_hosts()
            if host is not None and host not in valid_daq_hosts:
                raise ValueError(f"Invalid host: {host}. Valid hosts: {valid_daq_hosts}")

            plot_tasks = []
            if args.plot_view:
                plot_view_task = asyncio.create_task(
                    run_pano_image_preview(
                        addc,
                        host,
                        stream_movie_data=True,
                        stream_pulse_height_data=True,
                        update_interval_seconds=refresh_period,  # np.random.uniform(1.0, 1.0),
                        plot_update_interval=refresh_period,
                        module_ids=module_ids,
                        wait_for_ready=True,
                        shutdown_event=shutdown_event
                    )
                )
                plot_tasks.append(plot_view_task)

            if args.plot_phdist:
                plot_phdist_task = asyncio.create_task(
                    run_pulse_height_distribution(
                        addc,
                        host,
                        plot_update_interval=refresh_period,
                        durations_seconds=(10, 60, 600),
                        module_ids=module_ids,
                        shutdown_event=shutdown_event
                    )
                )
                plot_tasks.append(plot_phdist_task)
            if args.plot_speed:
                plot_speed_task = asyncio.create_task(
                    run_speed_test(
                        addc,
                        host,
                        module_ids=module_ids,
                        shutdown_event=shutdown_event
                    )
                )
                plot_tasks.append(plot_speed_task)
            # use gather to concurrently do different plots
            await asyncio.gather(*plot_tasks)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "daq_config_path",
        help="path to daq_config.json file for the current observing run",
    )

    parser.add_argument(
        "net_config_path",
        help="path to network_config.json file for the current observing run",
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
        "--plot-speed",
        help="whether to create a live speed test for the specified module id",
        action="store_true",
    )

    parser.add_argument(
        "--refresh-period",
        help="period between plot refresh events (in seconds). Default: 1.0",
        default=1.0,
        type=float,
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

    args = parser.parse_args()
    try:
        asyncio.run(run_demo_api(args))
    except KeyboardInterrupt:
        print("\nClient stopped forcefully.")
