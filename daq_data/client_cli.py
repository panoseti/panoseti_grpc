#!/usr/bin/env python3

import signal
import argparse
import logging
import json
import os.path
from collections import deque
import time
from datetime import datetime
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

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
from matplotlib.ticker import MaxNLocator
import textwrap

from panoseti_util import pff

CFG_DIR = Path('daq_data/config')


class PulseHeightDistribution:
    def __init__(self, durations_seconds, module_ids):
        self.durations = durations_seconds
        self.module_ids = list(module_ids)
        self.n_durations = len(self.durations)

        # For each module: list of deques per duration window
        self.start_times = {mod: [time.time() for _ in range(self.n_durations)]
                            for mod in self.module_ids}
        self.hist_data = {mod: [deque() for _ in range(self.n_durations)]
                          for mod in self.module_ids}
        self.vmins = {mod: [float('inf')] * self.n_durations for mod in self.module_ids}
        self.vmaxs = {mod: [float('-inf')] * self.n_durations for mod in self.module_ids}

        # Preallocate colors for distinct modules
        palette = sns.color_palette('husl', n_colors=len(self.module_ids))
        self.module_colors = {mod: palette[i] for i, mod in enumerate(self.module_ids)}

        # Figure/axes: one axis per duration window
        height = max(2.9 * self.n_durations, 6)
        plt.ion()
        self.fig, self.axes = plt.subplots(self.n_durations, 1, figsize=(6, height))
        if self.n_durations == 1:
            self.axes = [self.axes]

    def update(self, image, module_id):
        if module_id not in self.hist_data:
            # Dynamically add support for new modules if needed
            self.module_ids.append(module_id)
            self.start_times[module_id] = [time.time()] * self.n_durations
            self.hist_data[module_id] = [deque() for _ in range(self.n_durations)]
            self.vmins[module_id] = [float('inf')] * self.n_durations
            self.vmaxs[module_id] = [float('-inf')] * self.n_durations
            # Assign a color (expand palette if many modules)
            palette = sns.color_palette('husl', n_colors=len(self.module_ids))
            self.module_colors[module_id] = palette[len(self.module_ids)-1]

        max_pixel = int(np.max(image))
        now = time.time()
        for i, duration in enumerate(self.durations):
            if now - self.start_times[module_id][i] > duration:
                self.hist_data[module_id][i].clear()
                self.start_times[module_id][i] = now
                self.vmins[module_id][i] = float('inf')
                self.vmaxs[module_id][i] = float('-inf')
            self.hist_data[module_id][i].append(max_pixel)
            self.vmins[module_id][i] = min(self.vmins[module_id][i], max_pixel)
            self.vmaxs[module_id][i] = max(self.vmaxs[module_id][i], max_pixel)

    def plot(self):
        for i, duration in enumerate(self.durations):
            ax = self.axes[i]
            ax.clear()

            # Compute last refresh (latest start_time) for this duration window
            all_refresh = [
                self.start_times[mod][i] for mod in self.module_ids if self.hist_data[mod][i]
            ]
            if all_refresh:
                last_refresh_unix = max(all_refresh)
                last_refresh = datetime.fromtimestamp(last_refresh_unix).strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_refresh = "Never"

            # Axis limits
            mins = [self.vmins[mod][i] for mod in self.module_ids if self.hist_data[mod][i]]
            maxs = [self.vmaxs[mod][i] for mod in self.module_ids if self.hist_data[mod][i]]
            vmin = min(mins) if mins else 0
            vmax = max(maxs) if maxs else 1

            for mod in self.module_ids:
                values = self.hist_data[mod][i]
                if values:
                    sns.histplot(
                        list(values),
                        bins=100,
                        kde=False,
                        stat='density',
                        element='step',
                        label=f'Module {mod}',
                        color=self.module_colors[mod],
                        ax=ax,
                    )
            ax.set_xlim(vmin - 10, vmax + 10)
            ax.set_title(
                f"Refresh interval = {duration}s | Last refresh = {last_refresh}",
                fontsize=12,
            )
            ax.set_xlabel("ADC Value")
            ax.set_ylabel("Density")
            ax.legend(title="Module", fontsize=9, title_fontsize=10)
        self.fig.suptitle( f"Distribution of Max Pulse-Heights")
        self.fig.tight_layout()
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()


class PanoImagePreviewer:
    def __init__(
            self,
            stream_movie_data: bool,
            stream_pulse_height_data: bool,
            module_id_whitelist: list[int],
            text_width=25,
            font_size=7,
            row_height=2.8,
            window_size=100,
    ) -> None:
        self.stream_movie_data = stream_movie_data
        self.stream_pulse_height_data = stream_pulse_height_data
        self.module_id_whitelist = module_id_whitelist

        self.seen_modules = set()
        self.axes_map = {}
        self.cbar_map = {}
        self.im_map = {}
        self.window_size = window_size
        self.max_pix_map = {'PULSE_HEIGHT': deque(maxlen=self.window_size), 'MOVIE': deque(maxlen=self.window_size)}
        self.min_pix_map = {'PULSE_HEIGHT': deque(maxlen=self.window_size), 'MOVIE': deque(maxlen=self.window_size)}

        self.fig = None
        self.text_width = text_width
        self.font_size = font_size
        # self.cmap = np.random.choice(['magma', 'viridis', 'rocket', 'mako'])
        self.cmap = 'plasma'
        self.row_height = row_height
        self.num_rescale = 0

    def setup_layout(self, modules):
        """Sets up subplot layout: one row per module, two columns (PH left, Movie right)."""
        if self.fig is not None:
            plt.close(self.fig)
        modules = sorted(modules)
        n_modules = len(modules)
        self.fig, axs = plt.subplots(n_modules, 2, figsize=(self.row_height * 2.2, self.row_height * n_modules))
        if n_modules == 1:
            axs = np.array([axs])  # one row per module

        self.num_rescale = 0
        self.axes_map.clear()
        self.cbar_map.clear()
        self.im_map.clear()
        for row, module_id in enumerate(modules):
            self.axes_map[(module_id, 'PULSE_HEIGHT')] = axs[row, 0]
            self.axes_map[(module_id, 'MOVIE')] = axs[row, 1]

            im_ph = axs[row, 0].imshow(np.zeros((32, 32)), cmap=self.cmap)
            self.im_map[(module_id, 'PULSE_HEIGHT')] = im_ph
            im_mov = axs[row, 1].imshow(np.zeros((32, 32)), cmap=self.cmap)
            self.im_map[(module_id, 'MOVIE')] = im_mov

            # Create a divider for each axis for inline colorbar
            divider_ph = make_axes_locatable(axs[row, 0])
            cax_ph = divider_ph.append_axes('right', size='5%', pad=0.05)
            cbar_ph = self.fig.colorbar(im_ph, cax=cax_ph)
            self.cbar_map[(module_id, 'PULSE_HEIGHT')] = cbar_ph

            divider_mov = make_axes_locatable(axs[row, 1])
            cax_mov = divider_mov.append_axes('right', size='5%', pad=0.05)
            cbar_mov = self.fig.colorbar(im_mov, cax=cax_mov)
            self.cbar_map[(module_id, 'MOVIE')] = cbar_mov
        self.fig.tight_layout()
        plt.ion()
        plt.show()

    def update(self, parsed_pano_image):
        module_id = parsed_pano_image['module_id']
        pano_type = parsed_pano_image['type']
        header = parsed_pano_image['header']
        img = parsed_pano_image['image_array']
        frame_number = parsed_pano_image['frame_number']
        file = parsed_pano_image['file']

        # check if this module is new
        if module_id not in self.seen_modules:
            self.seen_modules.add(module_id)
            self.setup_layout(self.seen_modules)

        # update dynamic min and max data dequeues
        self.max_pix_map[pano_type].append(np.max(img))
        self.min_pix_map[pano_type].append(np.min(img))
        vmax = np.quantile(self.max_pix_map[pano_type], 0.95)
        vmin = np.quantile(self.min_pix_map[pano_type], 0.05)
        im = self.im_map[(module_id, pano_type)]
        im.set_data(img)
        im.set_clim(vmin, vmax)

        cbar = self.cbar_map.get((module_id, pano_type))
        cbar.ax.tick_params(labelsize=8)
        cbar.locator = MaxNLocator(nbins=6)
        cbar.update_ticks()
        cbar.ax.set_ylabel('ADC', rotation=270, labelpad=10, fontsize=8)
        cbar.ax.yaxis.set_label_position("right")
        ax = self.axes_map.get((module_id, pano_type))
        if ax is None:
            return

        # Prepare axis title with details
        ax_title = (f"{pano_type}"
                    + ("\n" if 'quabo_num' not in header else f": Q{int(header['quabo_num'])}\n")
                    + f"unix_t = {header['pandas_unix_timestamp'].time()}\n"
                    + f"frame_no = {frame_number}\n")
        ax_title += textwrap.fill(f"file = {file}", width=self.text_width)

        ax.set_title(ax_title, fontsize=self.font_size)
        ax.tick_params(axis='both', which='major', labelsize=8, length=4, width=1)

        start = pff.parse_name(file)['start']
        if len(self.module_id_whitelist) > 0:
            plt_title = f"Obs data from {start}, module_ids={set(self.module_id_whitelist)} [filtered]"
        else:
            plt_title = f"Obs data from {start}, module_ids={self.seen_modules} [all]"
        if self.num_rescale < len(self.seen_modules) * 3:
            self.fig.tight_layout()
            self.num_rescale += 1
        self.fig.suptitle(plt_title)
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()

def run_pulse_height_distribution(
    ddc: DaqDataClient,
    host: str,
    plot_update_interval: float,
    module_ids: tuple[int],
    durations_seconds=(5, 10, 30),
):
    """Streams pulse-height images and updates max pixel distribution histograms."""
    # pulse-height image streaming only
    stream = ddc.stream_images(
        host,
        stream_movie_data=False,
        stream_pulse_height_data=True,
        update_interval_seconds=-1,
        module_ids=module_ids,
        parse_pano_images=True
    )

    ph_dist = PulseHeightDistribution(durations_seconds, module_ids)
    last_plot_update_time = time.time()
    for parsed_pano_image in stream:
        # unpack pano image
        module_id = parsed_pano_image['module_id']
        pano_type = parsed_pano_image['type']
        header = parsed_pano_image['header']
        img = parsed_pano_image['image_array']
        frame_number = parsed_pano_image['frame_number']
        file = parsed_pano_image['file']

        if pano_type == 'PULSE_HEIGHT':
            # img += np.random.poisson(lam=50, size=img.shape)
            ph_dist.update(img, module_id)
            curr_time = time.time()
            if curr_time - last_plot_update_time > plot_update_interval:
                ph_dist.plot()
                last_plot_update_time = curr_time


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
    previewer = PanoImagePreviewer(
        stream_movie_data, stream_pulse_height_data,
        module_ids, row_height=3, font_size=6, text_width=30, window_size=1000
    )
    # Make the RPC call
    response_stream = ddc.stream_images(
        host,
        stream_movie_data=stream_movie_data,
        stream_pulse_height_data=stream_pulse_height_data,
        update_interval_seconds=update_interval_seconds,
        module_ids=module_ids,
        parse_pano_images=True,
    )

    # Process responses
    last_t = time.monotonic()
    for parsed_pano_image in response_stream:
        curr_t = time.monotonic()
        # print(f"time elapsed: {curr_t - last_t:.2f} s")
        last_t = curr_t

        previewer.update(parsed_pano_image)

def run_demo_api(args):
    with open(args.daq_config_path, "r") as f:
        daq_config = json.load(f)
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

    # parse args for plotting
    do_plot = args.plot_view or args.plot_phdist
    do_list_hosts = args.list_hosts
    do_reflect_services = args.reflect_services
    host = args.host
    module_ids = args.module_ids
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
            valid_daq_hosts = ddc.get_valid_daq_hosts()

            if do_list_hosts:
                print("-------------- ListHosts --------------")
                print(f"Valid DAQ hosts: {valid_daq_hosts}")

            if do_reflect_services:
                print("-------------- ReflectServices --------------")
                services = ddc.reflect_services(host)
                print(services)

            if do_init_hp_io:
                print("-------------- InitHpIo --------------")
                # check host
                if host is None:
                    raise ValueError("host must be specified for initializing hp_io")
                elif host not in valid_daq_hosts:
                    raise ValueError(f"Invalid host: {host}. Valid hosts: {valid_daq_hosts}")
                success = ddc.init_hp_io(host, hp_io_cfg, timeout=15.0)

            elif do_plot:
                print("-------------- StreamImages --------------")
                # check host
                if host is None:
                    raise ValueError("host must be specified for plotting")
                elif host not in valid_daq_hosts:
                    raise ValueError(f"Invalid host: {host}. Valid hosts: {valid_daq_hosts}")
                if args.plot_view:
                    run_pano_image_preview(
                        ddc,
                        host,
                        stream_movie_data=True,
                        stream_pulse_height_data=True,
                        update_interval_seconds=0.5,  # np.random.uniform(1.0, 1.0),
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
    except KeyboardInterrupt:
        print(f"'^C' received, closing connection to the DaqData server at {repr(connection_target)}")
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
        "--host",
        help="DaqData server hostname or IP address. Default: 'localhost'",
        default="localhost"
    )

    parser.add_argument(
        "--init",
        help="initialize the hp_io thread from the file [CFG] in config/ to track an in-progress run directory",
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

    default_log_level = 'info'
    parser.add_argument(
        "--log-level",
        help=f"set the log level for the DaqDataClient logger. Default: '{default_log_level}'",
        choices=["debug", "info", "warning", "error", "critical"],
        default=default_log_level
    )

    parser.add_argument(
        "--module-ids",
        help="whitelist for the module ids to stream data from. If empty, data from all available modules are returned.",
        nargs="*",
        type=int,
        default=[],
    )

    # run(host="10.0.0.60")
    args = parser.parse_args()
    # run_demo_grpc(args)
    run_demo_api(args)
