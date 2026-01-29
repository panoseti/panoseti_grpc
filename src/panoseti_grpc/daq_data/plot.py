
import time
from datetime import datetime
from collections import deque

from IPython.display import display, clear_output
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
from matplotlib.ticker import MaxNLocator
import textwrap

from panoseti_grpc.panoseti_util import pff

class PulseHeightDistribution:
    def __init__(self, durations_seconds, module_ids, plot_update_interval):
        self.durations = list(durations_seconds)
        self.module_ids = list(module_ids)
        self.plot_update_interval = plot_update_interval
        self.n_durations = len(self.durations)
        self.start_times = {mod: [time.time() for _ in range(self.n_durations)] for mod in self.module_ids}
        self.hist_data = {mod: [deque() for _ in range(self.n_durations)] for mod in self.module_ids}
        self.vmins = {mod: [float('inf')] * self.n_durations for mod in self.module_ids}
        self.vmaxs = {mod: [float('-inf')] * self.n_durations for mod in self.module_ids}
        self.module_colors = self._get_palette(self.module_ids)
        self.fig, self.axes = self._make_fig_axes()
        self.last_plot_update_time = time.time()
        self.num_rescale = 2
        self.jupyter_notebook = False  # set externally if running in notebook

    def _get_palette(self, module_ids):
        palette = sns.color_palette('husl', n_colors=len(module_ids))
        return {mod: palette[i] for i, mod in enumerate(module_ids)}

    def _make_fig_axes(self):
        height = max(2.9 * self.n_durations, 6)
        plt.ion()
        fig, axes = plt.subplots(self.n_durations, 1, figsize=(6, height))
        if self.n_durations == 1:
            axes = [axes]
        return fig, axes

    def _add_new_module(self, module_id):
        self.module_ids.append(module_id)
        self.start_times[module_id] = [time.time()] * self.n_durations
        self.hist_data[module_id] = [deque() for _ in range(self.n_durations)]
        self.vmins[module_id] = [float('inf')] * self.n_durations
        self.vmaxs[module_id] = [float('-inf')] * self.n_durations
        # Update palette
        self.module_colors = self._get_palette(self.module_ids)
        self.num_rescale = 2

    def update(self, parsed_pano_image):
        module_id = parsed_pano_image['module_id']
        pano_type = parsed_pano_image['type']
        image = parsed_pano_image['image_array']

        if pano_type != 'PULSE_HEIGHT':
            return

        # Add new module if needed (rare, so minimal cost)
        if module_id not in self.hist_data:
            self._add_new_module(module_id)

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

        # Only plot if interval elapsed
        curr_time = time.time()
        if curr_time - self.last_plot_update_time > self.plot_update_interval:
            self.plot()
            self.last_plot_update_time = curr_time

    def plot(self):
        for i, duration in enumerate(self.durations):
            ax = self.axes[i]
            ax.clear()
            all_refresh = [self.start_times[mod][i] for mod in self.module_ids if self.hist_data[mod][i]]
            last_refresh = (
                datetime.fromtimestamp(max(all_refresh)).strftime('%Y-%m-%d %H:%M:%S') if all_refresh else "Never"
            )
            # Plot histograms for all modules
            for mod in self.module_ids:
                values = self.hist_data[mod][i]
                if values:
                    sns.histplot(
                        values, bins=75, kde=True, stat='density',
                        element='step', label=f'{mod}',
                        color=self.module_colors[mod], ax=ax
                    )
            ax.set_title(
                f"Refresh interval = {duration}s | Last refresh = {last_refresh}",
                fontsize=12
            )
            ax.set_xlabel("ADC Value")
            ax.set_ylabel("Density")
            ax.legend(title="Module", fontsize=9, title_fontsize=10)
        if self.num_rescale > 0:
            self.fig.tight_layout()
            self.num_rescale -= 1
        self.fig.suptitle("Distribution of Max Pulse-Heights")
        self.fig.canvas.draw_idle()
        self.fig.canvas.flush_events()

        if self.jupyter_notebook:
            clear_output(wait=True)
            display(self.fig)


class PanoImagePreviewer:
    def __init__(
        self,
        stream_movie_data: bool,
        stream_pulse_height_data: bool,
        module_id_whitelist: tuple[int]=(),
        text_width=30,
        font_size=8,
        row_height=3,
        window_size=10,
        plot_update_interval=1.0,
        jupyter_notebook=False
    ):
        self.stream_movie_data = stream_movie_data
        self.stream_pulse_height_data = stream_pulse_height_data
        self.module_id_whitelist = module_id_whitelist
        self.plot_update_interval = plot_update_interval
        self.last_plot_update_time = time.monotonic()
        self.jupyter_notebook = jupyter_notebook
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
        self.cmap = 'plasma'
        self.row_height = row_height
        self.num_rescale = 0
        self.last_max = float('-inf')
        self.last_min = float('inf')

    def setup_layout(self, modules):
        """Sets up subplot layout: one row per module, two columns (PH left, Movie right)."""
        # Close any old figure
        if self.fig is not None:
            plt.close(self.fig)
        modules = sorted(modules)
        n_modules = len(modules)
        self.fig, axs = plt.subplots(n_modules, 2, figsize=(self.row_height * 2.2, self.row_height * n_modules))
        if n_modules == 1:
            axs = np.array([axs])
        self.axes_map.clear()
        self.cbar_map.clear()
        self.im_map.clear()
        for row, module_id in enumerate(modules):
            # Left = PULSE_HEIGHT, Right = MOVIE
            self.axes_map[(module_id, 'PULSE_HEIGHT')] = axs[row, 0]
            self.axes_map[(module_id, 'MOVIE')] = axs[row, 1]

            im_ph = axs[row, 0].imshow(np.zeros((16, 16)), cmap=self.cmap)
            self.im_map[(module_id, 'PULSE_HEIGHT')] = im_ph
            im_mov = axs[row, 1].imshow(np.zeros((32, 32)), cmap=self.cmap)
            self.im_map[(module_id, 'MOVIE')] = im_mov

            # Inline colorbars
            divider_ph = make_axes_locatable(axs[row, 0])
            cax_ph = divider_ph.append_axes('right', size='5%', pad=0.05)
            cbar_ph = self.fig.colorbar(im_ph, cax=cax_ph)
            self.cbar_map[(module_id, 'PULSE_HEIGHT')] = cbar_ph

            divider_mov = make_axes_locatable(axs[row, 1])
            cax_mov = divider_mov.append_axes('right', size='5%', pad=0.05)
            cbar_mov = self.fig.colorbar(im_mov, cax=cax_mov)
            self.cbar_map[(module_id, 'MOVIE')] = cbar_mov

        self.num_rescale = 2 * len(modules)    # layout optimization at start
        self.fig.tight_layout()
        if not self.jupyter_notebook:
            plt.ion()
            plt.show()

    def update(self, parsed_pano_image):
        pano_type = parsed_pano_image['type']
        img = parsed_pano_image['image_array']
        module_id = parsed_pano_image['module_id']
        header = parsed_pano_image['header']
        frame_number = parsed_pano_image['frame_number']
        file = parsed_pano_image['file']

        # If this is a new module, re-layout all plots
        if module_id not in self.seen_modules:
            self.seen_modules.add(module_id)
            self.setup_layout(self.seen_modules)

        # Compute min and max pixels for the current image
        curr_max = np.max(img)
        curr_min = np.min(img)

        # Check if a new dynamic range will be observed
        update_clim = curr_max > self.last_max
        update_clim |= curr_min < self.last_min
        # max or min pixel leaving the context window
        update_clim |= (len(self.max_pix_map[pano_type]) == self.window_size) and self.max_pix_map[pano_type][0] == self.last_max
        update_clim |= (len(self.min_pix_map[pano_type]) == self.window_size) and self.min_pix_map[pano_type][0] == self.last_min

        # Enqueue min and max pixels from teh current image
        self.max_pix_map[pano_type].append(curr_max)
        self.min_pix_map[pano_type].append(curr_min)
        im = self.im_map[(module_id, pano_type)]
        im.set_data(img)

        if update_clim:
            self.last_min = np.min(self.min_pix_map[pano_type])
            self.last_max = np.max(self.max_pix_map[pano_type])
            vmax = np.quantile(self.max_pix_map[pano_type], 0.95)
            vmin = np.quantile(self.min_pix_map[pano_type], 0.05)
            # Avoid degenerate scale
            if vmax == vmin:
                vmax += 1
            im.set_clim(vmin, vmax)
            cbar = self.cbar_map.get((module_id, pano_type))
            if cbar:
                cbar.ax.tick_params(labelsize=8)
                cbar.locator = MaxNLocator(nbins=6)
                cbar.update_ticks()
                cbar.ax.set_ylabel('ADC', rotation=270, labelpad=10, fontsize=8)
                cbar.ax.yaxis.set_label_position("right")

        # Update axes text
        ax = self.axes_map.get((module_id, pano_type))
        if ax is not None:
            ax_title = (
                f"{pano_type}"
                + (f"\n" if 'quabo_num' not in header else f": Q{int(header['quabo_num'])}\n")
                + f"unix_t = {header['pandas_unix_timestamp'].time()}\n"
                + f"frame_no = {frame_number}\n"
                + textwrap.fill(f"file = {file}", width=self.text_width)
            )
            ax.set_title(ax_title, fontsize=self.font_size)
            ax.tick_params(axis='both', which='major', labelsize=8, length=4, width=1)

        # Set window title for context
        parsed_name = pff.parse_name(file)
        if parsed_name and 'start' in parsed_name:
            start = parsed_name['start']
        else:
            start = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if len(self.module_id_whitelist) > 0:
            plt_title = f"Obs data from {start}, module_ids={set(self.module_id_whitelist)} [filtered]"
        else:
            plt_title = f"Obs data from {start}, module_ids={self.seen_modules} [all]"
        self.fig.suptitle(plt_title)

        # Only trigger a draw if enough time has elapsed (rate-limited)
        curr_time = time.monotonic()
        if curr_time - self.last_plot_update_time > self.plot_update_interval:
            self.plot()
            self.last_plot_update_time = curr_time

    def plot(self):
        # For rapid updates: only trigger full redraws when absolutely needed
        if self.num_rescale > 0:
            self.fig.tight_layout()
            self.num_rescale -= 1

        if not self.jupyter_notebook:
            self.fig.canvas.draw_idle()
            self.fig.canvas.flush_events()
        else:
            clear_output(wait=True)
            display(self.fig)