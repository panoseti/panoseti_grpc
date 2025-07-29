
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

from panoseti_util import pff

class PulseHeightDistribution:
    def __init__(self, durations_seconds, module_ids, plot_update_interval):
        self.durations = durations_seconds
        self.module_ids = list(module_ids)
        self.plot_update_interval = plot_update_interval
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

        self.last_plot_update_time = time.time()

    def update(self, parsed_pano_image):
        # unpack pano image
        module_id = parsed_pano_image['module_id']
        pano_type = parsed_pano_image['type']
        image = parsed_pano_image['image_array']

        if pano_type == 'PULSE_HEIGHT':
            # img += np.random.poisson(lam=50, size=img.shape)
            curr_time = time.time()
            if curr_time - self.last_plot_update_time > self.plot_update_interval:
                self.plot()
                self.last_plot_update_time = curr_time

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
            module_id_whitelist: tuple[int]=(),
            text_width=30,
            font_size=6,
            row_height=3,
            window_size=100,
            jupyter_notebook=False
    ) -> None:
        self.stream_movie_data = stream_movie_data
        self.stream_pulse_height_data = stream_pulse_height_data
        self.module_id_whitelist = module_id_whitelist
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

            im_ph = axs[row, 0].imshow(np.zeros((16, 16)), cmap=self.cmap)
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
        if not self.jupyter_notebook:
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
        #
        if not self.jupyter_notebook:
            self.fig.canvas.draw()
            self.fig.canvas.flush_events()
        else:
            clear_output(wait=True)
            display(self.fig)
