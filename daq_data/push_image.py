import datetime

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from pathlib import Path
import json
import base64
import requests

# Step 1: Connect to DaqData Server and Make Stream Request
from .daq_data_client import DaqDataClient  # Replace with actual import per your README

# Backend target (Grafana will query this REST endpoint for latest images)
BACKEND_API = 'http://127.0.0.1:8000/imagefeed'

class UploadImage:
    def __init__(self, backend_api):
        self.backend_api = backend_api

    def encode_image(self, img: np.ndarray):
        # Apply seaborn colormap: "rocket", "mako", etc.
        plt.figure(figsize=(1, 1), dpi=32)
        plt.axis('off')
        plt.imshow(img, cmap='rocket')
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0)
        plt.close()
        img_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        return img_base64


    def send_image(self, image_base64, timestamp=None):
        payload = {
            "image": f"data:image/png;base64,{image_base64}",
            "timestamp": str(datetime.time())  # Optional
        }
        requests.post(self.backend_api, json=payload)

    def update(self, pano_image):
        img_base64 = self.encode_image(pano_image['image_array'])
        self.send_image(img_base64)




# Configure paths
cfg_dir = Path('daq_data/config')
daq_config_file = 'daq_config_grpc_simulate.json'
hp_io_config_simulate_file = 'hp_io_config_simulate.json'

# Load configuration files
with open(cfg_dir / hp_io_config_simulate_file, 'r') as f:
    hp_io_cfg = json.load(f)

with open(cfg_dir / daq_config_file, 'r') as f:
    daq_config = json.load(f)


# 1. Connect to all DAQ nodes
with DaqDataClient(daq_config) as ddc:
    # 2. Instantiate visualization class
    upload_image = UploadImage(BACKEND_API)

    # 3. Call the StreamImages RPC on all valid DAQ nodes
    pano_image_stream = ddc.stream_images(
        hosts=[],
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=1.0,
        wait_for_ready=True,
        parse_pano_images=True,
    )

    # 4. Update visualization for each pano_image
    for pano_image in pano_image_stream:
        upload_image.update(pano_image)



