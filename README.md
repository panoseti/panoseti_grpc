# panoseti_grpc
Contains gRPC code for the PANOSETI project. See [here](https://github.com/panoseti/panoseti) for the main software repo.

# Environment Setup for gRPC Clients and Servers
Install `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install)), then follow these steps:
```bash
# 1. create the grpc-py39 conda environment
conda create -n grpc-py39 python=3.9
conda activate grpc-py39
conda install -c conda-forge grpcio-tools

# 2. install dependencies
# option 1: build from source (recommended for now)
git clone https://github.com/panoseti/panoseti_grpc.git
cd panoseti_grpc
pip install -r requirements.txt

# option 2: (in development)
pip install panoseti-grpc
```

[//]: # (pip install -r requirements.txt)


# Using the DaqDataClient API
The `DaqDataClient` is a Python gRPC client interface for the PANOSETI DaqData service. It provides a user-friendly API for scientists and developers to connect to DAQ nodes and stream real-time image data for analysis and visualization.

The client should be used as a [context manager](https://book.pythontips.com/en/latest/context_managers.html) to ensure network resources are handled correctly.

See [client.py](daq_data/client.py) for the implementation and [daq_data_client_demo.ipynb](daq_data_client_demo.ipynb) for code examples showing how to use it.

## Developing Real-Time Visualizations

1. Define a visualization class.
2. Implement an `update` method to modify the visualization given a new panoseti image.
3. Follow the code pattern provided in [daq_data_client_demo.ipynb](daq_data_client_demo.ipynb) to receive data with the `DaqDataClient` API:

```python
from daq_data.client import DaqDataClient
from daq_data.plot import PanoImagePreviewer

# 0. Specify configuration file paths
daq_config_path = 'path/to/your/daq_config.json'
network_config_path = 'path/to/your/network_config.json'

# 1. Connect to all DAQ nodes
with DaqDataClient(daq_config_path, network_config_path) as ddc:
    # 2. Instantiate visualization class
    previewer = PanoImagePreviewer(stream_movie_data=True, stream_pulse_height_data=True)

    # 3. Call the StreamImages RPC on all valid DAQ nodes
    pano_image_stream = ddc.stream_images(
        hosts=[],
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=2.0,
        wait_for_ready=True,
        parse_pano_images=True,
    )

    # 4. Update visualization for each pano_image
    for pano_image in pano_image_stream:
        previewer.update(pano_image)
```

<p style="text-align: center;"> <img src="https://github.com/panoseti/panoseti_grpc/raw/main/docs/demo_figure.png" alt="Example visualization with simulated data" width="400"> <br> Figure 1. PanoImagePreviewer visualizing a simulated observing run replaying data from 2024-07-25. </p>


## Client Initialization
The DaqDataClient requires configuration files specifying the IP addresses and data directories of the DAQ nodes and network configuration.
This information is given by [daq_config.json](https://github.com/panoseti/panoseti/wiki/Configuration-files#daq-config-daq_configjson) and [network_config.json](https://github.com/panoseti/panoseti/wiki/Configuration-files#network-config-network_configjson)

Note that the client should always be used as a [context manager](https://book.pythontips.com/en/latest/context_managers.html) to ensure network resources are handled correctly.

```python
from daq_data.client import DaqDataClient

# Instantiate the client using a 'with' statement
with DaqDataClient(daq_config_path, network_config_path) as client:
    # Your code to interact with the client goes here
    valid_hosts = client.get_valid_daq_hosts()
    print(f"Successfully connected to: {valid_hosts}")
```

## API Reference
All methods can accept a single host string or a list of host strings. If the `hosts` argument is omitted, the method will run on all available DAQ nodes that are responsive.
See [The DaqData Service](#the-daqdata-service) for implementation details.

### Checking Server Status
These methods help you verify connectivity and discover the services available on the DAQ nodes.

- `ping(host)`: Checks if a single DAQ host is online and responsive.

- `get_valid_daq_hosts()`: Returns a set of all hosts with DaqData servers that successfully responded to a ping.

- `reflect_services(hosts)`: Lists all available gRPC services and methods on the specified hosts. This is useful for exploring the server's capabilities.

```python
with DaqDataClient(daq_config_path, network_config_path) as client:
    # Get all responsive hosts
    hosts = client.get_valid_daq_hosts()
    print(f"Valid hosts: {hosts}")

    # Discover the services on the first valid host
    if hosts:
        host = list(hosts)[0]
        service_info = client.reflect_services(host)
        print(service_info)
```
### Initializing the Data Source
Before you can stream images, you must initialize the `hp_io` thread on the server. This thread monitors the observing run directory for new data files.
See [InitHpIo](#inithpio) for implementation details.

#### `init_hp_io(hosts, hp_io_cfg)`
Initializes the hp_io thread for a real observing run.

- `hosts`: The DAQ node(s) to initialize.
- `hp_io_cfg`: A dictionary with configuration parameters, as explained in [The hp_io_config.json File](#the-hp_io_configjson-file).

```python
with DaqDataClient(daq_config_path, network_config_path) as client:
    # Load hp_io configuration from a file
    with open('path/to/hp_io_config.json', 'r') as f:
        hp_io_config = json.load(f)
    # Initialize all valid hosts
    success = client.init_hp_io(hosts=None, hp_io_cfg=hp_io_config)
    if success:
        print("Successfully initialized hp_io on all DAQ nodes.")
```
#### `init_sim(host)`
A convenience function to initialize the server in simulation mode, which streams archived data for testing and development.

```python
with DaqDataClient(daq_config_path, network_config_path) as client:
    # Initialize the first valid host in simulation mode
    host = list(client.get_valid_daq_hosts())[0]
    success = client.init_sim(host)
    if success:
        print(f"Successfully initialized simulation on {host}.")
```
### Streaming Image Data
#### stream_images(...)
The primary method for receiving real-time data. It returns an infinite generator that yields image data as it becomes available from the server.
See [StreamImages](#streamimages) for implementation details.

- `hosts`: The DAQ node(s) to stream from.

- `stream_movie_data` (bool): Request movie-mode images.

- `stream_pulse_height_data` (bool): Request pulse-height images.

- `update_interval_seconds` (float): The desired update rate from the server.

- `module_ids` (tuple): A tuple of module IDs to stream. An empty tuple streams all modules.
- `parse_pano_images` (bool): If True, the raw `StreamImagesResponse.PanoImage` protobuf message is parsed
                into a Python dictionary. If False, the raw protobuf
                object is returned. Defaults to True.

```python
# Assume the server has already been initialized.
with DaqDataClient(daq_config_path, network_config_path) as client:
    # Create a request to stream pulse-height data for all modules
    pano_image_stream = client.stream_images(
        hosts=None,
        stream_movie_data=False,
        stream_pulse_height_data=True,
        update_interval_seconds=0.5,
        module_ids=()
    )

    # Process the first 10 images from the stream
    print("Starting image stream...")
    for pano_image in pano_image_stream:
        print(
            f"Received image from Module {pano_image['module_id']} "
            f"with shape {pano_image['image_array'].shape}"
        )
```

#### `PanoImage` Message Format
When `parse_pano_image` is set to True (default), `DaqDataClient.stream_images(...)` 
returns `StreamImagesResponse.PanoImage` as a Python dictionary with the following format:
```python
{
    'type': 'MOVIE',
    'header': {
        'quabo_1': {
            'pkt_tai': 529.0,
            'tv_sec': 1721882092.0,
            'pkt_nsec': 779007484.0,
            'tv_usec': 779356.0,
            'pkt_num': 36441.0
        },
        'quabo_0': {
            'tv_usec': 779336.0,
            'tv_sec': 1721882092.0,
            'pkt_nsec': 779007488.0,
            'pkt_num': 37993.0,
            'pkt_tai': 529.0
        },
        'quabo_3': {
            'tv_usec': 779347.0,
            'tv_sec': 1721882092.0,
            'pkt_nsec': 779007484.0,
            'pkt_num': 33692.0,
            'pkt_tai': 529.0
        },
        'quabo_2': {
            'tv_sec': 1721882092.0,
            'pkt_tai': 529.0,
            'pkt_nsec': 779007492.0,
            'pkt_num': 35058.0,
            'tv_usec': 779356.0
        },
        'wr_unix_timestamp': Decimal('1721882092.779007488'),
        'pandas_unix_timestamp': Timestamp('2024-07-25 04:34:52.779007488')
    },
    'shape': [32, 32],
    'bytes_per_pixel': 2,
    'image_array': array([[554, 184, 161, ..., 178, 317, 199],
       [479, 428, 181, ..., 177, 363, 260],
       [228, 312, 139, ..., 141, 280, 184],
       ...,
       [220, 191, 118, ..., 216, 187, 245],
       [  8, 462, 168, ..., 201, 420, 395],
       [443, 591, 233, ..., 114,  11, 485]], dtype=uint16),
    'file': 'start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_224.seqno_0.debug_TRUNCATED.pff',
    'frame_number': 88,
    'module_id': 224
}
```
- `type`: String specifying the image type (`MOVIE` or `PULSE_HEIGHT`). Corresponds to the PanoImage Type enum.

- `header`:
Dictionary containing original metadata from the protobuf header field, plus timestamp fields added by the parser:
    - Metadata values: e.g., packet/camera fields (`pkt_tai`, `pkt_nsec`, `tv_sec`, possibly subfields like `quabo_0`).
    - `wr_unix_timestamp` (added): Floating-point, the derived Unix timestamp with nanosecond precision, parsed from PanoSETI timing fields.
    - `pandas_unix_timestamp` (added): ISO-format string representing the exact image acquisition time.

- `shape`:
  List of two integers specifying the image shape: [rows, columns]. Currently, only `[16, 16]` and `[32, 32]` are possible.

- `bytes_per_pixel`:
  Integer indicating the number of bytes {1, 2} of each pixel in the `image_array`. Used to determine data type.


- `image_array`:
2D NumPy array data reshaped as specified by `shape`, and properly cast to either `np.uint8`, `np.uint16`, or `np.int16`.

- `file`:
String with the associated filename for the image, if provided.

- `frame_number`: 0-indexed frame number for this image within `file`.

- `module_id`:
Unsigned module ID of the telescope that produced this image.

### Full Example Workflow
This example demonstrates a complete workflow: initialize the server for a simulated run and then stream data from it. This pattern is shown in [daq_data_client_demo.ipynb](daq_data_client_demo.ipynb).

```python
from daq_data.client import DaqDataClient

# 0. Specify configuration file paths
daq_config_path = 'daq_data/config/daq_config_grpc_simulate.json'
network_config_path = 'daq_data/config/network_config_grpc_simulate.json'

# 1. Connect to all DAQ nodes
with DaqDataClient(daq_config_path, network_config_path) as client:
    # 2. Get valid hosts
    valid_hosts = client.get_valid_daq_hosts()
    if not valid_hosts:
        raise RuntimeError("No valid DAQ hosts found.")
    print(f"Connected to: {valid_hosts}")

    # 3. Initialize servers in simulation mode
    all_init_success = client.init_sim(valid_hosts)
    if not all_init_success:
        raise RuntimeError("Failed to initialize one or more servers.")
    print("All servers initialized for simulation.")

    # 4. Stream pulse-height and movie data from all modules
    pano_image_stream = client.stream_images(
        hosts=valid_hosts,
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=1.0,
        module_ids=()
    )

    # 5. Listen to the stream and process data
    print("Starting data stream. Press Ctrl+C to stop.")
    for pano_image in pano_image_stream:
        # In a real application, you would pass this data to a
        # visualization or analysis function.
        print(
            f"Image: Module {pano_image['module_id']}, "
            f"Type: {pano_image['type']}, "
            f"Timestamp: {pano_image['header']['pandas_unix_timestamp']}"
        )
```

## Using the DaqData Client CLI

```
daq_data/cli.py  - demonstrates real-time pulse-height and movie-mode visualizations using the DaqData API.

usage: cli.py [-h] [--host HOST] [--ping] [--list-hosts] [--reflect-services] [--init CFG_PATH] [--init-sim] [--plot-view] [--plot-phdist] [--refresh-period REFRESH_PERIOD]
              [--module-ids [MODULE_IDS ...]] [--log-level {debug,info,warning,error,critical}]
              daq_config_path net_config_path

positional arguments:
  daq_config_path       path to daq_config.json file for the current observing run
  net_config_path       path to network_config.json file for the current observing run

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           DaqData server hostname or IP address.
  --ping                ping the specified host
  --list-hosts          list available DAQ node hosts
  --reflect-services    list available gRPC services on the DAQ node
  --init CFG_PATH       initialize the hp_io thread with CFG_PATH='/path/to/hp_io_config.json'
  --init-sim            initialize the hp_io thread to track a simulated run directory
  --plot-view           whether to create a live data previewer
  --plot-phdist         whether to create a live pulse-height distribution for the specified module id
  --refresh-period REFRESH_PERIOD
                        period between plot refresh events (in seconds). Default: 1.0
  --module-ids [MODULE_IDS ...]
                        whitelist for the module ids to stream data from. If empty, data from all available modules are returned.
  --log-level {debug,info,warning,error,critical}
                        set the log level for the DaqDataClient logger. Default: 'info'

```

Below is an example workflow for using `daq_data/client_cli.py` to view real-time data from a real or simulated observing run directory.

#### On the Headnode
1. Start an observing session ([docs](https://github.com/panoseti/panoseti/wiki/sessions-and-configuration)).
2. Run `start.py` in the `panoseti/control` directory to start an observing run.

#### On each DAQ Node in `/path/to/daq_config.json`
1. Set up the `grpc-py39` environment as described above.
2. Set the working directory to `panoseti_grpc/`.
3. Run `python -m daq_data.server`.

#### On Any Computer
1. Update `hp_io_config.json` or create a new one (see docs below).
2. Set your working directory to `panoseti_grpc/`.
3. Set up the `grpc-py39` environment as described above and activate it.
4. `export DAQ_CFG=/path/to/daq_config.json`: (optional) create a convenient variable for `/path/to/daq_config.json`. If you don't want to do this, replace `$DAQ_CFG` in all following commands with `/path/to/daq_config.json`.
5. `export NET_CFG=/path/to/network_config.json`: (optional) create a convenient variable for `/path/to/network_config.json`. If you don't want to do this, replace `$NET_CFG` in all following commands with `/path/to/network_config.json`.
6. `python -m daq_data.cli -h`: see the available options.
7. `python -m daq_data.cli $DAQ_CFG $NET_CFG --list-hosts`: find DAQ node hosts running valid DaqData gRPC servers. Hostname arguments `H` to `--host` should be in the list of valid hosts returned by this command.
8. Initialize the `hp_io` thread on all DaqData servers:
    - (Real data) `python -m daq_data.cli $DAQ_CFG $NET_CFG --init /path/to/hp_io_config.json`: initialize `hp_io` from `hp_io_config.json`. See [The hp_io_config.json File](#the-hp_io_configjson-file) for details about this config file.
    - (Simulated data) `python -m daq_data.cli $DAQ_CFG $NET_CFG --init-sim`: initialize `hp_io` from `daq_data/config/hp_io_config_simulate.json`. This starts a stream of simulated data.
9. Start visualization apps:
    - `python -m daq_data.cli $DAQ_CFG $NET_CFG --plot-phdist`: make a `StreamImages` request and launch a real-time pulse-height distribution app.
    - `python -m daq_data.cli $DAQ_CFG $NET_CFG --plot-view`: make a `StreamImages` request and launch a real-time frame viewer app.

Commands organized below for convenience:
```bash
# 3. activate the grpc-py39 environment
conda activate grpc-py39

# 4-5. create environment variables
export DAQ_CFG=/path/to/daq_config.json
export NET_CFG=/path/to/network_config.json

# 6. see available options
python -m daq_data.cli -h

# 7. check gRPC server status
python -m daq_data.cli $DAQ_CFG $NET_CFG --list-hosts

# 8. Initialize the hp_io thread on all DaqData servers (choose one)
python -m daq_data.cli $DAQ_CFG $NET_CFG --init /path/to/hp_io_config.json  # real run
python -m daq_data.cli $DAQ_CFG $NET_CFG --init-sim                        # simulated run

# 9. Start visualization apps (choose one)
python -m daq_data.cli $DAQ_CFG $NET_CFG --plot-phdist  # pulse-height distribution
python -m daq_data.cli $DAQ_CFG $NET_CFG --plot-view    # frame viewer
```


Notes:
- On Linux, the `Ctrl+P` keyboard shortcut loads commands from your command history. Useful for running the `python -m daq_data.cli` module with different options.
- `panoseti_grpc` has a package structure, so your working directory should be the repo root, `panoseti_grpc/`, when running modules in `panoseti_grpc/daq_data/`.
- Each script (e.g. `server.py`) should be prefixed with **`python -m daq_data.`** and, because it is a module, be called without the `.py` extension. Following these guidelines gives the example command: **`python -m daq_data.server`**, instead of `daq_data/server.py` or  `python -m daq_data.server.py`.

# The DaqData Service
See [daq_data.proto](protos/daq_data.proto) for the protobuf specification of this service.


<table>
  <tr>
    <td style="text-align: center;">
      <img src="https://github.com/panoseti/panoseti_grpc/raw/main/docs/DaqData_StreamImages_overview.png" alt="DaqData Architecture" width="500"/><br>
      <em>Figure A. DaqData Architecture</em>
    </td>
    <td style="text-align: center;">
      <img src="https://github.com/panoseti/panoseti_grpc/raw/main/docs/DaqData_StreamImages_hp-io.png" alt="DaqData StreamImages hp-io" width="300"/><br>
      <em>Figure B. StreamImages RPC Flow</em>
    </td>
  </tr>
</table>


## Core Remote Procedure Calls

### `StreamImages`

- The gRPC server's `hp_io` thread compares consecutive snapshots of the current run directory to identify the last image frame for each Hashpipe data product, including `ph256`, `ph1024`, `img8`, `img16`. These image frames are subsequently broadcast to ready `StreamImages` clients.
- A given image frame of type `dp` from module `N` will be sent to a client when the following conditions are satisfied:
    1. The time since the last server response to this client is at least as long as the client’s requested `update_interval_seconds`.
    2. The client has requested data of type `dp`.
    3. Module `N` is on the client’s whitelist.
- $N \geq 0$ `StreamImages` clients may be concurrently connected to the server.

### `InitHpIo`

- Enables reconfiguration of the `hp_io` thread during an observing run.
- Requires an observing run to be active to succeed.
- $N \leq 1$ `InitHpIo` clients may be active at any given time. If an `InitHpIo` client is active, no other client may be.

### `Ping`
- Succeeds only if a client can contact the DaqData server. 



## The `hp_io_config.json` File

`hp_io_config.json` is used to configure `InitHpIo` RPCs to initialize the gRPC server's `hp_io` thread.

```json
{
  "data_dir": "/mnt/panoseti",
  "update_interval_seconds": 0.1,
  "force": true,
  "simulate_daq": false,
  "module_ids": [],
  "comments": "Configures the hp_io thread to track observing runs stored under /mnt/panoseti"
}
```

- `data_dir`: the data acquisition directory a Hashpipe instance is writing to. Contains `module_X/` directories.
- `update_interval_seconds`: the period, in seconds, between consecutive snapshots of the run directory. Must be greater than the minimum period specified by the `min_hp_io_update_interval_seconds` field in daq_data/config/daq_data_server_config.json.
- `force`: whether to force a configuration of `hp_io`, even if other clients are currently active.
    - If `true`, the server will stop all active `StreamImages` RPCs then re-configure the `hp_io` thread using the given configuration. During initialization, new `StreamImages` and `InitHpIo` clients may join a waiting queue, but will not be handled until after the configuration has finished (regardless of success or failure). Use this option to guarantee your `InitHpIo` request is handled.
    - If `false`, the `InitHpIo` request will only succeed if no other `StreamImages` RPCs are active. If any `StreamImages` RPCs are active, this `InitHpIo` RPC will immediately return with information about the number of active`StreamImages`. Use this option if other users may be using the server.
- `simulate_daq`: overrides `data_dir` and causes the server to stream data from archived observing data. Use this option for debugging and developing visualizations without access to observatory hardware.
- `module_ids`: whitelist of module data sources.
    - If empty, the server will broadcast data snapshots from all active modules (detected automatically).
    - If non-empty, the server will only broadcast data from the specified modules.




# UbloxControl Service (TODO)
...
