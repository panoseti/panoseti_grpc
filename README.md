![PANOSETI gRPC CI](https://github.com/panoseti/panoseti_grpc/actions/workflows/ci.yml/badge.svg)
# panoseti_grpc
Contains gRPC code for the PANOSETI project. See [here](https://github.com/panoseti/panoseti) for the main software repo.

# Environment Setup for gRPC Clients and Servers
Install `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install)), then follow these steps:
```bash
# 0. Clone this repo and go to the repo root 
git clone https://github.com/panoseti/panoseti_grpc.git
cd panoseti_grpc

# 1. Create the grpc-py39 conda environment
conda create -n grpc-py39 python=3.9
conda activate grpc-py39
conda install -c conda-forge grpcio-tools

# 2. Install package dependencies
# option 1: (recommended for now)
pip install -r requirements.txt

# option 2: (in development)
pip install panoseti-grpc
```

[//]: # (pip install -r requirements.txt)

# The Snapshot API in Two Slides
<img width="1473" height="764" alt="overview of the api architecture" src="https://github.com/user-attachments/assets/260e2d92-f616-4d61-b475-2e982df11de4" />
<img width="1483" height="758" alt="file i/o parallels" src="https://github.com/user-attachments/assets/f9b79460-93a4-4b42-a40f-621f9e0c8f20" />

# Using the `DaqDataClient` API


`DaqDataClient` is a Python API for the gRPC DaqData service, providing
a simple interface for collecting real-time pulse-height and movie-mode data from an in-progress observing run.

The client should be used as a [context manager](https://book.pythontips.com/en/latest/context_managers.html) to ensure network resources are handled correctly.

See [client.py](daq_data/client.py) for the implementation and [daq_data_client_demo.ipynb](daq_data_client_demo.ipynb) for code examples showing how to use it.

## Developing Real-Time Visualizations

1. Define a function or class for visualizing pulse-height and/or movie-mode data. In the example below, we use `PanoImagePreviewer` for visualization ([code](daq_data/plot.py)).
2. Implement an `update` method to modify the visualization given a new panoseti image. See [PanoImage Message Format](#panoimage-message-format) for details about the structure of each element yielded by `stream_images`.
3. Follow the code patterns provided in [daq_data_client_demo.ipynb](daq_data_client_demo.ipynb) to stream images from the DAQ nodes to your visualization program.

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

### Example Workflow
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

## Using `AioDaqDataClient` 
The `AioDaqDataClient` provides an asynchronous interface to the DaqData service, ideal for I/O bound applications, such as simple visualizations or distribution plotting. 
It is built on [grpc.aio](https://grpc.github.io/grpc/python/grpc_asyncio.html) and is designed for use within an [asyncio](https://docs.python.org/3/library/asyncio.html) event loop.

The API methods mirror the synchronous client, but they are coroutines and must be called with `await`. The client should be used as an asynchronous context manager (`async with`).

- Asynchronous calls: All RPC methods (e.g., `ping`, `init_sim`, `stream_images`) are async and must be awaited.
- Async context manager: The client must be entered using `async with`.
- Async iteration: The `stream_images` method returns an `AsyncGenerator`, which must be iterated over with `async for`.

## Asynchronous Workflow Examples
### Introductory
This example demonstrates how to use the AioDaqDataClient to initialize a simulated run and stream data asynchronously. This pattern is ideal for applications that need to handle concurrent operations efficiently, such as a real-time dashboard or a multi-threaded analysis script.

```python
import asyncio
from daq_data.client import AioDaqDataClient

async def main():
    # 0. Specify configuration file paths
    daq_config_path = 'daq_data/config/daq_config_grpc_simulate.json'
    network_config_path = 'daq_data/config/network_config_grpc_simulate.json'

    # 1. Connect to all DAQ nodes asynchronously
    async with AioDaqDataClient(daq_config_path, network_config_path) as client:
        # 2. Get valid hosts
        valid_hosts = await client.get_valid_daq_hosts()
        if not valid_hosts:
            raise RuntimeError("No valid DAQ hosts found.")
        print(f"Connected to: {valid_hosts}")

        # 3. Initialize servers in simulation mode
        all_init_success = await client.init_sim(valid_hosts)
        if not all_init_success:
            raise RuntimeError("Failed to initialize one or more servers.")
        print("All servers initialized for simulation.")

        # 4. Asynchronously stream data
        pano_image_stream = client.stream_images(
            hosts=valid_hosts,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=1.0,
        )

        # 5. Process the stream with an async for loop
        print("Starting async data stream. Press Ctrl+C to stop.")
        async for pano_image in pano_image_stream:
            print(
                f"Image: Module {pano_image['module_id']}, "
                f"Type: {pano_image['type']}, "
                f"Timestamp: {pano_image['header']['pandas_unix_timestamp']}"
            )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stream stopped.")
```

### Client: Graceful Shutdown with `stop_event`

The asynchronous client, `AioDaqDataClient`, supports a `stop_event` argument for gracefully terminating long-running streams like `stream_images`. This is needed for applications that need to clean up resources properly on a `SIGINT` (Ctrl+C) or `SIGTERM`.

When a `stop_event` (an `asyncio.Event` object) is passed to the client's constructor, the `stream_images` method will monitor it. If the event is set, the client will immediately stop listening for new data, cancel the underlying gRPC stream, and allow the calling coroutine to exit cleanly.

### Example: Robust Asynchronous Workflow
```python
import asyncio
import signal
from daq_data.client import AioDaqDataClient

async def main():
    # 1. Create a shutdown event
    shutdown_event = asyncio.Event()

    # 2. Define a signal handler to set the event
    def _signal_handler(*_):
        print("\\nShutdown signal received, closing client stream...")
        shutdown_event.set()

    # 3. Attach the handler to the asyncio event loop
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # 4. Pass the event to the client constructor
    async with AioDaqDataClient(
        daq_config,
        network_config,
        stop_event=shutdown_event
    ) as client:
        try:
            # The stream will run until Ctrl+C is pressed
            pano_image_stream = await client.stream_images(
                hosts=[],
                stream_movie_data=True,
                stream_pulse_height_data=True,
                update_interval_seconds=1.0,
            )
            
            # Iterate over the async generator
            async for pano_image in pano_image_stream:
                print(f"Received image for module {pano_image['module_id']}")
            
        except asyncio.CancelledError:
            print("Stream cancelled.")

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(main())
        await main_task
    except KeyboardInterrupt:
        print("Client stopped.")
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
      <img src="https://github.com/panoseti/panoseti_grpc/raw/main/docs/DaqData_architecture.png" alt="DaqData Architecture" width="750"/><br>
      <em>DaqData Architecture</em>
    </td>
  </tr>
</table>

## System Architecture
The DaqData service is a high-performance gRPC server designed for distributing real-time streams of PANOSETI images collected by the production observing software. Its architecture is built to handle multiple data streams from either live DAQ hardware (Hashpipe) or a sophisticated simulation engine, providing a unified interface for clients.

The system's data flow is designed for efficiency and modularity:

- External Inputs: Data originates from either a live Hashpipe instance in a production environment or the Simulation Engine during testing. These inputs can write to the filesystem, signal updates via named pipes, or stream data directly over Unix Domain Sockets (UDS).

- Data Ingestion Layer: A set of DataSource classes (`PollWatcherDataSource`, `PipeWatcherDataSource`, `UdsDataSource`) are responsible for monitoring these inputs. Each DataSource is tailored to a specific ingestion method, making the system extensible.

- Server Core: The central `HpIoManager` orchestrates the data flow. It runs the active DataSources, consumes all incoming data from a central `asyncio.Queue`, and updates a Latest Data Cache. This cache holds the most recent frame for each data product, allowing for immediate, low-latency access for clients.

- Client Interaction: Clients connect to the `DaqDataServicer` via gRPC. When a client calls the `StreamImage` RPC, the server reads directly from the Latest Data Cache to stream the most up-to-date images. This architecture decouples data ingestion from client servicing, ensuring that the system remains responsive and scalable.


## Core Remote Procedure Calls

### `StreamImages`

- The gRPC server's `hp_io` thread compares consecutive snapshots of the current run directory to identify the last image frame for each Hashpipe data product, including `ph256`, `ph1024`, `img8`, `img16`. These image frames are subsequently broadcast to ready `StreamImages` clients.
  - Details: `hp_io` assumes that `data_dir/` has the following structure and tracks updates to each `*.pff` file within it.
    ```text
    data_dir/
        ├── module_1/
        │   ├── obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd
        │   │   ├── start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.pff
        │   │   ├── start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_1.pff
        │   │   ...
        │   │   
        │   ├── obs_*/  
        │   │   ...
        │   ...
        │
        ├── module_2/
        │   └── obs_*/
        │       ...
        │
        └── module_N/
            └── obs_*/
    ```
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
- Returns `True` only if a client can contact the DaqData server. 

### `UploadImages`
- Provides a mechanism for injecting data directly into the server's broadcast queue, bypassing the filesystem.
- Ideal for designing high-throughput simulations and testing situations where the filesystem is a primary bottleneck.
  - The server's `"rpc"` simulation mode uses an `AioDaqDataClient` instance to upload thousands of archived PANOSETI images per second using the `UploadImages` RPC.
- Mechanism: The client sends a stream of PanoImage objects. On the server, these images are placed into a high-priority `upload_queue`. The `HpIoManager` consumes from this queue and immediately broadcasts the images to all connected StreamImages clients, just as it would for data detected on the filesystem.


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


## The `daq_data_server_config.json` File
This file configures the core behavior of the DaqData gRPC server.

```json
{
    "init_from_default": false,
    "default_hp_io_config_file": "hp_io_config_simulate.json",
    "unix_domain_socket": "unix:///tmp/daq_data.sock",
    "max_concurrent_rpcs": 100,
    "max_read_queue_size": 50,
    "min_hp_io_update_interval_seconds": 0.001,
    "shutdown_grace_period": 5,
    "read_status_pipe_name": "read_status_2",

    "acquisition_methods": {
        "filesystem_poll": { "enabled": false },
        "filesystem_pipe": { "enabled": false },
        "uds": {
            "enabled": true,
            "data_products": ["ph256", "img16"]
        }
    },

    "simulate_daq_cfg": {
        "simulation_mode": "uds",
        "sim_module_ids": [224],
        "movie_type": "img16",
        "ph_type": "ph256",
        "update_interval_seconds": 0.01,
        "source_data": {
            "movie_pff_path": "path/to/movie.pff",
            "ph_pff_path": "path/to/ph.pff"
        },
        "filesystem_cfg": {
            "sim_data_dir": "daq_data/simulated_data_dir",
            "sim_run_dir_template": "module_{module_id}/obs_SIMULATE",
            "daq_active_file": "module_{module_id}.daq-active"
        },
        "strategies": {
            "filesystem_poll": { "frames_per_pff": 1000 },
            "filesystem_pipe": { "frames_per_pff": 1000 },
            "uds": { "data_products": ["ph256", "img16"] },
            "rpc": {}
        }
    }
}
```
* `init_from_default` (boolean): If `true`, the server automatically starts the `HpIoManager` on boot using the configuration from `default_hp_io_config_file`.
* `default_hp_io_config_file` (string): Path to the default `hp_io_config.json` file to use if `init_from_default` is true.
* `unix_domain_socket` (string): The path for the Unix Domain Socket (UDS) for efficient local inter-process communication. Format: `"unix:///path/to/socket.sock"`.
* `max_concurrent_rpcs` (integer): The maximum number of simultaneous client connections the server will accept.
* `max_read_queue_size` (integer): The buffer size for each client's outgoing data queue.
* `min_hp_io_update_interval_seconds` (float): The minimum allowed value for the data polling interval, to prevent excessive CPU usage.
* `shutdown_grace_period` (integer): The time in seconds the server will wait for active RPCs to finish during a graceful shutdown.
* `read_status_pipe_name` (string): The filename for the named pipe used by the `filesystem_pipe` data source to receive signals from Hashpipe or the simulation.
* **`acquisition_methods`** (object): This section enables or disables the different data ingestion methods. At least one must be enabled for the server to acquire data.
    * `filesystem_poll` (object): Watches a directory for file changes. Less efficient but robust.
    * `filesystem_pipe` (object): Listens to a named pipe for signals that a new file is ready. More efficient than polling.
    * `uds` (object): Listens for data streamed directly over a Unix Domain Socket. This is the highest performance method, bypassing the filesystem for data transfer.
        * `data_products` (array): A list of data products (e.g., `"ph256"`) to accept over UDS.
* **`simulate_daq_cfg`** (object): This section configures the simulation engine, used when an `InitHpIo` request has `simulate_daq: true`.
    * `simulation_mode` (string): The strategy the simulator will use to generate data. Must correspond to an enabled `acquisition_method`. Valid modes: `"filesystem_poll"`, `"filesystem_pipe"`, `"uds"`, `"rpc"`.
    * `sim_module_ids` (array): A list of module IDs to simulate.
    * `movie_type` / `ph_type` (string): The data product types to use for movie and pulse-height frames.
    * `update_interval_seconds` (float): The interval at which the simulation generates and sends new frames.
    * `source_data` (object): Paths to the `.pff` files containing the raw frames to be used for simulation.
    * `filesystem_cfg` (object): Configuration for filesystem-based simulation strategies, including the data directory and file templates.
    * `strategies` (object): Mode-specific configurations for each simulation strategy.




# UbloxControl Service

The `UbloxControl` service provides a high-performance gRPC interface for configuring and streaming data from u-blox ZED-F9T timing modules. It is designed to give remote clients a simple control over the hardware.

The typical workflow involves two main steps:

1. A client sends an `InitF9t` request containing a detailed configuration to set up the ZED-F9T module. The server uses this to connect to the correct serial device, apply settings for GNSS constellations and timing signals, and verify the chip's identity.
2. Once initialized, the client calls the `CaptureUblox` RPC to subscribe to a real-time stream of UBX protocol messages, such as timing and navigation data.

## Core Remote Procedure Calls

The service exposes two primary RPCs for interacting with the ZED-F9T chip.

### `InitF9t`

This RPC is the entry point for configuring the hardware. It is a unary call where the client sends a single request and receives a single response.

`rpc InitF9t(InitF9tRequest) returns (InitF9tResponse)`

**Functionality:**

* **Connects and Verifies**: The server connects to the ZED-F9T using the serial device path specified in the configuration (e.g., `/dev/ttyACM1`). It then polls the chip to detect its model and unique hardware ID, ensuring it matches the client's expected configuration.
* **Applies Configuration**: It applies a list of configuration key-value pairs to the device registers. This includes setting up GNSS signal processing (e.g., enabling GPS L1/L2), configuring the timepulse outputs (TP1/TP2), and enabling specific UBX message types like `TIM-TP` and `NAV-TIMEUTC`.
* **Verifies Settings**: After applying the settings, the server polls the device again to verify that all configuration values were written correctly to the specified memory layer (e.g., RAM).
* **Starts I/O**: On success, the server starts a persistent background task that continuously reads data from the serial port and caches the latest UBX messages.

The request contains the F9T configuration and a `force_init` flag, which, if true, will terminate any existing client streams before re-initializing the chip.

### `CaptureUblox`

This RPC establishes a persistent, server-side stream for receiving real-time data from an already-initialized F9T chip.

`rpc CaptureUblox(CaptureUbloxRequest) returns (stream CaptureUbloxResponse)`

**Functionality:**

* **Client Subscription**: A client calls this RPC to subscribe to data. The request can include an array of regular expression `patterns` to filter for specific message types (e.g., `[".*"]` to receive all messages, or `["TIM-TP", "NAV-TIMEUTC"]` for specific ones).
* **Initial Cache Broadcast**: Upon connection, the server immediately sends the client all currently cached UBX messages that match the requested patterns. This ensures the client has the most recent state without having to wait for new messages.
* **Real-time Streaming**: After the initial broadcast, the server streams new UBX messages to the client in real-time as they are read from the hardware.

Each `CaptureUbloxResponse` message contains the packet's identity (`name`), its raw byte `payload`, and a `parsed_data` structure with the decoded fields.

## The `f9t_config.json5` File
```json5
{
  // General config settings for all F9T chips
  "baud": 115200,
  "apply_to_layers": ["RAM", "BBR"], // Options: RAM, BBR (battery backup RAM), flash, default?
  "verify_layer": "RAM",
  "register_csv": "../initialize/ZED-F9T_Registers.csv",


  "cfg_key_settings": [
    // --- Constellations & signals ---
    { "key": "CFG_SIGNAL_GPS_ENA", "value": 1 },

    // UBX-TIM-TP (qErr) at 1 Hz on USB and/or UART1
    { "key": "CFG_MSGOUT_UBX_TIM_TP_USB", "value": 1 },
    { "key": "CFG_MSGOUT_UBX_TIM_TP_UART1", "value": 1 },
    { "key": "CFG_MSGOUT_UBX_NAV_TIMEUTC_USB",   "value": 1 },
    { "key": "CFG_MSGOUT_UBX_NAV_TIMEUTC_UART1", "value": 1 },
    
    // More keys here...
  ],

  // Chip-specific configuration information
  "f9t_chips": [
    {
      "f9t_uid": "DF03A241BC",
      "host": "localhost",// TODO: get this from daq_config.json + network_config.json
      "port": 50051,    // TODO: get this from daq_config.json + network_config.json
      "device": "/dev/ttyACM1",                         // TODO: replace with real device file
      "position": {
        "format": "LLH",              // "LLH" or "ECEF"
        "lat_deg": 37.4219999,        // degrees
        "lon_deg": -122.0840575,      // degrees
        "height_m": 12.345,           // meters (ellipsoidal)
        "acc_m": 0.02                 // 3D accuracy estimate (meters) for fixed mode
      }
    },
    // More chip configurations here...
  ]
}

```

The behavior of the `InitF9t` RPC is primarily defined by the `f9t_config.json5` file. This file uses the **JSON5 format**, which supports features like comments and trailing commas to improve readability.

The configuration is structured as follows:

* **Global Settings**: Top-level keys that apply to all chips, such as `baud` rate, `apply_to_layers` (which memory to write to, e.g., `["RAM", "BBR"]`), and `verify_layer` (which memory to read from for verification).
* **`cfg_key_settings`**: A list of objects, where each object defines a specific u-blox register to configure. The `"key"` is the register name (e.g., `CFG_SIGNAL_GPS_ENA`), and the `"value"` is the desired setting. These are used to control everything from which GNSS signals are enabled to the frequency and duty cycle of the timepulse outputs.
* **`f9t_chips`**: An array of objects, each defining a specific ZED-F9T device. This allows a single configuration file to manage multiple hardware units. Each object contains:
    * `f9t_uid`: The 10-digit unique ID of the chip, used for verification.
    * `device`: The filesystem path to the serial port (e.g., `/dev/ttyACM1`).
    * `position`: A critical object for timing applications. It specifies the antenna's fixed position in latitude, longitude, and height (`lat_deg`, `lon_deg`, `height_m`) and its accuracy (`acc_m`). This information is required for the F9T's time-only mode to function correctly.


## Basic Usage Example

The following Python example demonstrates how to use an asynchronous client to connect to the `UbloxControl` service, initialize the F9T, and stream data. This pattern is implemented in `simple_client.py`.

```python
import asyncio
import grpc
import copy
from ublox_control import ublox_control_pb2, ublox_control_pb2_grpc
from ublox_control.resources import default_f9t_cfg  # Assumes a loaded config
from google.protobuf.json_format import ParseDict, MessageToDict
from google.protobuf.struct_pb2 import Struct

async def main():
    """
    Client workflow for connecting to UbloxControl, initializing the F9T,
    and capturing UBX data streams.
    """
    # Use a 'with' statement to ensure the gRPC channel is properly closed.
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        # 1. Prepare the InitF9t request from the configuration file.
        # This example uses the first chip defined in the f9t_chips list.
        chip_config = default_f9t_cfg['f9t_chips'][^2_0]
        
        # Create a complete config for this specific chip
        f9t_config_for_rpc = copy.deepcopy(default_f9t_cfg)
        del f9t_config_for_rpc['f9t_chips']
        f9t_config_for_rpc.update(chip_config)

        init_request = ublox_control_pb2.InitF9tRequest(
            f9t_config=ParseDict(f9t_config_for_rpc, Struct()),
            force_init=True  # Force re-initialization
        )

        # 2. Call the InitF9t RPC to configure the chip.
        try:
            init_response = await stub.InitF9t(init_request)
            print(f"InitF9t successful: {init_response.message}")
        except grpc.aio.AioRpcError as e:
            print(f"Error initializing F9T: {e.details()}")
            return

        # 3. Create a request to capture all UBX data.
        capture_request = ublox_control_pb2.CaptureUbloxRequest(
            patterns=[".*"]  # Use regex to match all message types
        )

        # 4. Listen to the data stream from the CaptureUblox RPC.
        print("Starting UBX data stream...")
        try:
            # The async for loop will process messages as they arrive.
            async for response in stub.CaptureUblox(capture_request):
                # The response object contains the raw payload and parsed data.
                parsed_dict = MessageToDict(response.parsed_data)
                print(f"Received message: {response.name}, "
                      f"Timestamp: {response.pkt_unix_timestamp.ToJsonString()}")
                # print(f"Parsed data: {parsed_dict}")

        except grpc.aio.AioRpcError as e:
            print(f"Data stream failed: {e.details()}")
        except KeyboardInterrupt:
            print("Stream stopped by user.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Client shut down.")

```
