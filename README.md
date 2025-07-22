# panoseti_grpc
Contains gRPC code for the PANOSETI project. See [here](https://github.com/panoseti/panoseti) for the main software repo.

## Environment Setup for gRPC Clients and Servers

Follow steps below to prepare your environment:

1. Install `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install))
2. Clone this (`panoseti_grpc`) repo onto a [data acquisition node](https://github.com/panoseti/panoseti/wiki/Nodes-and-modules#daq-nodes) (DAQ node).
3. Run the following commands to create the `grpc-py39` environment. 
```bash
git clone https://github.com/panoseti/panoseti_grpc.git
cd panoseti_grpc
conda create -n grpc-py39 python=3.9
conda activate grpc-py39
conda install -c conda-forge grpcio-tools
pip install -r requirements
```

# DaqData Service
![DaqData_StreamImages.png](docs/DaqData_StreamImages_overview.png)

## Core RPCs
### `StreamImages`

![DaqData_StreamImages_hp-io.png](docs/DaqData_StreamImages_hp-io.png)
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


## Working with the gRPC DaqData API

### Using the Demo Client Script

```
demo_daq_data_client.py  - demonstrates real-time pulse-height and movie-mode visualizations using the gRPC DaqData API.

usage: demo_daq_data_client.py [-h] [--host HOST] [--init CFG_PATH] [--init-sim] [--plot-view] [--plot-phdist] [--module-ids [MODULE_IDS ...]]

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           DaqData server hostname or IP address. Default: 'localhost'
  --init CFG_PATH       initialize the hp_io thread from the file [CFG] in config/ to track an in-progress run directory
  --init-sim            initialize the hp_io thread to track a simulated run directory
  --plot-view           whether to create a live data previewer
  --plot-phdist         whether to create a live pulse-height distribution for the specified module id
  --module-ids [MODULE_IDS ...]
                        whitelist for the module ids to stream data from. If empty, data from all available modules is returned.
```

Below is an example workflow for viewing real-time data products during an observing run.
Note that because panoseti_grpc has a package structure, for commands 4+ your working directory should be the repo root, `panoseti_grpc/`, and each command should be prefixed with **`python -m daq_data.`** This is why the full command for step 4 is **`python -m daq_data.daq_data_server.py`**, instead of something like `./daq_data_server.py`

1. Set up your environment as described above.
2. Update your `hp_io_config.json` file or create a new one (see docs below).
3. Run `start.py` in the `panoseti/control` directory to start Hashpipe on the DAQ node.
4. Run `python -m daq_data.daq_data_server.py` on the DAQ node, with hostname `H`.
5. Run `python -m daq_data.demo_daq_data_client.py -h` to see the available options.
6. Run `python -m daq_data.demo_daq_data_client.py --host H` on any computer to verify its network connection to the DAQ node.
7. Run `python -m daq_data.demo_daq_data_client.py --host H --init daq_data/config/hp_io_config_example.json` to initialize the server with an `InitHpIo` request based on the configuration given in `hp_io_config_example.json`.
8. Run `python -m daq_data.demo_daq_data_client.py --host H --plot-phdist` to make a `StreamImages` request and open a real-time pulse-height distribution visualization app.


### The hp_io_config.json file

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

## Developing Real-Time Visualizations with the gRPC DaqData API

[todo: expand]

1. Define a visualization class.
2. Implement a method that updates a visualization given a new panoseti image.
3. Follow the following code pattern:
![viz_api_code_demo.png](docs/viz_api_code_demo.png)

# UbloxControl Service (TODO)
...
