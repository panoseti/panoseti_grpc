![PANOSETI gRPC CI](https://github.com/panoseti/panoseti_grpc/actions/workflows/ci.yml/badge.svg)
![PyPI Version](https://img.shields.io/pypi/v/panoseti-grpc)
# PANOSETI gRPC Services

This repository contains the microservice architecture for the PANOSETI observatory. It provides gRPC interfaces for high-speed data acquisition, hardware control, and telemetry logging.
Contains gRPC code for the PANOSETI project. See [here](https://github.com/panoseti/panoseti) for the main software repo.

## Service Directory

Each service operates independently. Click the links below for detailed API documentation and configuration guides.

| Service | Description | Status        | Documentation |
| :--- | :--- |:--------------| :--- |
| **DAQ Data** | High-throughput image streaming and Hashpipe acquisition control. | 🟢 Production | [**Read Docs**](./src/panoseti_grpc/daq_data/README.md) |
| **U-blox Control** | GNSS receiver configuration (F9T/F9P) and raw UBX data streaming. | 🟢 Production | [**Read Docs**](./src/panoseti_grpc/ublox_control/README.md) |
| **Telemetry** | Centralized sensor logging, health monitoring, and InfluxDB archiving. | 🟡 Beta       | [**Read Docs**](./src/panoseti_grpc/telemetry/README.md) |

---

## 📦 Installation (Client Mode)

If you only need to write scripts to control the observatory or analyze data, install the package from PyPI:

```bash
pip install panoseti-grpc
```

Example Usage:

```python
from panoseti_grpc.telemetry.client import TelemetryClient

# Connect to a running Telemetry Service
client = TelemetryClient("localhost", 50051)
client.log_flexible("test_device", "01", {"status": "Online"})
```

## 🛠️ Development Environment Setup for gRPC Clients and Servers
Install `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install)), then follow these steps:
```bash
# 0. Clone this repo and go to the repo root 
git clone https://github.com/panoseti/panoseti_grpc.git
cd panoseti_grpc

# 1. Create the grpc-py39 conda environment
conda create -n grpc-py39 python=3.9
conda activate grpc-py39

# 2. Install in editable mode with development dependencies
pip install -e .[dev]
```




