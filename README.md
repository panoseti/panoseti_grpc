![PANOSETI gRPC CI](https://github.com/panoseti/panoseti_grpc/actions/workflows/ci.yml/badge.svg)
![PyPI Version](https://img.shields.io/pypi/v/panoseti-grpc)
# PANOSETI gRPC Services

This repository contains the microservice architecture for the PANOSETI observatory. It provides gRPC interfaces for real-time data access, observatory control, and general telemetry logging.
See [here](https://github.com/panoseti/panoseti) for the main software repo.

## Service Directory

Each service operates independently. Click the links below for detailed API documentation and configuration guides.

| Service | Description                                            | Status        | Documentation |
| :--- |:-------------------------------------------------------|:--------------| :--- |
| **DAQ Data** | Streams real-time science data directly from Hashpipe. | 🟢 Production | [**Read Docs**](./src/panoseti_grpc/daq_data/README.md) |
| **DAQ Control** | Manages Hashpipe lifecycle on DAQ nodes (start/stop/status). | 🔶 In Development | [**Read Docs**](./src/panoseti_grpc/daq_control/README.md) |
| **U-blox Control** | Controls and configures GNSS chips (F9T/F9P).          | 🟢 Production | [**Read Docs**](./src/panoseti_grpc/ublox_control/README.md) |
| **Telemetry** | Collects metadata from remote Linux machines.          | 🟡 Beta       | [**Read Docs**](./src/panoseti_grpc/telemetry/README.md) |

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

# Upload metadata
client.log_flexible("example", "weather-01", {"status": "Online", "is-raining": True})
```

---

# 🛠️ Development & Contribution

## Environment Setup
If you are deploying the servers on the head node or contributing to the codebase, we recommend installing `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install)), then following these steps to setup your environment:
```bash
# 0. Clone this repo and go to the repo root
git clone https://github.com/panoseti/panoseti_grpc.git
cd panoseti_grpc

# 1. Create the grpc-py314 conda environment
conda create -n grpc-py314 python=3.14
conda activate grpc-py314

# 2. Install in editable mode with development dependencies
pip install -e ".[dev]"
```

## 🚦 Unified Server

All three active services (`daq_data`, `daq_control`, `telemetry`) can run on a **single port** under a unified process. gRPC routes RPCs by proto package name automatically — no collision between services.

### Quick Start

```bash
# All services on one port (default: 50051)
panoseti-server

# DAQ node: daq_data + daq_control only
panoseti-server --profile daq_node

# Head node: telemetry only
panoseti-server --profile headnode

# Custom config file
panoseti-server --config /etc/panoseti/server.toml

# List all registered services
panoseti-server --list-services
```

### Deployment Profiles

| Profile | Services | Machine |
|---------|----------|---------|
| `default` | telemetry + daq_data + daq_control | Single-machine dev / test |
| `daq_node` | daq_data + daq_control | Each DAQ compute node |
| `headnode` | telemetry | Observatory head node |

On DAQ nodes (`telemetry = false`), services configured with `grpc_logging = true` automatically forward logs to the head node's telemetry endpoint via the `HEADNODE_IP` / `HEADNODE_GRPC_PORT` environment variables.

### Config File Structure

Bundled profiles live in `src/panoseti_grpc/config/`. A custom `server.toml` follows this structure:

```toml
[server]
port = 50051
shutdown_grace_period = 5.0
log_dir = "/var/log/panoseti"
grpc_logging = true

[server.services]
telemetry   = true
daq_data    = true
daq_control = true

[telemetry]
redis_host = "localhost"
redis_port = 6379

[daq_data]
# ... DaqDataServerConfig fields ...

[daq_control]
log_dir = "/var/log/panoseti"
```

## 🧪 Testing

We use a comprehensive CI pipeline (GitHub Actions) to verify every commit. You can—and should—run these same tests locally before pushing code.

### Run CI Tests Locally via Bash Scripts (Recommended)

To run a CI test locally, use one of the scripts in `scripts/run-ci-tests/`.
Each service has an associated script which builds the Docker containers and runs the appropriate test suites.

#### Examples:
```bash
# Run DAQ Data Service tests
./scripts/run-ci-tests/run-daq-data-ci-test.sh

# Run U-blox Control Service tests
./scripts/run-ci-tests/run-ublox-control-ci-test.sh
```

---

## 🚀 Adding New Services

The PANOSETI gRPC architecture is designed to be extensible. New services slot into the unified server without modifying `PanosetiServer` itself — only `server.py` registration and config additions are needed.

### 0. Branching Strategy

Always create a new feature branch off the development branch:

```bash
git checkout dev
git checkout -b feature/daq-control-service

```

### 1. Define the Interface (.proto)

Create a new Protocol Buffer definition file in the `protos/` directory. This defines the contract between your client and server.

* **File:** `protos/daq_control.proto`
* **Example:**
```protobuf
syntax = "proto3";
package panoseti.daq_control;

service DaqControl {
  rpc SetHighVoltage (VoltageRequest) returns (StatusResponse) {}
}

message VoltageRequest { float voltage = 1; }
message StatusResponse { bool success = 1; }

```



### 2. Compile the Protos

Run the compilation script to generate the Python gRPC code.

```bash
python scripts/compile_protos.py

```

This will automatically generate two files in `src/panoseti_grpc/generated/`:

* `daq_control_pb2.py` (Message definitions)
* `daq_control_pb2_grpc.py` (Client/Server stubs)

### 3. Create the Service Module

Create a new directory for your service source code. You **must** include an `__init__.py` file for Python to recognize it as a package.

```bash
mkdir -p src/panoseti_grpc/daq_control
touch src/panoseti_grpc/daq_control/__init__.py

```

### 4. Implement Client & Server

Develop your application logic. You can now import your generated protobuf code using the package path.

**Example `src/panoseti_grpc/daq_control/server.py`:**

```python
import grpc
from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc

class DaqControlServicer(daq_control_pb2_grpc.DaqControlServicer):
    def SetHighVoltage(self, request, context):
        print(f"Setting voltage to {request.voltage}")
        return daq_control_pb2.StatusResponse(success=True)

```

### 5. Register with the Unified Server

To make the service available via `panoseti-server`, add it to `src/panoseti_grpc/server.py`:

1. Write `async def _make_<name>_servicer(cfg, shutdown_event)` that returns `(servicer, [post_start_coros])`
2. Add `<name>: NewServiceConfig = Field(default_factory=NewServiceConfig)` to `PanosetiServerConfig`
3. Add `<name>: bool = False` to `ServiceToggles`
4. Call `ServiceRegistry.register(ServiceDescriptor("<name>", ...))` at module level
5. Add a `[<name>]` section to the relevant `server*.toml` profile files

### 6. Add CI Tests

Finally, ensure your new service is robust by adding a test suite.

1. Create a test directory: `tests/<name>/`
2. Add a BuildKit stage to the root `Dockerfile.ci` for your test environment.
3. Add a runner script in `scripts/run-ci-tests/run-<name>-ci-test.sh`.
4. Create unit and integration tests with [pytest](https://docs.pytest.org/en/stable/).
