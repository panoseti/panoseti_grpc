![PANOSETI gRPC CI](https://github.com/panoseti/panoseti_grpc/actions/workflows/ci.yml/badge.svg)
![PyPI Version](https://img.shields.io/pypi/v/panoseti-grpc)
# PANOSETI gRPC Services

This repository contains the microservice architecture for the PANOSETI observatory. It provides gRPC interfaces for high-speed data acquisition, hardware control, and telemetry logging.
Contains gRPC code for the PANOSETI project. See [here](https://github.com/panoseti/panoseti) for the main software repo.

## Service Directory

Each service operates independently. Click the links below for detailed API documentation and configuration guides.

| Service | Description                                            | Status        | Documentation |
| :--- |:-------------------------------------------------------|:--------------| :--- |
| **DAQ Data** | Streams real-time science data directly from Hashpipe. | 🟢 Production | [**Read Docs**](./src/panoseti_grpc/daq_data/README.md) |
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
client.log_flexible("test_device", "01", {"status": "Online"})
```

---

# 🛠️ Development & Contribution

## Environment Setup
If you are deploying the servers on the head node or contributing to the codebase, we recommend installing `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install)), then following these steps to setup your environment:
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

The PANOSETI gRPC architecture is designed to be extensible. If you are developing a new service (e.g., the upcoming `daq_control`), follow this standard workflow.

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

### 5. Add CI Tests

Finally, ensure your new service is robust by adding a test suite.

1. Create a test directory: `tests/daq_control/`
2. Add a `Dockerfile` for your test environment.
3. Add a generic runner script in `scripts/run-ci-tests/run-daq-control-ci-test.sh`.
4. Create unit and integration tests with [pytest](https://docs.pytest.org/en/stable/).
