![PANOSETI gRPC CI](https://github.com/panoseti/panoseti_grpc/actions/workflows/ci.yml/badge.svg)
![PyPI Version](https://img.shields.io/pypi/v/panoseti-grpc)
# PANOSETI gRPC Services

This repository contains the microservice architecture for the PANOSETI observatory. It provides gRPC interfaces for real-time data access, observatory control, and general telemetry logging.
See [here](https://github.com/panoseti/panoseti) for the main software repo.

## Service Directory

Each service operates independently. Click the links below for detailed API documentation and configuration guides.

| Service | Description                                            | Status        | Documentation |
| :--- |:-------------------------------------------------------|:--------------| :--- |
| **DAQ Data** | Streams real-time science data from Hashpipe via headnode gateway. | 🟢 Production | [**Read Docs**](./docs/daq_data_service.md) |
| **DAQ Control** | Manages Hashpipe lifecycle on DAQ nodes (start/stop/status). | 🟢 Production | [**Read Docs**](./docs/daq_control_service.md) |
| **Telemetry** | Device status → Redis/InfluxDB; log shipping → Grafana Alloy → Loki. | 🟡 Beta | [**Read Docs**](./docs/telemetry_service.md) |
| **ML Inference** | Real-time cloud-detection scores from the streaming pipeline; pub-sub for predictions and alerts. | 🟡 Beta | [**Read Docs**](./docs/ml_inference_service.md) |
| **U-blox Control** | Controls and configures GNSS chips (F9T/F9P).          | 🔴 Deprecated | [**Read Docs**](./src/panoseti_grpc/ublox_control/README.md) |

---

## 📜 Changelog
Keep track of the latest changes, modernization efforts, and breaking changes in our [**Changelog**](./docs/CHANGELOG.md).

---

## 🖥️ Installation (Server Mode)

If you're deploying the unified gRPC server (`pseti-grpc server`) on the head node or a DAQ node, install it as a standalone CLI tool with `uv` — this puts `pseti-grpc` on your `PATH`, isolated in its own environment, without needing a full dev setup:

```bash
uv tool install panoseti-grpc
```

See the "🚦 Unified Server" section below for how to configure and run it.

---

## 📦 Installation (Client Mode)

If you only need to write scripts to control the observatory or analyze data, install the package from PyPI:

```bash
pip install panoseti-grpc
```

Example Usage:

```python
# Stream real-time science images from the headnode gateway
import asyncio
from panoseti_grpc.daq_data.client import AioDaqDataClient

async def main():
    async with AioDaqDataClient(host="headnode", port=50051) as client:
        async for image in client.stream_images(stream_movie_data=True):
            print(f"Module {image['module_id']}  {image['type']}")

asyncio.run(main())
```

```python
# Upload device telemetry
from panoseti_grpc.telemetry.client import TelemetryClient
client = TelemetryClient("headnode", 50051)
client.log_flexible("DEV_weather", "station-01", {"status": "Online", "is-raining": True})
```

---

# 🛠️ Development & Contribution

## Environment Setup
If you are deploying the servers on the head node or contributing to the codebase, we recommend installing `miniconda` ([link](https://www.anaconda.com/docs/getting-started/miniconda/install)), then following these steps to setup your environment:
```bash
# 0. Clone this repo and go to the repo root
git clone https://github.com/panoseti/panoseti_grpc.git
cd panoseti_grpc

# Option 1. Install with pip
conda create -n grpc-py314 python=3.14
conda activate grpc-py314
pip install -e ".[dev]"

# Option 2. Install with uv
uv tool install -e .

```

## 🚦 Unified Server

All three active services (`daq_data`, `daq_control`, `telemetry`) can run on a **single port** under a unified process. gRPC routes RPCs by proto package name automatically — no collision between services.

### Quick Start

```bash
# See the options
pseti-grpc -h

# Copy the packaged server config templates (server*.toml) and a .env
# template to the current directory, to customize
pseti-grpc --config-template
pseti-grpc --env-template

# Simplest single-node setup: set PSETI_GRPC_PORT (and PSETI_GRPC_HOST for
# clients not on the same host) once -- in a .env file or the environment --
# and `pseti-grpc server` plus every client command (stat, reflect,
# telemetry, daq-data, daq-control, ...) agree on the same host:port with
# no flags needed.
pseti-grpc server --profile default

# DAQ node: daq_data (edge role) + daq_control
pseti-grpc server --profile daq_node

# Head node: telemetry + daq_data (gateway role, fans in every edge node)
pseti-grpc server --profile headnode

# Custom config file
pseti-grpc server --config /etc/panoseti/server.toml

# List all registered services
pseti-grpc server --list-services

# Check gRPC service health, Alloy liveness, and log disk usage
pseti-grpc daqnode --log-dir /var/log/panoseti
```

### Deployment Profiles

| Profile | Services | Machine |
|---------|----------|---------|
| `default` | telemetry + daq_data + daq_control | Single-machine dev / test |
| `daq_node` | daq_data (edge) + daq_control | Each DAQ compute node |
| `headnode` | telemetry + daq_data (gateway) | Observatory head node |
| `gateway` | telemetry + daq_data (gateway) | Same shape as `headnode`; kept separate for sites that want a telemetry-only vs. gateway split later |

None of these profiles hardcode `[server].port` — it resolves at startup from (highest priority first) `--port` (hidden, deployment/debug-only), the env var named by `--port-env` (also hidden — role-scoped fleet deployments, e.g. `HEADNODE_GRPC_PORT`/`DAQNODE_GRPC_PORT`), `PSETI_GRPC_PORT` (the simple single-node knob — read by both `pseti-grpc server`'s own default and every `pseti-grpc` client command's `--port` default), the legacy `GRPC_PORT` env var, then the built-in 50051 default (`PanosetiServerConfig.port` / `unified_main.resolve_bind_port()`). An explicit `port =` line in a custom `--config` TOML always wins over every env var, so don't add one if you want the deployment's `.env` to control it — this is exactly how a hardcoded `port = 50052` in an earlier version of the `headnode` profile silently desynced from every client still assuming 50051.

`PSETI_GRPC_HOST` is the client-side counterpart to `PSETI_GRPC_PORT`: the default `--host` for every `pseti-grpc` command. Both are unrelated to `HEADNODE_IP`/`HEADNODE_GRPC_PORT` (below) — see `.env_example` (or `pseti-grpc --env-template`) for the full rundown.

On DAQ nodes (`telemetry = false`), services configured with `grpc_logging = true` automatically forward logs to the head node's telemetry endpoint via the `HEADNODE_IP` / `HEADNODE_GRPC_PORT` environment variables.

### Environment Configuration

`pseti-grpc` (and `python -m panoseti_grpc`) auto-loads a `.env` file from the current working directory at startup (or a specific file via `PSETI_GRPC_ENV_FILE`) — plain `KEY=value` lines, no `export` needed. Run `pseti-grpc --env-template` to copy the packaged `.env_example` to `./.env_grpc_<timestamp>` as a starting point, and `pseti-grpc --config-template` for the bundled `server*.toml` profiles.

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

### Unified QA Runner (Recommended)

The most efficient way to run quality checks and tests is via the unified QA runner:

```bash
# Run all linters and test suites
python tests/qa.py all

# Run specific tasks
python tests/qa.py lint
python tests/qa.py telemetry
```

### Run CI Tests Locally via Bash Scripts

Alternatively, you can use the individual scripts in `scripts/run-ci-tests/`.
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

To make the service available via `pseti-grpc server`, add it to `src/panoseti_grpc/server.py`:

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
