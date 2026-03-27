# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PANOSETI gRPC Services — a microservice architecture for the PANOSETI observatory providing real-time data access, observatory control, and telemetry via gRPC interfaces. Python 3.9+, built on asyncio and Protocol Buffers.

## Development Setup

```bash
conda create -n grpc-py39 python=3.9
conda activate grpc-py39
pip install -e ".[dev]"
```

## Common Commands

### Build / Code Generation
```bash
# Recompile all .proto files → generates src/panoseti_grpc/generated/*_pb2.py and *_pb2_grpc.py
python scripts/compile_protos.py
```

### Testing
Each service has a CI shell script that sets up Docker dependencies and runs pytest:
```bash
./scripts/run-ci-tests/run-daq-data-ci-test.sh
./scripts/run-ci-tests/run-daq-control-test.sh
./scripts/run-ci-tests/run-telemetry-ci-test.sh
./scripts/run-ci-tests/run-ublox-ci-test.sh
./scripts/run-ci-tests/run-hashpipe-daq-data-ci.sh   # requires real/simulated Hashpipe hardware
```

Run a single test file directly:
```bash
pytest tests/daq_data/test_server.py
pytest tests/telemetry/ -k "test_log_flexible"
```

### Lint / Format
```bash
black src/ tests/ scripts/    # line-length 120
flake8 src/ tests/ scripts/   # max-line-length 120, ignores E203, W503
```

### Run a service locally
```bash
python -m panoseti_grpc.daq_data.server
python -m panoseti_grpc.daq_control.server
python -m panoseti_grpc.telemetry.server
python -m panoseti_grpc.ublox_control.server
```

## Architecture

### Services

| Service | Status | Purpose |
|---------|--------|---------|
| `daq_data` | Production | Streams real-time science images from Hashpipe shared memory |
| `ublox_control` | Production | Configures and streams data from ZED-F9T/F9P GNSS chips |
| `telemetry` | Beta | Collects metadata/health; hybrid Redis (hot) + InfluxDB (cold) + Loki (logs) |
| `daq_control` | In Development | High-voltage control and DAQ system configuration |

Each service lives under `src/panoseti_grpc/<service>/` and follows the pattern:
- `server.py` — gRPC servicer implementation
- `client.py` — Python client (sync + async variants)
- `config.py` — Pydantic-validated configuration and schema

### Proto → Generated Code Flow
`.proto` files in `protos/` → `python scripts/compile_protos.py` → `src/panoseti_grpc/generated/`. Never edit generated files directly.

### DAQ Data Service
Bridges Hashpipe shared-memory ring buffers (C/C++ hardware pipeline) to gRPC streams. Key abstractions:
- `data_sources.py` — abstracts UDS, RPC, and simulated Hashpipe data sources
- `hp_io_manager.py` — manages Hashpipe I/O threads
- `simulate.py` — synthetic data generator used in tests

### Telemetry Service
Two storage tiers controlled by `mode` field:
- **Production mode:** strict Pydantic schema, permanent Redis + InfluxDB storage
- **Experimental mode:** flexible JSON, 24 h TTL in Redis only

`logger.py` provides an async gRPC logging handler that injects Git commit, hostname, and PID metadata automatically.

### U-blox Control Service
Communicates via serial port using the UBX binary protocol (`pyubx2`). Configuration is JSON5-based (`f9t_config.json`) with position, constellation, and timepulse settings.

### Shared Utilities
`src/panoseti_grpc/panoseti_util/` — PFF file format, config-file parsing, DAQ shutdown helpers. Used across services.

## Testing Infrastructure
- Tests use `pytest-asyncio` for async test support.
- Integration tests spin up Docker Compose stacks (Redis, InfluxDB, InfluxDB) defined in each service's test directory.
- `conftest.py` files in each test directory provide server fixtures and client factories.
- DAQ Control tests build a custom C++ Hashpipe binary inside Docker (see `tests/daq_control/Dockerfile`).
