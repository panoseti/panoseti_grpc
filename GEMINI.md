# GEMINI.md - PANOSETI gRPC Services Context

This file serves as a foundational mandate for Gemini CLI interactions within the `panoseti_grpc` repository. It outlines the project's architecture, development workflows, and engineering standards.

## 🌌 Project Overview
PANOSETI gRPC Services is a microservice-based control and data acquisition layer for the PANOSETI observatory. It leverages **Python 3.14+**, **asyncio**, and **gRPC** to provide high-performance, real-time access to observatory hardware and data streams.

### Core Services
- **`daq_data`**: Streams real-time science images from the Hashpipe C++ pipeline via Unix Domain Sockets (UDS).
- **`daq_control`**: Manages the lifecycle of the Hashpipe process (start/stop/status) on DAQ nodes.
- **`telemetry`**: A hybrid logging and metadata collection service using Redis (hot storage) and InfluxDB (cold storage).
- **`ublox_control`**: Configures and streams data from ZED-F9T/F9P GNSS chips via serial/UBX.

### Architecture
The project uses a **Unified Server** model (`panoseti_grpc.server.py`) where multiple services are co-hosted on a single gRPC port. Services are registered via a `ServiceRegistry` and configured using Pydantic models.

---

## 🛠️ Development & Operations

### Building & Code Generation
**Crucial:** Always recompile protos after editing `.proto` files to ensure type stubs and generated code are in sync.
```bash
python scripts/compile_protos.py
```
This script generates both Python source (`_pb2.py`) and MyPy type stubs (`.pyi`) using `mypy-protobuf`.

### Testing & Quality Control
We use a unified QA runner located in the `tests/` directory.
```bash
# Run all linters and test suites
python tests/qa.py all

# Run specific tasks
python tests/qa.py lint               # Ruff (lint/format) + MyPy
python tests/qa.py daq_data           # Individual test suite
```
Configuration is driven by `tests/qa.toml`.

### Running the Server
```bash
panoseti-server                       # All services enabled
panoseti-server --profile daq_node    # DAQ-specific subset
panoseti-server --profile headnode    # Telemetry-only subset
```

---

## 📏 Engineering Standards

### Coding Style & Linting
- **Ruff**: Primary linter and formatter.
- **MyPy**: Strict type checking is required.
- **Line Length**: 120 characters.
- **Style**: Follow PEP-8, but prefer the modern, concise patterns found in the unified server implementation. Use Pydantic for configuration schemas.

### gRPC & Protobuf
- **Generated Code**: Located in `src/panoseti_grpc/generated/`. **NEVER** edit these files manually.
- **Type Safety**: Always generate `.pyi` stubs. MyPy is configured to ignore the `.py` generated files but use the `.pyi` stubs for type safety.
- **Relative Imports**: The compilation script automatically patches generated code to use relative imports (`from . import ...`).

### Testing Conventions
- **Integration Tests**: Typically require Docker Compose (defined in `tests/<service>/docker-compose.test.yml`).
- **Isolation**: Each test should aim for environment isolation (e.g., using `TemporaryDirectory` for socket paths).
- **Async**: Use `pytest-asyncio` with `asyncio_mode = "auto"`.

---

## 📁 Key Directory Map
- `protos/`: Protocol Buffer definitions.
- `src/panoseti_grpc/`: Core package source code.
    - `generated/`: Generated gRPC/Protobuf code and type stubs.
    - `config/`: Bundled deployment profiles (TOML).
- `tests/`: Unified QA runner and service-specific test suites.
- `scripts/`: Maintenance scripts (compilation, CI helpers).
- `Dockerfile.ci`: Multi-stage BuildKit file for CI and local QA.
