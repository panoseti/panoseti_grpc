# GEMINI.md - PANOSETI gRPC Services Context

This file serves as a foundational mandate for Gemini CLI interactions within the `panoseti_grpc` repository. It outlines the project's architecture, development workflows, and engineering standards.

## 🌌 Project Overview
PANOSETI gRPC Services is a microservice-based control and data acquisition layer for the PANOSETI observatory. It leverages **Python 3.14+**, **asyncio**, and **gRPC** to provide high-performance, real-time access to observatory hardware and data streams.

### Architecture
- **Unified Server**: Multiple services co-hosted on a single gRPC port (`panoseti_grpc.server.py`).
- **Async-First Clients**: Native `AsyncDaqControlClient` and `AsyncDaqDataClient` using `grpc.aio` for non-blocking coordination.
- **Model-Driven Requests**: Dedicated `client_models.py` for each service provide client-side parameter validation decoupled from server-side filesystem checks.

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
pseti test grpc all

# Run specific tasks
pseti test grpc lint               # Ruff (lint/format) + MyPy
pseti test grpc daq-control        # Individual test suite
```
Configuration is driven by `tests/qa.toml`. Unique project names (`-p pseti-grpc-NAME`) MUST be used for Docker isolation.

---

## 📏 Engineering Standards

### Coding Style & Linting
- **Ruff**: Primary linter and formatter.
- **MyPy**: Strict type checking is required.
- **Pydantic**: Use for all configuration schemas and request validation. Prefer attribute access over dictionary indexing.

### gRPC & Protobuf
- **Generated Code**: Located in `src/panoseti_grpc/generated/`. **NEVER** edit these files manually.
- **Type Safety**: Always generate `.pyi` stubs.
- **Async Clients**: MUST implement `__aenter__` and `__aexit__` for channel lifecycle management.

### Testing Conventions
- **Isolation**: Integration tests MUST NOT bind to host ports. Use bridge networks for inter-container communication.
- **Project Scope**: Each test suite MUST use a unique Docker Compose project name to prevent network/container collisions during concurrent runs.
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
