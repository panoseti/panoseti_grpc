# GEMINI.md - PANOSETI gRPC Services Context

This file is the foundational mandate for Gemini CLI interactions within the `panoseti_grpc` repository. It covers architecture, development workflows, and engineering standards.

## Project Overview

PANOSETI gRPC Services is a microservice control and data-acquisition layer for the PANOSETI observatory. Built on **Python 3.14+**, **asyncio**, and **gRPC**.

### Active Services

| Service | Status | Purpose |
|---------|--------|---------|
| `daq_data` | Production | Streams real-time science images from Hashpipe via UDS |
| `daq_control` | Production | Hashpipe lifecycle, manifest generation, selective cleanup |
| `telemetry` | Beta | Device status → Redis/InfluxDB; log shipping → Grafana Alloy → Loki |
| `ublox_control` | 🔴 Deprecated | Disabled by default; removed in next major release |

### Architecture

**Unified server**: All active services co-host on a single gRPC port. gRPC routes RPCs by proto package name automatically — no collision.

**DAQ Data gateway/edge topology**:
```
Consumer → AioDaqDataClient(headnode, port)
                │
         DaqDataGatewayServicer  (headnode)
           ├── AioDaqDataClient(daq-node-1, port)
           └── AioDaqDataClient(daq-node-N, port)
                       │   UDS
                DaqDataServicer (each DAQ node) ← Hashpipe
```
Consumers always connect to the **headnode gateway** — a single endpoint. The gateway fans in from all edge nodes. M×N connection scaling is eliminated.

**Clients**: Single-target `AioDaqDataClient(host, port)` / `DaqDataClient(host, port)`. Same shape as `AsyncDaqControlClient(host, port)`.

**Shared gRPC machinery (`grpc_utils`)**:
- `@grpc_call` — wraps async/sync/generator methods; maps `grpc.RpcError → PanosetiRpcError`; never suppresses `CancelledError`
- `AsyncChannelManager` — owns channel lifecycle with keepalive options
- `HealthClient` — wraps `grpc.health.v1`; replaces `daq_data.Ping`
- `build_retry_service_config()` — declarative retry policy
- Typed exceptions: `UnavailableError`, `DeadlineExceededError`, `FailedPreconditionError`, …

**Health checks**: `PanosetiServer.run()` auto-registers `grpc.health.v1.HealthServicer`. Use `HealthClient.check("daqdata.DaqData")` or `grpc_health_probe` instead of the deprecated `Ping` RPC.

**Per-host logging**: `get_logger(service, log_dir=...)` writes `.log` and `.jsonl` under `{log_dir}/{hostname}/`. Alloy globs `{log_dir}/*/*.jsonl` and ships to Loki, labeling by hostname. Four simultaneous output paths: console (Rich), `.log` (plain text), `.jsonl` (Alloy → Loki), gRPC `Log` RPC (shadow period).

**Deployment profiles**:

| Profile | Services | Machine |
|---------|----------|---------|
| `default` | telemetry + daq_data + daq_control | Single-machine dev/test |
| `daq_node` | daq_data + daq_control | Each DAQ compute node |
| `headnode` | telemetry | Observatory head node |

**`pseti-grpc daqnode`**: CLI command reporting gRPC service health (`grpc.health.v1`), Grafana Alloy liveness, and log-disk usage. Use for liveness probes in place of the old `Ping` RPC.

---

## Development & Operations

### Building & Code Generation

Always recompile protos after editing `.proto` files:
```bash
python scripts/compile_protos.py
```
Generates `_pb2.py` and `.pyi` type stubs in `src/panoseti_grpc/generated/`. **Never edit generated files.**

### Testing & Quality Control

```bash
# Run all linters and test suites
pseti test grpc all
python tests/qa.py all

# Specific test suites
pseti test grpc lint          # Ruff + MyPy
pseti test grpc daq-control   # DAQ Control tests
pseti test grpc daq-data      # DAQ Data tests
pseti test grpc telemetry     # Telemetry tests

# Run locally without the pseti CLI
python tests/qa.py daq_data
pytest tests/unified_server/unit/ -v
pytest tests/unified_server/integration/ -v --timeout=90
```

Configuration is in `tests/qa.toml`. Each test suite MUST use a unique Docker Compose project name (`-p pseti-grpc-NAME`) to prevent collisions.

---

## Engineering Standards

### Coding Style

- **Ruff**: Primary linter and formatter. Line length 120.
- **MyPy**: Strict type checking required.
- **Pydantic**: Use for all config schemas and request validation. Prefer attribute access over dict indexing.

### gRPC & Protobuf

- Generated code in `src/panoseti_grpc/generated/`. **Never edit manually.**
- Async clients MUST implement `__aenter__` / `__aexit__` for channel lifecycle.
- All client RPC methods MUST be decorated with `@grpc_call` from `grpc_utils.decorators`.
- Error handling: catch `PanosetiRpcError` subclasses, never raw `grpc.RpcError`.

### Testing Conventions

- **Isolation**: Integration tests MUST NOT bind to host ports. Use bridge networks.
- **Project scope**: Each test suite MUST use a unique Docker Compose project name.
- **Async**: Use `pytest-asyncio` with `asyncio_mode = "auto"`.
- **Redis polling**: Tests that assert Redis state after RPCs MUST poll with a timeout (not `time.sleep`) because `RedisBatcher` introduces flush latency.

### Error Handling

All RPC handlers MUST be decorated with `@grpc_error_handler`:
```python
from panoseti_grpc.util.error_handling import grpc_error_handler

class MyServicer(MyServiceServicer):
    @grpc_error_handler
    async def MyRPC(self, request, context):
        ...
```

Clients use `@grpc_call` for typed exception mapping:
```python
from panoseti_grpc.grpc_utils import FailedPreconditionError
try:
    await client.CleanupData(params)
except FailedPreconditionError as e:
    logger.error("Manifest digest mismatch: %s", e.details)
```

---

## Key Directory Map

```
protos/                          Protocol Buffer definitions
src/panoseti_grpc/
    generated/                   Generated gRPC/Protobuf code + .pyi stubs
    config/                      Bundled deployment profiles (TOML)
    daq_data/                    DaqData service (server, client, aggregator, simulate)
    daq_control/                 DaqControl service (server, client, manifest)
    telemetry/                   Telemetry service (server, client, logger)
    grpc_utils/                  Shared: decorators, exceptions, health, retries, channel
    util/                        Cross-service utilities (error_handling, resources)
    _cli/                        pseti-grpc CLI (root, daqnode, daq_data, telemetry)
tests/                           Unified QA runner + service-specific test suites
scripts/                         Maintenance scripts (compilation, CI helpers)
alloy/                           Grafana Alloy config + docker-compose
Dockerfile.ci                    Multi-stage BuildKit for CI and local QA
```
