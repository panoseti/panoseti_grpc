# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PANOSETI gRPC Services — a microservice architecture for the PANOSETI observatory providing real-time data access, observatory control, and telemetry via gRPC interfaces. Python 3.14+, built on asyncio and Protocol Buffers.

## Development Setup

```bash
conda create -n grpc-py314 python=3.14
conda activate grpc-py314
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

Unified server (recommended):
```bash
panoseti-server                                   # all services (default config)
panoseti-server --profile daq_node                # daq_data + daq_control
panoseti-server --profile headnode                # telemetry only
panoseti-server --config /path/to/server.toml    # custom config file
panoseti-server --list-services                   # print registered services and exit
python -m panoseti_grpc                           # equivalent to panoseti-server
```

Individual service entry points (standalone):
```bash
panoseti-daq-data
panoseti-daq-control
panoseti-telemetry
# or via python -m:
python -m panoseti_grpc.daq_data.server
python -m panoseti_grpc.daq_control.server
python -m panoseti_grpc.telemetry.server
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
Bridges Hashpipe (C/C++ hardware pipeline) to gRPC streams via Unix Domain Sockets (UDS) — the sole supported data path. Key abstractions:
- `data_sources.py` — `UdsDataSource`: acts as a UDS server; one instance per data product (`img8`, `img16`, `ph256`, `ph1024`). Hashpipe connects as a UDS client and sends `[2-byte big-endian module_id][PFF frame]` tuples.
- `hp_io_manager.py` — `HpIoManager`: drains the central `asyncio.Queue(maxsize=500)`, assigns monotonic `frame_id`s, discovers module IDs dynamically from the stream, and writes to `latest_data_cache[module_id]['ph'|'movie']`.
- `simulate.py` — `UdsStrategy`: the test simulation connects to the server's UDS sockets as a Hashpipe stand-in and replays archived PFF frames. `SimulationManager` must be started **after** `HpIoManager` (sockets must exist first).
- `managers.py` — `HpIoTaskManager` owns the task lifecycle; `ClientManager` manages reader slots and the writer lock. The writer lock is acquired by `InitHpIo` and cancels all active `StreamImages` readers before reconfiguring.
- `state.py` — `ReaderState` tracks per-client cursor (`last_sent_movie_id`, `last_sent_ph_id`); `StreamImages` polls `latest_data_cache` and only sends frames with a higher `frame_id` than what the client last received.

**Pub/sub model:** `latest_data_cache` is a shared dict (not per-reader queues). Each `StreamImages` reader polls at its `update_interval_seconds` and sends any frame whose `frame_id > last_sent_id`. Fast producers overwrite slow producers — no frame queuing per reader.

**Tests:** All daq_data integration tests create isolated servers in `tempfile.TemporaryDirectory()` with unique socket paths. The `server_config_base` fixture loads `tests/daq_data/config/daq_data_server_config.json`; test helpers call `_make_server_config(server_config_base, socket_dir)` to override paths.

### Telemetry Service
Two storage tiers controlled by device type:
- **Production devices** (registered in schema): strict Pydantic schema, permanent Redis hash + InfluxDB timeseries. Redis TTL = -1 (no expiry). Key format: `{DEVICE_TYPE}_{device_id}`.
- **Experimental / DEV_ devices**: flexible JSON payload, Redis TTL ≤ 3600 s.
- **Unknown device types**: routed to `SANDBOX:{type}:{device_id}` namespace with positive TTL.

`RedisBatcher` in `server.py` batches up to 100 log RPCs before writing to the `logs:ingress` Redis list. Integration tests that check Redis must account for this flush delay — use polling helpers rather than `time.sleep` fixed waits. The `grpc_client.send_log_future()` method returns a future; call `.result()` on all futures before asserting Redis state.

`logger.py` provides an async gRPC logging handler that injects Git commit, hostname, and PID metadata automatically.

### U-blox Control Service
Communicates via serial port using the UBX binary protocol (`pyubx2`). Configuration is JSON5-based (`f9t_config.json`) with position, constellation, and timepulse settings.

### Unified Server
`src/panoseti_grpc/server.py` hosts all three services on a single `grpc.aio.Server` instance (one port). gRPC routes RPCs by proto package name automatically, so there is no collision.

**Deployment profiles** (`src/panoseti_grpc/config/`):

| Profile | Services | Machine |
|---------|----------|---------|
| `default` (`server.toml`) | telemetry + daq_data + daq_control | Single-machine dev/test |
| `daq_node` (`server_daq_node.toml`) | daq_data + daq_control | Each DAQ compute node |
| `headnode` (`server_headnode.toml`) | telemetry | Observatory head node |

**Initialization order** (`INIT_ORDER = ["telemetry", "daq_data", "daq_control"]`): telemetry servicer is registered and the port is live before other servicers are created, so their `get_logger(..., grpc_enabled=True)` calls can connect to the telemetry endpoint immediately. On a DAQ node (telemetry=false), `grpc_logging=true` means logs go to `HEADNODE_IP:HEADNODE_GRPC_PORT` via `AsyncGrpcHandler`'s existing remote connection — no code change needed.

**Adding a new service (5-step checklist):**
1. Implement servicer and proto; run `python scripts/compile_protos.py`
2. Write `async def _make_<name>_servicer(cfg, shutdown_event) -> (servicer, [post_start_coros])` in `server.py`
3. Add `<name>: NewServiceConfig = Field(default_factory=NewServiceConfig)` to `PanosetiServerConfig`
4. Add `<name>: bool = False` field to `ServiceToggles`
5. Call `ServiceRegistry.register(ServiceDescriptor("<name>", ...))` at module level in `server.py`

No changes to `PanosetiServer` itself are needed.

### Shared Utilities
- `src/panoseti_grpc/util/` — cross-service utilities:
  - `resources.py`: `load_package_resource()` / `load_package_json()` — importlib.resources-based file loader
  - `error_handling.py`: `grpc_error_handler` decorator — catches unhandled exceptions and aborts with `INTERNAL` status
- `src/panoseti_grpc/panoseti_util/` — PFF file format, config-file parsing, DAQ shutdown helpers. Used across services.

### DAQ Control Service
Manages the Hashpipe process lifecycle on each DAQ node. `DaqControlServicer` tracks the hashpipe PID (`self.hashpipe_pid`). Key behaviors:
- `StartDaq` fails immediately if a Hashpipe process is already running (guards against double-start).
- `StopDaq` sends `SIGINT` and blocks until the process exits; returns `success=True` if already stopped.
- `CleanupData` is blocked while `hashpipe_pid > 0`.
- Hashpipe stdout/stderr are streamed to per-run log files under `{data_dir}/{run_dir}/hp_stdout.log` and `hp_stderr.log`.

## Testing Infrastructure
- `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`) — all async tests run without explicit markers.
- Integration tests spin up Docker Compose stacks defined in each service's `tests/<service>/` directory. The CI scripts handle this automatically.
- `conftest.py` files in each test directory provide server fixtures and client factories.
- DAQ Data integration tests create isolated servers in `tempfile.TemporaryDirectory()` — no Docker needed, purely in-process.
- DAQ Control tests build a real C++ Hashpipe binary inside Docker using the shared `Dockerfile.ci` (root of repo, BuildKit stage `daq-control-test`). The `test_concurrent_requests.py` and `test_process_edge_cases.py` tests use `module_id=[252]` to avoid conflicts with the main integration test (which uses `[250, 251]`).
- Telemetry integration tests require a running Redis instance (provided by Docker Compose). Tests that assert Redis state after RPCs must poll with a timeout rather than using fixed `time.sleep` delays, because `RedisBatcher` introduces a flush latency.

## Key Gotchas
- **`init_sim()` vs `init_hp_io()`**: `init_sim()` on `AioDaqDataClient` is a convenience wrapper; to force re-init (e.g. in tests), use `init_hp_io(hosts, hp_io_cfg)` with `hp_io_cfg = {**hp_io_config_simulate, "simulate_daq": True, "force": True}`.
- **UDS simulation ordering**: `SimulationManager.setup_environment()` must be called *after* `HpIoManager` is valid because it connects to the UDS sockets that `HpIoManager` creates.
- **Non-breaking space in README files**: Some older README sections contain `\xa0` (non-breaking space) characters from copy-paste. The `Edit` tool will fail to match these — use a Python `str.replace` via `Bash` instead.
- **Proto changes require recompilation**: After editing any `.proto` file, run `python scripts/compile_protos.py` before testing.
