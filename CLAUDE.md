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
Unified QA runner (recommended):
```bash
python tests/qa.py all                # Run all linters and test suites
python tests/qa.py lint               # Run linters (Ruff, MyPy)
python tests/qa.py telemetry          # Run specific test suite (e.g. telemetry)
```

Individual service CI shell scripts (legacy):
```bash
./scripts/run-ci-tests/run-daq-data-ci-test.sh
./scripts/run-ci-tests/run-daq-control-test.sh
./scripts/run-ci-tests/run-telemetry-ci-test.sh
./scripts/run-ci-tests/run-unified-server-ci-test.sh
./scripts/run-ci-tests/run-ublox-ci-test.sh
./scripts/run-ci-tests/run-hashpipe-daq-data-ci.sh
```


Run unified server tests locally (unit tests require no services; integration tests skip gracefully without Redis):
```bash
pytest tests/unified_server/unit/ -v
pytest tests/unified_server/integration/ -v --timeout=90
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

Observatory CLI (`pseti-grpc`) for a running server:
```bash
pseti-grpc status                      # probe all services and print table
pseti-grpc reflect                     # list services via gRPC reflection
pseti-grpc telemetry log --message '{"event":"test"}'
pseti-grpc daq-data ping
pseti-grpc daq-data init-sim           # init simulation mode
pseti-grpc daq-data stream --seconds 5
pseti-grpc daq-control status
pseti-grpc --host mynode --port 50051 status  # connect to remote
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
| `daq_control` | Production | Start/stop Hashpipe, generate manifests, selective cleanup with integrity precondition |
| `telemetry` | Beta | Device status → Redis/InfluxDB; log shipping via Grafana Alloy → Loki (shadow period) |
| `ublox_control` | 🔴 Deprecated | GNSS chip control — disabled by default; removed in next major release |

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

**Two independent pipelines** share one gRPC service:

**Device status path (`ReportStatus` RPC)** — the active, authoritative path:
- **Production devices** (registered in schema): strict Pydantic schema, permanent Redis HASH + InfluxDB timeseries. TTL = -1. Key: `{DEVICE_TYPE}_{device_id}`.
- **DEV_ devices**: flexible JSON payload, Redis TTL ≤ 3600 s.
- **Unknown types**: routed to `SANDBOX:{type}:{device_id}` namespace with positive TTL.
`RedisBatcher` in `server.py` batches up to 100 `ReportStatus` RPCs before flushing to Redis. Integration tests must poll rather than using fixed `time.sleep` waits because of this flush latency.

**Log shipping path — shadow period (Alloy migration):**

`logger.py` (`get_logger()`) currently writes to **three** destinations simultaneously:
1. Console via `RichHandler` (human-readable).
2. `{service}.log` — plain text `RotatingFileHandler`.
3. `{service}.jsonl` — structured JSON `RotatingFileHandler` (`JsonlFormatter`), one JSON object per line.  **Grafana Alloy** reads `.jsonl` files from `$PANOSETI_LOG_DIR/` and ships them to Loki (see `alloy/config.alloy`).
4. gRPC `Log` RPC via `AsyncGrpcHandler` — legacy path running in parallel during the migration window.

The `.jsonl` format emits: `timestamp`, `service`, `level`, `message`, `hostname`, `pid`, `thread`, plus any `extra` dict fields (`git_commit`, `run_id`, …).

Once the Alloy soak period passes (log-line divergence < 0.1% vs. gRPC path over 7 observing days), the gRPC `Log` RPC, `AsyncGrpcHandler`, and `RedisBatcher` log queue will be removed.

### U-blox Control Service
**Deprecated.** Disabled by default (`ublox_control = false` in all server profiles). Will be removed in the next major release. Migrate GNSS data ingestion to `Telemetry.ReportStatus` with `GnssPayload`.

### Unified Server
`src/panoseti_grpc/server.py` hosts all active services on a single `grpc.aio.Server` instance (one port). gRPC routes RPCs by proto package name automatically, so there is no collision.

After all services start, the server automatically calls `grpc_utils.health.register_health()`, which registers a `grpc.health.v1.HealthServicer` and marks every active service `SERVING`. Use `grpc_health_probe` or `HealthClient` instead of the old `daq_data.Ping` RPC for liveness probes:
```bash
grpc_health_probe -addr=daqnode-1:50051 -service=panoseti.daq_control
```

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
- `CleanupData` is blocked while `hashpipe_pid > 0`. Supports two modes via `CleanupMode` enum:
  - `CLEANUP_FULL` (0, default) — legacy `rmtree` behavior; wire-compatible with old clients.
  - `CLEANUP_SELECTIVE` (1) — deletes only files matching `delete_patterns` not covered by `preserve_patterns`. **Requires `manifest_digest`** (SHA-256 of the manifest file content): the server recomputes the digest of its local manifest and aborts with `FAILED_PRECONDITION` if values differ. This guarantees no `.pff` data is deleted without head-node integrity confirmation.
- `GenerateManifest` — computes blake3/xxhash/sha256 checksums for run files; writes a 4-column manifest atomically (`{digest}  {size}  {mtime_ns}  {relpath}`); implemented in `manifest.py` using `asyncio.to_thread` for blocking I/O.
- `GetManifest` — server-streaming RPC that yields `ManifestEntry` per line of the manifest file; path-traversal guarded.
- Hashpipe stdout/stderr are streamed to per-run log files under `{data_dir}/{run_dir}/hp_stdout.log` and `hp_stderr.log`.

**Client methods** (`AsyncDaqControlClient` / `DaqControlClient`) are all decorated with `@grpc_call` from `grpc_utils.decorators`, which maps `grpc.RpcError → PanosetiRpcError` subclasses and never suppresses `asyncio.CancelledError`. Callers catch typed exceptions instead of raw `grpc.RpcError`:
```python
from panoseti_grpc.grpc_utils import FailedPreconditionError
try:
    await client.CleanupData(params)
except FailedPreconditionError as exc:
    logger.error("Cleanup refused — manifest digest mismatch: %s", exc.details)
```

Pydantic models in `client_models.py` (`CleanupDataParameters`, `GenerateManifestParameters`, …) are validated before hitting the network.

### grpc_utils — Shared gRPC Machinery
`src/panoseti_grpc/grpc_utils/` is a service-agnostic package imported by all active services. See `grpc_utils/README.md` for the full decision framework. Key modules:

| Module | What it provides |
|---|---|
| `exceptions.py` | `PanosetiRpcError` + 8 typed subclasses; `from_rpc_error(e, target)` factory |
| `decorators.py` | `@grpc_call` — wraps async/sync/generator methods; maps `grpc.RpcError`; never suppresses `CancelledError` |
| `channel.py` | `AsyncChannelManager` — owns channel lifecycle with keepalive options |
| `retries.py` | `build_retry_service_config()` — declarative retry policy JSON |
| `health.py` | `register_health(server, names)` + `HealthClient`; auto-called by `PanosetiServer.run()` |
| `interceptors.py` | Lightweight client/server interceptor stubs |

**Concurrency rule** (see `grpc_utils/README.md` for full rationale):
- Use `asyncio.TaskGroup` for all-or-nothing fan-outs (startup, manifest gen, teardown ladder).
- Use outcome-collection under `TaskGroup` for best-effort fan-outs (cleanup, stop-all, probes).
- Never silently discard exceptions from `asyncio.gather(return_exceptions=True)`.

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
- **`grpc_error_handler` and async generators**: The decorator in `util/error_handling.py` uses `inspect.isasyncgenfunction` to detect server-streaming handlers (like `GetManifest`). For such functions it wraps them in an `agen_wrapper` that yields items. If you add a new server-streaming RPC and the decorator is not working, verify `inspect.isasyncgenfunction(your_handler)` returns `True`. Plain `async def` handlers (unary RPCs) are handled by the standard `async_wrapper` branch.
- **Submodule divergence resolution**: When the grpc submodule and main grpc branch diverge, use `git fetch <worktree-path>/grpc HEAD` from the main grpc directory to fetch objects from the worktree, then rebase. Conflicts are typically limited to overlapping edits in `client.py` — check `git diff <base>..<branch1>` and `git diff <base>..<branch2>` side-by-side before rebasing.
