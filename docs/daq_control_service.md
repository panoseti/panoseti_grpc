# DAQ Control Service

The DAQ Control Service manages the lifecycle of the Hashpipe data-acquisition process on each DAQ node. It exposes a gRPC interface for starting, stopping, and monitoring Hashpipe instances, generating data integrity manifests, and cleaning up run data directories selectively or in full.

---

## Overview

Each physical DAQ node runs one instance of this server. The observatory control system (or any authorized client) issues RPCs to coordinate science runs across all nodes.

```
Observatory Control ──gRPC──► DaqControlServicer
                                    │
                                    ├── asyncio.create_subprocess_exec("hashpipe …")
                                    ├── psutil  (PID tracking, process signals)
                                    ├── manifest.py  (blake3/xxhash checksums)
                                    └── get_logger  (stdout/stderr piped to log files + Loki)
```

Hashpipe subprocess output is streamed in real time to two per-run log files (`hp_stdout.log`, `hp_stderr.log`) under `{data_dir}/{run_dir}/`, and forwarded to the Telemetry gRPC logger (Loki sink) simultaneously.

---

## Python Client API

We provide both synchronous and asynchronous clients. For performance-critical coordination (like starting an observatory run), the **Async Client** is recommended.

### Async Client (`AsyncDaqControlClient`)

The async client uses `grpc.aio` and should be used as an async context manager to ensure the gRPC channel is closed properly.

```python
from panoseti_grpc.daq_control.client import AsyncDaqControlClient

async def main():
    async with AsyncDaqControlClient(host="daq-node-01", port=50051) as client:
        # 1. Start a run
        await client.StartDaq({
            "data_dir": "/data/panoseti",
            "daq_ip_addr": "192.168.1.10",
            "bindhost": "eth0",
            "max_file_size_mb": 1024,
            "group_ph_frames": True,
            "run_dir": "start_2026-01-01T120000Z.pffd",
            "obs": "gj1132",
            "module_id": [224, 225],
        })

        # 2. Check status
        success, status = await client.StatusDaq({
            "data_dir": "/data/panoseti",
            "check_hashpipe_running": True
        })
        print(f"Hashpipe running: {status['hashpipe_running']}")
```

### Sync Client (`DaqControlClient`)

A standard blocking client for simple scripts or REPL usage.

```python
from panoseti_grpc.daq_control.client import DaqControlClient

client = DaqControlClient(host="daq-node-01", port=50051)
client.StopDaq({"data_dir": "/data", "run_dir": "run.pffd"})
client.close()
```

---

## gRPC API Reference

### `StartDaq`
...

Launches a new Hashpipe instance. Fails immediately if a Hashpipe process is already running.

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root directory for PANOSETI data (e.g. `/data/panoseti`) |
| `daq_ip_addr` | `string` | IP address of this DAQ node |
| `bindhost` | `string` | Ethernet interface name for packet reception (e.g. `eth0`) |
| `max_file_size_mb` | `float` | Maximum PFF output file size in MB |
| `group_ph_frames` | `bool` | Group pulse-height frames from all four Quabos into one PFF record |
| `run_dir` | `string` | Subdirectory name for this run (created under `data_dir`) |
| `obs` | `string` | Observation name tag (written into PFF headers) |
| `module_id` | `repeated uint32` | Module IDs (0–255) assigned to this node |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | `true` if Hashpipe started successfully |
| `message` | `string` | Error description on failure |

**Side effects**
- Creates `{data_dir}/module.config` listing the assigned module IDs.
- Creates `{data_dir}/{run_dir}/` for config files.
- Creates `{data_dir}/module_{id}/{run_dir}/` for each module ID.
- Spawns Hashpipe in a new process session (`start_new_session=True`).
- Starts background log-streaming task piping Hashpipe stdout/stderr.

---

### `StopDaq`

Sends `SIGINT` to the running Hashpipe process and waits for it to exit.

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root data directory (used for validation) |
| `run_dir` | `string` | Run subdirectory (used for validation) |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | `true` if Hashpipe has stopped (or was not running) |

Returns `success=True` immediately if no Hashpipe process is tracked.

---

### `StatusDaq`

Queries the state of this DAQ node. All checks are opt-in via boolean flags.

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root data directory |
| `check_hashpipe_running` | `bool` | Whether to check if Hashpipe is alive |
| `check_disk_usage` | `bool` | Whether to report disk usage for `data_dir` |
| `check_run_dirs` | `bool` | Whether to list `.pffd` run directories under `data_dir` |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | Always `true` if the RPC reaches the server |
| `hashpipe_running` | `bool` | Whether the tracked Hashpipe PID is alive |
| `disk_usage` | `google.protobuf.Struct` | Keys: `total_disk_space`, `used_disk_space`, `free_disk_space` (bytes); `-1` when not requested |
| `run_dirs` | `repeated string` | Paths matching `{data_dir}/*.pffd` |

---

### `CleanupData`

Deletes run data from DAQ node directories. **Blocked while Hashpipe is running.**

Supports two cleanup modes controlled by the `mode` field:

#### `CLEANUP_FULL` (default, legacy-compatible)

Removes the entire run directory tree via `rmtree`. Wire-compatible with pre-Phase-1 clients (the `mode` field defaults to `CLEANUP_FULL` when absent).

#### `CLEANUP_SELECTIVE`

Walks the run directory and deletes only files matching any `delete_patterns` glob that are **not** also matched by `preserve_patterns`. Empty subdirectories are left in place. Used by the Transfer Daemon to remove science PFF files while keeping metadata (`.json`, `.log`, manifests) as a permanent on-DAQ catalog.

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root data directory |
| `run_dir` | `string` | Run subdirectory to clean |
| `module_id` | `repeated uint32` | Module IDs whose per-module run dirs to process |
| `force` | `bool` | Bypass the "hashpipe running" guard (use with caution) |
| `mode` | `CleanupMode` | `CLEANUP_FULL` (0, default) or `CLEANUP_SELECTIVE` (1) |
| `delete_patterns` | `repeated string` | Glob patterns to delete in selective mode (e.g. `["*.pff"]`) |
| `preserve_patterns` | `repeated string` | Glob patterns that take precedence over delete (e.g. `["*.json", "*.log"]`) |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | `true` if cleanup completed without errors |
| `message` | `string` | Error description on failure |
| `deleted_count` | `uint32` | Number of files deleted (selective mode) |
| `freed_bytes` | `uint64` | Bytes freed (selective mode) |
| `preserved_paths` | `repeated string` | Relative paths of files that were preserved (audit trail) |

**Typical Transfer Daemon usage:**
```python
client.CleanupData({
    "data_dir": "/app/data",
    "run_dir": "start_2026-01-01T120000Z.pffd",
    "module_id": [224, 225],
    "mode": "CLEANUP_SELECTIVE",
    "delete_patterns": ["*.pff"],
    "preserve_patterns": ["*.json", "*.log", "*.toml"],
})
```

---

### `GenerateManifest`

Computes a cryptographic checksum manifest for files in a module's run directory. Manifests are written atomically to the DAQ node and can be streamed back via `GetManifest`.

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root data directory |
| `run_dir` | `string` | Run subdirectory |
| `module_id` | `repeated uint32` | Module ID(s) to generate manifests for |
| `algorithm` | `string` | Hash algorithm: `"blake3"` (default) → `"xxh3_128"` → `"sha256"` fallback chain |
| `include_patterns` | `repeated string` | Glob patterns for files to include (default: `["*.pff"]`) |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | `true` if manifest was written successfully |
| `message` | `string` | Error description on failure |
| `manifest_path` | `string` | Absolute path to the written manifest file |
| `file_count` | `uint32` | Number of files hashed |
| `total_bytes` | `uint64` | Total bytes of hashed files |
| `elapsed_seconds` | `double` | Wall-clock time for hashing |
| `algorithm` | `string` | Algorithm actually used (may differ from request if fallback triggered) |

**Manifest file format** (4-column, newline-delimited):
```
{digest_hex}  {size_bytes}  {mtime_ns}  {relative_path}
```
Written atomically via `tempfile.mkstemp` + `os.replace`. Compatible with `b3sum --check` when using blake3.

**Implementation:** `manifest.py` — `async def compute_manifest(run_dir, patterns, algo)`. Uses `asyncio.to_thread` for blocking I/O. Algorithm fallback chain: blake3 → xxhash → sha256.

---

### `GetManifest`

Server-streaming RPC. Reads a previously generated manifest and yields one `ManifestEntry` per file.

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root data directory |
| `run_dir` | `string` | Run subdirectory |
| `module_id` | `uint32` | Module ID whose manifest to stream |

**Streamed `ManifestEntry` fields**

| Field | Type | Description |
|---|---|---|
| `relative_path` | `string` | Path relative to the module run directory |
| `digest_hex` | `string` | Hex-encoded checksum |
| `size_bytes` | `uint64` | File size in bytes |
| `mtime_ns` | `int64` | Modification time in nanoseconds since epoch |

The server guards against path traversal: any entry with `..` in its path is skipped and logged as a warning.

---

## Typical Async Workflow

### Multi-node coordination

```python
import asyncio
from panoseti_grpc.daq_control.client import AsyncDaqControlClient

async def run_observatory():
    nodes = ["daq-1", "daq-2", "daq-3"]
    run_dir = "start_2026-01-01T120000Z.pffd"
    
    # 1. Start all nodes concurrently
    async def start_node(host):
        async with AsyncDaqControlClient(host=host) as client:
            return await client.StartDaq({...})

    results = await asyncio.gather(*[start_node(n) for n in nodes])
    
    # ... Wait for run to finish ...

    # 2. Generate manifests concurrently
    async def gen_manifest(host):
        async with AsyncDaqControlClient(host=host) as client:
            return await client.GenerateManifest({...})
            
    await asyncio.gather(*[gen_manifest(n) for n in nodes])
```

---

## gRPC API Reference

## Configuration & Validation

Request parameters are validated server-side via [Pydantic v2](https://docs.pydantic.dev/) models (`config.py`):

| Model | Used by | Key constraints |
|---|---|---|
| `StartDaqModel` | `StartDaq` | `data_dir` auto-created; `module_id` values in 0–255; `obs`/`bindhost` 1–16 chars |
| `StopDaqModel` | `StopDaq` | `run_dir` must already exist under `data_dir` |
| `StatusDaqModel` | `StatusDaq` | `data_dir` must exist |
| `CleanupDataModel` | `CleanupData` | `run_dir` must exist; `CLEANUP_SELECTIVE` requires non-empty `delete_patterns` |
| `GenerateManifestModel` | `GenerateManifest` | `algorithm` must be `"blake3"`, `"xxh3_128"`, or `"sha256"`; `include_patterns` non-empty |

---

## Log Files

| File | Location | Content |
|---|---|---|
| Server log | `/var/log/panoseti/daq_control_server.log` | Service lifecycle events, RPC results |
| Hashpipe stdout | `{data_dir}/{run_dir}/hp_stdout.log` | Per-run Hashpipe standard output |
| Hashpipe stderr | `{data_dir}/{run_dir}/hp_stderr.log` | Per-run Hashpipe standard error |
| Manifest | `{data_dir}/module_{id}/{run_dir}/manifest.{algo}` | Per-module checksum manifest |

All logs are also forwarded to Loki via the Telemetry gRPC logger when the Telemetry service is reachable.

---

## Running the Server

```bash
# Default port 50051
GRPC_PORT=50051 python -m panoseti_grpc.daq_control.server

# Via Docker (recommended for production)
docker compose -f docker/daq_control/docker-compose.yml up
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GRPC_PORT` | `50051` | TCP port the server listens on |

---

## Testing

```bash
# Run the full DAQ Control CI test suite
./scripts/run-ci-tests/run-daq-control-test.sh

# Or via unified QA runner
python tests/qa.py daq_control
```

Tests are under `tests/daq_control/` and cover:

**Unit tests** (`tests/daq_control/unit/`):
- `test_proto_schema.py` — proto field/enum existence for all new fields (CleanupMode, delete_patterns, GenerateManifest, ManifestEntry, etc.)
- `test_cleanup_model.py` — Pydantic rejects `CLEANUP_SELECTIVE` with empty `delete_patterns`; accepts `CLEANUP_FULL` without patterns
- `test_manifest_model.py` — algorithm enum guard; default include_patterns; invalid algorithm rejected

**Integration tests** (`tests/daq_control/integration/`):
- `test_cleanup_selective.py` — populate fake run dir with `.pff` + `.json` + `.log`; call `CLEANUP_SELECTIVE`; assert only `.pff` removed; `preserved_paths` echoes metadata files
- `test_manifest_roundtrip.py` — generate manifest via `GenerateManifest`; stream back via `GetManifest`; verify digest count and sizes match `hashlib` reference; 4-column format verified
- Existing lifecycle tests: start/stop/status/cleanup, concurrent request handling, process edge cases (crash detection, stale PID recovery, log file placement, disk usage reporting)

**Wire compatibility:** All new proto fields use new tag numbers. `CleanupMode` defaults to `CLEANUP_FULL` (value 0). Old clients that omit `mode` continue to get legacy `rmtree` behavior.
