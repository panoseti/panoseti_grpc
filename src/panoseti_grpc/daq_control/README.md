# DAQ Control Service

The **PANOSETI DAQ Control Service** is a gRPC service for managing Hashpipe data-acquisition instances on DAQ nodes. It handles the full lifecycle of an observation run: starting and stopping Hashpipe, monitoring node status, and cleaning up data directories after a run completes.

## Key Features

* **Full Run Lifecycle Management:** Start, stop, and clean up a Hashpipe observation run via a single unified gRPC API.
* **Pre-flight Validation:** All request parameters are validated with Pydantic before any file system or process operation is performed.
* **Structured Logging:** Server logs are written to `/var/log/panoseti/daq_control_server.log` via `RotatingFileHandler` and forwarded to the central Telemetry service.
* **Per-run Hashpipe Logs:** Hashpipe's `stdout` and `stderr` are captured asynchronously and stored as `hp_stdout.log` / `hp_stderr.log` inside the run directory, and also shipped to the Telemetry service.
* **Status Monitoring:** Query Hashpipe process status, disk usage, and existing run directories in a single RPC call.

---

## RPC API

The service exposes four unary RPCs defined in [`protos/daq_control.proto`](../../../../protos/daq_control.proto).

### `StartDaq`

Starts a Hashpipe instance on the DAQ node. Fails immediately if Hashpipe is already running.

| Parameter | Type | Description |
|---|---|---|
| `data_dir` | `str` | Root directory for PANOSETI data (e.g. `/mnt/panoseti`) |
| `daq_ip_addr` | `str` | IP address of this DAQ node |
| `bindhost` | `str` | Network interface for receiving packets (e.g. `enp171s0`) |
| `max_file_size_mb` | `uint32` | Max output file size in MB (1–99999) |
| `group_ph_frames` | `bool` | Group PH frames from 4 Qubaos into a single frame |
| `run_dir` | `str` | Name of the run directory to create under `data_dir` (conventionally `*.pffd`) |
| `obs` | `str` | Observation name tag (max 16 chars) |
| `module_id` | `list[int]` | Module IDs (0–255) assigned to this DAQ node |

Creates the following directory layout before launching Hashpipe:

```
{data_dir}/
├── module.config            ← space-separated list of module IDs
├── {run_dir}/               ← config directory for this run
│   ├── hp_stdout.log        ← Hashpipe stdout (RotatingFileHandler)
│   └── hp_stderr.log        ← Hashpipe stderr (RotatingFileHandler)
└── module_{id}/{run_dir}/   ← per-module data directory (one per module_id)
```

### `StopDaq`

Sends `SIGINT` to the running Hashpipe process and waits for it to exit. Returns success immediately if no instance is running (idempotent).

| Parameter | Type | Description |
|---|---|---|
| `data_dir` | `str` | Root data directory |
| `run_dir` | `str` | Run directory name |

### `StatusDaq`

Returns node status. Each check is optional and controlled by a boolean flag.

| Parameter | Type | Description |
|---|---|---|
| `data_dir` | `str` | Root data directory (must exist) |
| `check_hashpipe_running` | `bool` | Include Hashpipe process status in response |
| `check_disk_usage` | `bool` | Include total/used/free disk space in response |
| `check_run_dirs` | `bool` | Include list of `*.pffd` directories in response |

Response fields:

| Field | Type | Description |
|---|---|---|
| `hashpipe_running` | `bool` | Whether Hashpipe is currently running |
| `disk_usage` | `dict` | `total_disk_space`, `used_disk_space`, `free_disk_space` (bytes) |
| `run_dirs` | `list[str]` | Absolute paths of all `*.pffd` directories in `data_dir` |

### `CleanupData`

Removes the run directory and all per-module run directories. Rejected if Hashpipe is still running.

| Parameter | Type | Description |
|---|---|---|
| `data_dir` | `str` | Root data directory (must exist) |
| `run_dir` | `str` | Run directory to delete (must exist) |
| `module_id` | `list[int]` | Module IDs whose data directories will be deleted |

Removes:
- `{data_dir}/{run_dir}/`
- `{data_dir}/module_{id}/{run_dir}/` for each `id` in `module_id`

---

## Usage

### 1. Running the Server

The server is designed to run as a daemon on each DAQ node.

```bash
# Start the server on the default port (50051)
python -m panoseti_grpc.daq_control.server

# Specify a custom port via environment variable
GRPC_PORT=50052 python -m panoseti_grpc.daq_control.server
```

Server logs are written to `/var/log/panoseti/daq_control_server.log` and forwarded to the Telemetry service (if running).

### 2. Using the Client API

`DaqControlClient` wraps the gRPC stub and raises Python exceptions on failure:
- `ValueError` — server rejected the request (e.g. Hashpipe already running)
- `ConnectionError` — network or gRPC transport failure

#### Full Observation Run Example

```python
from panoseti_grpc.daq_control.client import DaqControlClient

client = DaqControlClient(host="192.168.0.228", port=50051)

# --- 1. Start an observation run ---
client.StartDaq({
    "data_dir":        "/mnt/panoseti",
    "daq_ip_addr":     "192.168.0.228",
    "bindhost":        "enp171s0",
    "max_file_size_mb": 512,
    "group_ph_frames": True,
    "run_dir":         "obs_2026_03_19.pffd",
    "obs":             "palomar-01",
    "module_id":       [250, 251],
})

# --- 2. Check node status mid-run ---
_, status = client.StatusDaq({
    "data_dir":              "/mnt/panoseti",
    "check_hashpipe_running": True,
    "check_disk_usage":       True,
    "check_run_dirs":         False,
})
print(f"Hashpipe running: {status['hashpipe_running']}")
print(f"Free disk: {status['disk_usage']['free_disk_space'] / 1e9:.1f} GB")

# --- 3. Stop the run ---
client.StopDaq({
    "data_dir": "/mnt/panoseti",
    "run_dir":  "obs_2026_03_19.pffd",
})

# --- 4. Clean up data directories after transferring files ---
client.CleanupData({
    "data_dir":  "/mnt/panoseti",
    "run_dir":   "obs_2026_03_19.pffd",
    "module_id": [250, 251],
})
```

#### Error Handling

```python
try:
    client.StartDaq({...})
except ValueError as e:
    print(f"Server rejected request: {e}")   # e.g. Hashpipe already running
except ConnectionError as e:
    print(f"Could not reach server: {e}")
```

### 3. CLI

The CLI is suitable for manual operation and scripting. It reads all parameters from a JSON config file.

```bash
# Start a run
python -m panoseti_grpc.daq_control.cli --op startdaq --config configs/test.json

# Check node status
python -m panoseti_grpc.daq_control.cli --op statusdaq --config configs/test.json

# Stop the run
python -m panoseti_grpc.daq_control.cli --op stopdaq --config configs/test.json

# Clean up data directories
python -m panoseti_grpc.daq_control.cli --op cleanupdata --config configs/test.json

# Connect to a remote node
python -m panoseti_grpc.daq_control.cli --host 192.168.0.228 --port 50051 --op statusdaq --config configs/test.json
```

#### Config File Format

All four operations can share a single JSON file. Each top-level key maps to one operation:

```json
{
    "startdaq": {
        "data_dir":        "/mnt/panoseti",
        "daq_ip_addr":     "192.168.0.228",
        "bindhost":        "enp171s0",
        "max_file_size_mb": 10,
        "group_ph_frames": true,
        "run_dir":         "test.pffd",
        "obs":             "ucb-lab",
        "module_id":       [250, 251]
    },
    "stopdaq": {
        "data_dir": "/mnt/panoseti",
        "run_dir":  "test.pffd"
    },
    "statusdaq": {
        "data_dir":               "/mnt/panoseti",
        "check_hashpipe_running": true,
        "check_disk_usage":       true,
        "check_run_dirs":         true
    },
    "cleanupdata": {
        "data_dir":  "/mnt/panoseti",
        "run_dir":   "test.pffd",
        "module_id": [250, 251]
    }
}
```

A ready-to-edit template is at [`configs/test.json`](configs/test.json).

---

## Logging

### Server Logs

The server uses `get_logger` from the Telemetry service, which attaches:

- **`RotatingFileHandler`** — writes to `/var/log/panoseti/daq_control_server.log`
- **`AsyncGrpcHandler`** — ships logs to the central Telemetry/Loki service
- **Console handler** — rich-formatted output to the terminal

```bash
# Tail server logs directly
tail -f /var/log/panoseti/daq_control_server.log
```

### Hashpipe Process Logs

When `StartDaq` launches Hashpipe, its `stdout` and `stderr` are captured asynchronously and written to the run directory:

```
{data_dir}/{run_dir}/
├── hp_stdout.log    ← Hashpipe standard output
└── hp_stderr.log    ← Hashpipe standard error
```

Both files are also forwarded to the central Telemetry service in real time. The log files use `RotatingFileHandler` to prevent unbounded disk growth during long runs.

---

## Troubleshooting

| Error | Cause | Solution |
|---|---|---|
| `ValueError: Server rejected data: Found N HASHPIPE instances running` | `StartDaq` called while Hashpipe is already running | Call `StopDaq` first, or check the node manually |
| `ValueError: Server rejected data: HASHPIPE is running, pid[...]` | `CleanupData` called while Hashpipe is running | Always call `StopDaq` before `CleanupData` |
| `ConnectionError: gRPC failed: ...` | Server is unreachable | Verify the server is running on the correct host/port |
| `ValidationError` from Pydantic | Invalid parameter values (e.g. `module_id > 255`, `bindhost` > 16 chars) | Check the parameter constraints in the RPC table above |
| `hp_stdout.log` / `hp_stderr.log` not created | Hashpipe process exited before writing any output | Check `hp_stderr.log` in the run directory or server logs for the startup error |
