# DAQ Control Service

The DAQ Control Service manages the lifecycle of the Hashpipe data-acquisition process on each DAQ node. It exposes a gRPC interface for starting, stopping, and monitoring Hashpipe instances, and for cleaning up run data directories.

---

## Overview

Each physical DAQ node runs one instance of this server. The observatory control system (or any authorized client) issues RPCs to coordinate science runs across all nodes.

```
Observatory Control ──gRPC──► DaqControlServicer
                                    │
                                    ├── asyncio.create_subprocess_exec("hashpipe …")
                                    ├── psutil  (PID tracking, process signals)
                                    └── get_logger  (stdout/stderr piped to log files + Loki)
```

Hashpipe subprocess output is streamed in real time to two per-run log files (`hp_stdout.log`, `hp_stderr.log`) under `{data_dir}/{run_dir}/`, and forwarded to the Telemetry gRPC logger (Loki sink) simultaneously.

---

## gRPC API

### `StartDaq`

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

Deletes run data directories. **Blocked while Hashpipe is running.**

**Request fields**

| Field | Type | Description |
|---|---|---|
| `data_dir` | `string` | Root data directory |
| `run_dir` | `string` | Run subdirectory to delete |
| `module_id` | `repeated uint32` | Module IDs whose per-module run dirs to delete |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | `true` if all directories were removed |
| `message` | `string` | Error description on failure |

Deletes: `{data_dir}/{run_dir}/` and `{data_dir}/module_{id}/{run_dir}/` for each ID.

---

## Typical Workflow

```python
from panoseti_grpc.daq_control.client import DaqControlClient

client = DaqControlClient(host="daq-node-01", port=50051)

# 1. Start a run
client.StartDaq({
    "data_dir": "/data/panoseti",
    "daq_ip_addr": "192.168.1.10",
    "bindhost": "eth0",
    "max_file_size_mb": 1024,
    "group_ph_frames": True,
    "run_dir": "run_20260101_120000",
    "obs": "gj1132",
    "module_id": [224, 225],
})

# 2. Check status
ok, status = client.StatusDaq({
    "data_dir": "/data/panoseti",
    "check_hashpipe_running": True,
    "check_disk_usage": True,
    "check_run_dirs": False,
})
print(status["hashpipe_running"])   # True
print(status["disk_usage"])         # {'total_disk_space': ..., ...}

# 3. Stop the run
client.StopDaq({
    "data_dir": "/data/panoseti",
    "run_dir": "run_20260101_120000",
})

# 4. (Optional) Clean up data
client.CleanupData({
    "data_dir": "/data/panoseti",
    "run_dir": "run_20260101_120000",
    "module_id": [224, 225],
})
```

---

## Configuration & Validation

Request parameters are validated server-side via [Pydantic v2](https://docs.pydantic.dev/) models (`config.py`):

| Model | Used by | Key constraints |
|---|---|---|
| `StartDaqModel` | `StartDaq` | `data_dir` auto-created; `module_id` values in 0–255; `obs`/`bindhost` 1–16 chars |
| `StopDaqModel` | `StopDaq` | `run_dir` must already exist under `data_dir` |
| `StatusDaqModel` | `StatusDaq` | `data_dir` must exist |
| `CleanupDataModel` | `CleanupData` | `run_dir` must exist under `data_dir` |

---

## Log Files

| File | Location | Content |
|---|---|---|
| Server log | `/var/log/panoseti/daq_control_server.log` | Service lifecycle events, RPC results |
| Hashpipe stdout | `{data_dir}/{run_dir}/hp_stdout.log` | Per-run Hashpipe standard output |
| Hashpipe stderr | `{data_dir}/{run_dir}/hp_stderr.log` | Per-run Hashpipe standard error |

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
```

Tests are under `tests/daq_control/` and cover:
- Unit tests: config validation, process helpers
- Integration tests: full start/stop/status/cleanup lifecycle, concurrent request handling, process edge cases (crash detection, stale PID recovery, log file placement, disk usage reporting)
