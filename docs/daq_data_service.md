# DaqData Service

## Architecture Overview

The DaqData service uses a **two-tier gateway/edge architecture**:

```
Consumers (notebooks, analysis scripts, pseti-grpc CLI)
    │
    └── AioDaqDataClient(headnode_host, port)     ← single connection
            │   gRPC StreamImages / Status
            ▼
    DaqDataGatewayServicer  (headnode)
        ├── AioDaqDataClient(daq-node-1, port)
        ├── AioDaqDataClient(daq-node-2, port)
        └── AioDaqDataClient(daq-node-N, port)
                │   gRPC StreamImages
                ▼
    DaqDataServicer (each DAQ node)
        │   Unix Domain Socket
        ▼
    Hashpipe output_thread
```

**Consumers always connect to the headnode gateway** — a single `host:port`. The gateway fans in from all edge nodes and multiplexes streams to connected clients. M×N connection scaling is eliminated.

Edge node servers **auto-initialize** the UDS data path on startup (no `InitHpIo` call required). The `InitHpIo` RPC remains available as optional reconfiguration (e.g. to change `data_dir` or force a restart with simulation data).

---

## Quick Start

### Async Client (recommended)

```python
import asyncio
from panoseti_grpc.daq_data.client import AioDaqDataClient

async def main():
    async with AioDaqDataClient(host="headnode", port=50051) as client:
        async for image in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=1.0,
        ):
            print(f"Module {image['module_id']}  {image['type']}  {image['header']['pandas_unix_timestamp']}")

asyncio.run(main())
```

### Sync Client

```python
from panoseti_grpc.daq_data.client import DaqDataClient

with DaqDataClient(host="headnode", port=50051) as client:
    for image in client.stream_images(
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=1.0,
    ):
        print(f"Module {image['module_id']}")
```

### Development / Simulation

To stream simulated data without hardware, start an edge server in simulation mode and connect via the gateway (or directly):

```bash
# Start edge server with simulation auto-init
pseti-grpc server --profile daq_node
# Or override the default hp_io config to simulation mode:
# pseti-grpc server --profile daq_node --config /path/to/sim_override.toml
```

```python
async with AioDaqDataClient(host="localhost", port=50051) as client:
    # Force simulation mode via optional reconfiguration
    await client.init_sim()
    async for image in client.stream_images(stream_movie_data=True):
        ...
```

---

## Python Client API Reference

Both `AioDaqDataClient` (async, `grpc.aio`) and `DaqDataClient` (sync, blocking) share the same method set. Use the async client for anything performance-critical.

### Constructor

```python
AioDaqDataClient(host: str, port: int, stop_event: asyncio.Event | None = None)
DaqDataClient(host: str, port: int)
```

- `host` / `port`: Address of the **headnode gateway** (or a direct edge node for testing).
- `stop_event`: When set, `stream_images` stops iterating and exits cleanly. Useful for SIGINT/SIGTERM handling.

### `stream_images(...)`

The primary method. Returns an `AsyncGenerator` (async client) or `Generator` (sync client) of parsed `PanoImage` dicts.

| Argument | Type | Default | Description |
|---|---|---|---|
| `stream_movie_data` | bool | `False` | Request movie-mode (image) frames |
| `stream_pulse_height_data` | bool | `False` | Request pulse-height frames |
| `update_interval_seconds` | float | `1.0` | Desired update rate |
| `module_ids` | tuple | `()` | Module ID whitelist; empty = all modules |
| `parse_pano_images` | bool | `True` | Parse proto to dict; `False` returns raw proto |

Returns `DEADLINE_EXCEEDED` if no frames arrive within the server's `reader_timeout`.

### `init_hp_io(hp_io_cfg: dict)`

Optional reconfiguration. Reinitializes the `HpIoManager` on the connected server. Acquires exclusive writer access and cancels all active `StreamImages` RPCs. Use `force=True` to preempt active readers.

```python
await client.init_hp_io({
    "data_dir": "/mnt/panoseti",
    "update_interval_seconds": 0.1,
    "force": True,
    "simulate_daq": False,
})
```

### `init_sim()`

Convenience wrapper for `init_hp_io` with `simulate_daq=True`. Deprecated for production use — prefer the edge server's auto-init with a real UDS path.

### `status()`

Returns the `StatusResponse` indicating whether `hp_io` is initialized.

### Health checking

Use `grpc_utils.HealthClient` or `pseti-grpc daqnode` instead of `Ping`:

```python
from panoseti_grpc.grpc_utils.health import HealthClient
async with HealthClient(host="headnode", port=50051) as hc:
    ok = await hc.check("daqdata.DaqData")
```

```bash
pseti-grpc daqnode --log-dir /var/log/panoseti
```

---

## Graceful Shutdown with `stop_event`

```python
import asyncio, signal
from panoseti_grpc.daq_data.client import AioDaqDataClient

async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with AioDaqDataClient("headnode", 50051, stop_event=stop) as client:
        async for image in client.stream_images(stream_movie_data=True):
            process(image)

asyncio.run(main())
```

---

## `PanoImage` Message Format

When `parse_pano_images=True` (default), `stream_images` yields dicts with this structure:

```python
{
    'type': 'MOVIE',                    # or 'PULSE_HEIGHT'
    'header': {
        'quabo_0': {
            'tv_usec': 779336.0,
            'tv_sec': 1721882092.0,
            'pkt_nsec': 779007488.0,
            'pkt_num': 37993.0,
            'pkt_tai': 529.0
        },
        # ... quabo_1 through quabo_3 ...
        'wr_unix_timestamp': Decimal('1721882092.779007488'),
        'pandas_unix_timestamp': Timestamp('2024-07-25 04:34:52.779007488')
    },
    'shape': [32, 32],                  # or [16, 16]
    'bytes_per_pixel': 2,               # 1 or 2
    'image_array': np.ndarray(...),     # dtype=uint8|uint16|int16, shape=(32,32)
    'file': 'start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_224.seqno_0.pff',
    'frame_number': 88,
    'module_id': 224
}
```

- `wr_unix_timestamp`: Derived Unix timestamp with nanosecond precision from WR/GNSS timing fields.
- `pandas_unix_timestamp`: Pandas-compatible timestamp.
- `image_array`: 2D NumPy array reshaped and cast from raw PFF bytes.

---

## Server Architecture (Edge Node)

```
Hashpipe output_thread
    │  [2-byte module_id][PFF frame]
    ▼
UdsDataSource (one per data product: img8, img16, ph256, ph1024)
    │  asyncio.Queue(maxsize=500)
    ▼
HpIoManager._processing_loop()
    │  assigns monotonic frame_id
    ▼
latest_data_cache[module_id]['movie'|'ph']
    │  polled by each reader at their update_interval
    ▼
StreamImages RPC → gateway → gRPC client
```

**Key components:**

- `UdsDataSource` — UDS server for one data product. Hashpipe connects as a client and sends `[2-byte big-endian module_id][PFF frame]` tuples.
- `HpIoManager` — Drains the central `asyncio.Queue`, assigns monotonic `frame_id`s, discovers module IDs dynamically, and writes to `latest_data_cache`.
- `latest_data_cache` — Shared dict `[module_id]['ph'|'movie']` storing the most-recent frame per (module, type) pair. Readers poll at their `update_interval_seconds`.
- `DaqDataGatewayServicer` — (headnode) Holds one `AioDaqDataClient` per edge node; fans in `StreamImages` RPCs using `asyncio.TaskGroup` with best-effort semantics (a down edge node does not cancel the merged stream).

**Auto-init:** Edge servers call `start_initial_task()` on startup from `default_hp_io_config_file` (default: real UDS, not simulation). No `InitHpIo` call is required before streaming in production.

---

## Core RPCs

### `StreamImages`

Streams `PanoImage` frames at `update_interval_seconds`. A frame is delivered when the interval has elapsed and a newer frame exists for the requested (module, type) pair. Any number of concurrent readers are supported.

### `InitHpIo`

Optional reconfiguration. Acquires exclusive writer access and cancels active `StreamImages` readers. Use `force=true` to preempt them.

### `Status`

Returns `hp_io_initialized` flag.

### `Ping`

**Deprecated.** Use `grpc.health.v1` (`HealthClient.check("daqdata.DaqData")`) instead.

---

## `daq_data_server_config.json` Reference

```json
{
    "init_from_default": true,
    "default_hp_io_config_file": "hp_io_config.json",
    "unix_domain_socket": "unix:///tmp/daq_data.sock",
    "max_concurrent_rpcs": 100,
    "max_read_queue_size": 50,
    "min_hp_io_update_interval_seconds": 0.001,
    "reader_timeout": 5.0,
    "shutdown_grace_period": 5.0,
    "hp_io_stop_timeout": 5.0,
    "log_dir": null,
    "grpc_logging": false,
    "acquisition_methods": {
        "uds": {
            "enabled": true,
            "data_products": ["img8", "img16", "ph256", "ph1024"],
            "socket_path_template": "/tmp/hashpipe_grpc.dp_{dp_name}.sock",
            "read_timeout": 60.0
        }
    },
    "simulate_daq_cfg": { "..." : "..." }
}
```

| Field | Default | Description |
|---|---|---|
| `init_from_default` | `true` | Auto-start `HpIoManager` on boot from `default_hp_io_config_file`. |
| `default_hp_io_config_file` | `"hp_io_config.json"` | Config file to load on auto-init (relative to `daq_data/config/`). |
| `unix_domain_socket` | `null` | Extra UDS listener for local IPC. |
| `max_concurrent_rpcs` | `100` | Max simultaneous client connections. |
| `max_read_queue_size` | `50` | `asyncio.Queue` capacity for the frame buffer. |
| `min_hp_io_update_interval_seconds` | `0.001` | Floor for client-requested `update_interval_seconds`. |
| `reader_timeout` | `5.0` | Seconds before `StreamImages` aborts with `DEADLINE_EXCEEDED`. |
| `shutdown_grace_period` | `5.0` | Seconds the gRPC server waits during graceful shutdown. |
| `hp_io_stop_timeout` | `5.0` | Seconds to wait for `HpIoManager` to stop. |
| `log_dir` | `null` | Directory for rotating log files. |
| `grpc_logging` | `false` | Forward logs to the Telemetry gRPC server. Disable in dev/test. |

---

## `hp_io_config.json` Reference

Used by `InitHpIo` (manual reconfiguration) and on-startup auto-init.

```json
{
    "data_dir": "/mnt/panoseti",
    "update_interval_seconds": 0.1,
    "force": true,
    "simulate_daq": false,
    "module_ids": [],
    "comments": "Real UDS path, all modules"
}
```

| Field | Description |
|---|---|
| `data_dir` | Run directory root. Contains `module_X/` subdirs. Ignored when `simulate_daq=true`. |
| `update_interval_seconds` | Snapshot period (must be ≥ `min_hp_io_update_interval_seconds`). |
| `force` | Preempt active `StreamImages` clients to re-init. |
| `simulate_daq` | Stream archived data (for development/testing). |
| `module_ids` | Module whitelist; `[]` = all modules. |

---

## Logging

Edge servers use `get_logger()` from `panoseti_grpc.telemetry.logger`, writing to:

1. Console (Rich)
2. `{service}.log` — rotating plain-text under `{log_dir}/{hostname}/`
3. `{service}.jsonl` — structured JSON for Grafana Alloy → Loki
4. Telemetry gRPC `Log` RPC (shadow period, when `grpc_logging=true`)

Set `grpc_logging: false` (the default) when no Telemetry server is running to avoid connection noise.

---

## Performance Notes

**Pub/sub polling model:** `latest_data_cache` stores only the most-recent frame per `(module_id, data_product)` pair — there is no per-reader queue. Fast producers silently overwrite slow ones; frame loss at high rates is by design.

**Idle detection:** If no fresh frames arrive for `reader_timeout` seconds, `StreamImages` aborts with `DEADLINE_EXCEEDED`. This fires when Hashpipe stops or simulation ends.

**Socket permissions:** UDS sockets are created with `0o600`. Hashpipe must run as the same OS user as the gRPC server.

**Frame rate:** Hashpipe uses non-blocking `writev()` and drops frames when the UDS buffer is full. The gRPC layer adds one `asyncio.Queue(maxsize=500)` for burst absorption only.
