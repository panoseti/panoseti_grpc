# Telemetry Service

The PANOSETI Telemetry Service provides two independent pipelines under a single gRPC interface:

1. **Device status** (`ReportStatus` RPC) — validated hardware payloads → Redis (hot) + InfluxDB (cold)
2. **Log shipping** — structured `.jsonl` files → Grafana Alloy → Loki (primary path); gRPC `Log` RPC (shadow path during migration)

---

## Log Shipping (Grafana Alloy)

### 4-Path Unified Logger

All services use `panoseti_grpc.telemetry.logger.get_logger()`, which writes to **four destinations simultaneously**:

| Path | Output | Consumer |
|---|---|---|
| Console | Rich-formatted text | Developer / operator |
| `{service}.log` | Plain text, rotating | Local inspection |
| `{service}.jsonl` | Structured JSON, one record per line | **Grafana Alloy → Loki** (primary) |
| gRPC `Log` RPC | Protobuf `LogEntry` | Legacy path (shadow period) |

Files land in `{log_dir}/{hostname}/` — the per-host subdirectory lets Alloy glob `{log_dir}/*/*.jsonl` and label logs by host automatically.

### Usage

```python
from panoseti_grpc.telemetry.logger import get_logger

logger = get_logger(
    "daq_control.server",
    log_dir="/var/log/panoseti",   # writes to /var/log/panoseti/<hostname>/
    grpc_enabled=True,             # also forward via gRPC Log RPC
    console=True,
)

logger.info("Hashpipe started", extra={"run_id": "start_2026-01-01T120000Z", "module_ids": [224, 225]})
```

`extra` fields are merged into the top-level JSON object and are queryable in LogQL:
```
{service="daq_control.server"} | json | run_id = "start_2026-01-01T120000Z"
```

### JSONL Record Format

```json
{
    "timestamp": "2026-01-01 12:00:00,123",
    "service": "daq_control.server",
    "level": "INFO",
    "message": "Hashpipe started",
    "hostname": "pseti-daqnode-0",
    "pid": 1234,
    "thread": "MainThread",
    "run_id": "start_2026-01-01T120000Z",
    "module_ids": [224, 225]
}
```

### Grafana Alloy Configuration

`alloy/config.alloy` ships `.jsonl` files to Loki. Alloy is deployed as a systemd service alongside `pseti-grpc server`:

```bash
# Check Alloy liveness
curl http://localhost:12345/-/ready

# Or via pseti-grpc daqnode
pseti-grpc daqnode --log-dir /var/log/panoseti
```

The Alloy glob pattern `local.file_match "panoseti"` reads from `/var/log/panoseti/*/*.jsonl`, matching all per-host subdirectories.

---

## Device Status Path (`ReportStatus`)

### Data Modes

| Mode | Validation | Storage | TTL | Key prefix |
|---|---|---|---|---|
| **Production** | Strict Pydantic schema | Redis HASH + InfluxDB | -1 (permanent) | Device-specific (e.g. `UBLOX_ZED-F9T_`) |
| **Experimental** (`DEV_`) | None | Redis only | ≤ 24 h | Must start with `DEV_` |
| **Unknown** | None | Redis only | Positive TTL | `SANDBOX:{type}:{device_id}` |

### Client Usage

```python
from panoseti_grpc.telemetry.client import TelemetryClient

client = TelemetryClient("headnode", 50051)

# Production device — must match the Pydantic schema for 'gnss'
client.log_strict("gnss", "dome_01", {
    "satellites": 12,
    "lat": 37.3382,
    "lon": -121.8863,
    "fix_mode": "3D",
    "extra_data": {"dilution": 1.5}
})

# Experimental / R&D — any JSON, 24 h TTL
client.log_flexible("DEV_LIDAR_V1", "prototype_A", {
    "distance_mm": 4502,
    "signal_strength": 88,
})
```

### Configuration (`telemetry_config.toml`)

```toml
[devices.gnss]
mode = "production"
redis_prefix = "UBLOX_ZED-F9T_"
description = "Main GNSS Timing modules"

[devices.new_lidar]
mode = "experimental"
redis_prefix = "DEV_LIDAR_V1_"
ttl_seconds = 86400
```

### Adding a New Production Device

1. Define a Pydantic model in `src/panoseti_grpc/telemetry/config.py`:
    ```python
    class ChillerModel(BaseModel):
        water_temp: float
        flow_rate: float
        extra_data: dict[str, Any] | None = Field(default_factory=dict)
    ```
2. Add the device to `SCHEMA_MAP` in `config.py`.
3. Add a `[devices.chiller]` entry to `telemetry_config.toml` with `mode = "production"`.

---

## Architecture

### Log shipping pipeline

```
Python service (get_logger)
    ├── RichHandler → stdout
    ├── RotatingFileHandler → {service}.log
    ├── RotatingFileHandler → {service}.jsonl  ←── Grafana Alloy reads this
    └── AsyncGrpcHandler → Log RPC             ←── shadow path
                                │
                         TelemetryServicer
                                │
                         RedisBatcher → Redis logs:ingress list
                                │
                         storeLoki.py → Loki
```

**Resilience:** `AsyncGrpcHandler` uses a background worker thread and gRPC `wait_for_ready` semantics — logging never blocks or crashes the caller. When the gRPC path is unavailable, the `.jsonl` Alloy path continues unaffected.

**RedisBatcher:** Batches up to 100 `ReportStatus` / `Log` RPCs into a single Redis write. Integration tests must poll rather than use fixed `time.sleep` waits because of this flush latency.

---

## Running the Service

The Telemetry service runs as part of the unified server on the head node:

```bash
pseti-grpc server --profile headnode
```

Or standalone:
```bash
python -m panoseti_grpc.telemetry.server
panoseti-telemetry
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `HEADNODE_IP` | `localhost` | Host for the remote Telemetry gRPC endpoint |
| `HEADNODE_GRPC_PORT` | `50051` | Port for the remote Telemetry gRPC endpoint |
| `GRPC_PORT` | `50051` | Port this server listens on |

---

## Health Checking

```bash
pseti-grpc daqnode --log-dir /var/log/panoseti
grpc_health_probe -addr=headnode:50051 -service=telemetry.Telemetry
```

---

## Infrastructure (Loki + Grafana)

The Loki stack runs via Docker on the head node:

```bash
docker compose -f alloy/docker-compose.yml up -d
```

Loki runs as user ID `10001`. Set permissions before first start:
```bash
mkdir -p ./loki-data
sudo chown -R 10001:10001 ./loki-data
```

Verify: `http://HEADNODE_IP:3100/ready`

---

## Testing

```bash
python tests/qa.py telemetry
./scripts/run-ci-tests/run-telemetry-ci-test.sh
```

Tests require a running Redis instance (provided by Docker Compose). All assertions on Redis state must poll with a timeout — never use fixed `time.sleep` — because of `RedisBatcher` flush latency.

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| "Schema Violation" error | Payload doesn't match Pydantic model | Check `config.py` for the device schema |
| "Unregistered Type" warning | Device type not in TOML | Add to `telemetry_config.toml` |
| Data disappearing | Experimental type with short TTL | Check `ttl_seconds` in TOML |
| Logs missing from Loki | Alloy not reading `.jsonl` | Run `pseti-grpc daqnode`; check Alloy `/-/ready` |
