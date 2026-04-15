# Telemetry Service

The **PANOSETI Telemetry Service** is a high-throughput, distributed aggregation pipeline designed to ingest status updates, sensor readings, and health metrics from PANOSETI components (U-blox, DAQ nodes, HK sensors, etc.). It stores the latest state in **Redis** (Hot Store) for real-time control loops and archives historical data to **InfluxDB** (Cold Store) for analysis.

## 🚀 Key Features

* **Centralized Logging (Loki):**
  * Async gRPC logging handler that ships logs from all nodes to a central **Loki** instance.
    * Automatic metadata injection (Git Commit, PID, Thread, Hostname).
    * Non-blocking architecture to ensure logging never crashes observations.
* **Observability:** Built-in latency tracking, rich logging, and CLI visualization tools.
* **Hybrid Schema Strategy:**
    * **Production Mode:** Strictly validated schemas (Pydantic 2.x) for critical hardware.
    * **Experimental Mode:** Flexible JSON logging for R&D and rapid prototyping.
* **Hot/Cold Storage Architecture:**
  * **Redis:** O(1) access to the *current* state of every device.
  * **InfluxDB:** Time-series history for dashboards (Grafana).
  * **Loki:** Log database for log dashboards (Grafana).
* **Automatic Hygiene:** Experimental data is auto-deleted (TTL) after 24 hours to prevent database pollution.


---

## 🛠️ Configuration & Data Modes

The system's behavior is controlled by [telemetry/telemetry_config.toml](telemetry_config.toml). Every device type is categorized into one of two modes:

### 1. Production Mode (Strict)
* **Use Case:** Established/Important hardware (GNSS, White Rabbit, etc.).
* **Validation:** Payloads MUST match the Pydantic schemas defined in [telemetry/config.py](config.py).
* **Storage:** Permanent (TTL = 0).
* **Naming:** Redis keys use a specific hardware prefix (e.g., `UBLOX_ZED-F9T_`).

### 2. Experimental Mode (Flexible)
* **Use Case:** Debugging, new sensor prototyping, one-off scripts.
* **Validation:** None. You can send any JSON structure.
* **Storage:** Temporary. Data expires automatically (default: 24h).
* **Naming:** Redis keys MUST start with `DEV_` to clearly indicate their volatile nature.

**Example `telemetry_config.toml`:**
```toml
# --- PRODUCTION ---
[devices.gnss]
mode = "production"
redis_prefix = "UBLOX_ZED-F9T_"
description = "Main GNSS Timing modules"

# --- EXPERIMENTAL ---
[devices.new_lidar]
mode = "experimental"
redis_prefix = "DEV_LIDAR_V1_"
ttl_seconds = 86400  # Auto-delete after 1 day

```

---

## 💻 Usage

### 1. Running the Service

The service should be run as a module on the Headnode. It will automatically connect to the Redis server.

```bash
# Start Server (Default Port: 50051)
python -m panoseti_grpc.telemetry.server
```

### 2. Using the Client API: `TelemetryClient`

`TelemetryClient` automatically handles the nuances of the underlying gRPC protocol.

```python
from panoseti_grpc.telemetry.client import TelemetryClient

client = TelemetryClient("localhost", 50051)

# --- SCENARIO A: Logging Production Data ---
# Must match the schema for 'gnss' (lat, lon, satellites, etc.)
client.log_strict("gnss", "dome_01", {
    "satellites": 12,
    "lat": 37.3382,
    "lon": -121.8863,
    "fix_mode": "3D",
    # You can add extra fields safely via 'extra_data' without breaking schema
    "extra_data": {"dilution": 1.5}
})

# --- SCENARIO B: Rapid Prototyping (R&D) ---
# Send any dict. It will be stored in Redis with a 24h TTL.
# Device type 'new_lidar' must be defined in TOML as 'experimental'.
client.log_flexible("new_lidar", "prototype_A", {
    "distance_mm": 4502,
    "signal_strength": 88,
    "status": "calibrating"
})

```

### 3. CLI Tools

The package includes a powerful CLI for generating load, testing connectivity, and visualizing latency.

```bash
# Generate mixed traffic (Production + Experimental)
python -m panoseti_grpc.telemetry.cli --type mixed --count 1000

# Test strictly typed GNSS messages
python -m panoseti_grpc.telemetry.cli --type gnss --delay 0.1

```

---

## 📜 Centralized Logging (Loki)

The Telemetry Service provides a high-performance logging pipeline. Instead of scattering text logs across 6 different distributed nodes, logs are shipped via gRPC to the Headnode, cached in Redis, and indexed by **Loki** for real-time analysis in Grafana.

### 👨‍💻 Client Usage (Python)

Do not manually construct gRPC messages. Use the provided helper function to attach the centralized logger to your existing scripts.

**Basic Integration:**

```python
import logging
from panoseti_grpc.telemetry.logger import get_logger

# Run this ONCE at startup. 
# It attaches the gRPC handler to the Root Logger, capturing everything.
logger = get_logger(
    service_name="Dome_Control", 
    level=logging.INFO,
    grpc_enabled=True,
    console=True
)

# Use standard logging as usual - it now goes to Loki!
logger.info("Dome rotation started")

# Exceptions are automatically formatted and shipped
try:
    x = 1 / 0
except Exception:
    logger.exception("Critical math failure")

```

**Structured Logging (Best Practice):**
Pass a dictionary in the `extra` field. This becomes queryable JSON in Grafana.

```python
# In Python
logger.info("Filter wheel moved", extra={"position": 2, "temp_c": 12.5})

# In Loki Query
# {service="Dome_Control"} | json | position = 2

```

### 🔍 Metadata & Context

The system automatically enriches your logs with context to help debug distributed failures:

* **`host`**: Which machine produced the log (e.g., `node-04`).
* **`git_commit`**: The exact software version running (from `git`).
* **`process_id` / `thread_name**`: Identifies specific runtime instances.
* **`trace_id`**: Correlates actions across services (if provided).


### 🐳 Infrastructure Setup

The logging stack (Loki + Grafana) runs via Docker on the Headnode.
The relevant Loki docker-compose file is [docker-compose.loki.yml](docker-compose.loki.yml)

**1. Prepare Data Directory**
Loki runs as user ID `10001`. You **must** set permissions correctly or the container will fail to start.

```bash
# Create directory and set ownership to the container user
mkdir -p ./loki-data
sudo chown -R 10001:10001 ./loki-data

```

**2. Start the Stack**

```bash
docker compose -f docker-compose.loki.yml up -d

```

**3. Verify Status**
Visit `http://HEADNODE_IP:3100/ready` to check if Loki is accepting logs.

---

### Architecture

1. **Client:** Python loggers use an `AsyncGrpcHandler` to ship logs + metadata to the Headnode.
   * *Resilience:* Uses a background worker thread and gRPC `wait_for_ready` semantics. If the server is down, logs are buffered locally or queued in the gRPC channel until connectivity restores.

2. **Server:** Validates schema and pushes logs to a Redis List (`logs:ingress`) using an async batcher (N log entries = 1 Redis write).
3. **Storage:** A worker (`storeLoki.py`) consumes from Redis and pushes to Loki, indexing metadata (Trace ID, Host) while storing the payload compressed.

---

## 👨‍💻 Developer Guide

### Adding a New Production Device

1. **Define Schema:** Add a Pydantic model to [src/panoseti_grpc/telemetry/config.py](config.py).
```python
class ChillerModel(BaseModel):
    water_temp: float
    flow_rate: float
    # Use Pydantic 2.x Field with default_factory for dicts
    extra_data: dict[str, Any] | None = Field(default_factory=dict)

```


2. **Register:** Add the mapping to `SCHEMA_MAP` in `config.py`.
3. **Configure:** Add the entry to `telemetry_config.toml` with `mode = "production"`.

### Developing with "Flexible" Mode

1. **Configure:** Add your new type to `telemetry_config.toml`:
```toml
[devices.my_test]
mode = "experimental"
redis_prefix = "DEV_TEST_"
```


2. **Code:** Use `client.log_flexible("my_test", ...)` immediately. No server restart required if you are just adding data.

### Testing Logging

You can simulate a stream of realistic telescope logs (errors, warnings, varying components) to verify the pipeline:

```bash
# fast log stream
python -m panoseti_grpc.telemetry.cli --type log --delay 0.01

# slow heartbeat
python -m panoseti_grpc.telemetry.cli --type log --delay 1.0

```

### Troubleshooting

* **"Schema Violation" Error:** You are sending data to a production device that doesn't match its Pydantic model. Check `config.py`.
* **"Unregistered Type" Warning:** The server received a device type not in the TOML file. It falls back to the `SANDBOX` namespace.
* **Data Disappearing?** Check if you are using an Experimental type. Data auto-expires based on the `ttl_seconds` setting.

### Observability

* **Real-time Monitor:** Run `redis-cli monitor` to see raw commands.
* **Logs:** The server uses `rich` logging. Set `LOG_LEVEL=DEBUG` to see every payload and its processing time.
