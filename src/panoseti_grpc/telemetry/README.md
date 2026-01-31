# Telemetry Service

The **PANOSETI Telemetry Service** is a high-throughput, distributed aggregation pipeline designed to ingest status updates, sensor readings, and health metrics from PANOSETI components (U-blox, DAQ nodes, HK sensors, etc.). It stores the latest state in **Redis** (Hot Store) for real-time control loops and archives historical data to **InfluxDB** (Cold Store) for analysis.

## 🚀 Key Features

* **Hot/Cold Storage Architecture:**
    * **Redis:** O(1) access to the *current* state of every device.
    * **InfluxDB:** Time-series history for dashboards (Grafana).
* **Hybrid Schema Strategy:**
    * **Production Mode:** Strictly validated schemas (Pydantic) for critical hardware.
    * **Experimental Mode:** Flexible JSON logging for R&D and rapid prototyping.
* **Automatic Hygiene:** Experimental data is auto-deleted (TTL) after 24 hours to prevent database pollution.
* **Observability:** Built-in latency tracking, rich logging, and CLI visualization tools.



---

## 🛠️ Configuration & Data Modes

The system's behavior is controlled by `telemetry_config.toml`. Every device type is categorized into one of two modes:

### 1. Production Mode (Strict)
* **Use Case:** Critical hardware (GNSS, Chillers, DAQ).
* **Validation:** Payloads MUST match the Pydantic schemas defined in `src/panoseti_grpc/telemetry/config.py`.
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

The service is typically run via Docker Compose, but can be run manually for development.

```bash
# Start Server (Default Port: 50051)
python -m panoseti_grpc.telemetry.server

# Start Archiver (Bridges Redis -> InfluxDB)
python -m panoseti_grpc.telemetry.archiver

```

### 2. Using the Client Library

The Python client automatically handles the nuances of the underlying gRPC protocol.

```python
from panoseti_grpc.telemetry.client import TelemetryClient

client = TelemetryClient("localhost", 50051)

# --- SCENARIO A: Logging Production Data ---
# Must match the schema for 'gnss' (lat, lon, satellites, etc.)
client.log_strict("gnss", "dome_01", {
    "lat": 37.3382,
    "lon": -121.8863,
    "satellites": 12,
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

## 👨‍💻 Developer Guide

### Adding a New Production Device

1. **Define Schema:** Add a Pydantic model to `src/panoseti_grpc/telemetry/config.py`.
```python
class ChillerModel(BaseModel):
    water_temp: float
    flow_rate: float
    extra_data: Optional[Dict[str, Any]] = {}

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

### Troubleshooting

* **"Schema Violation" Error:** You are sending data to a production device that doesn't match its Pydantic model. Check `config.py`.
* **"Unregistered Type" Warning:** The server received a device type not in the TOML file. It falls back to the `SANDBOX` namespace.
* **Data Disappearing?** Check if you are using an Experimental type. Data auto-expires based on the `ttl_seconds` setting.

### Observability

* **Real-time Monitor:** Run `redis-cli monitor` to see raw commands.
* **Logs:** The server uses `rich` logging. Set `LOG_LEVEL=DEBUG` to see every payload and its processing time.
