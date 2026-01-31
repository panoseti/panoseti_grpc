## Telemetry Service

The Telemetry Service is a high-throughput aggregation pipeline designed to ingest status updates, sensor readings, and health metrics from distributed PANOSETI components (U-blox, DAQ nodes, HK sensors) and store them for real-time monitoring and historical analysis.

### Architecture

The pipeline utilizes a **"Hot/Cold" storage strategy** to handle high-frequency updates without bottlenecking:

1. **Ingest (gRPC):** Clients send data via `log_strict` (schema-validated) or `log_flexible` (arbitrary JSON) calls.
2. **Processing (Server):**
* **Validation:** Strict payloads are validated against Pydantic models (e.g., `GnssModel`) to ensure data integrity.
* **Flattening:** Nested JSON objects are flattened (e.g., `{"gps": {"lat": 10}}`  `gps_lat: 10`) to map efficiently to Redis Hash fields.


3. **State Cache (Redis):** The "Hot" layer. Holds the *current* state of every device. This allows for extremely fast, O(1) lookups of the latest system status.
4. **Archival (InfluxDB):** The "Cold" layer. An asynchronous process (e.g., cron or background worker) snapshots Redis state into InfluxDB for time-series visualization (Grafana).

### Usage

**Client Example:**

```python
from panoseti_grpc.telemetry.client import TelemetryClient

client = TelemetryClient("localhost", 50051)

# 1. Strict Logging (Validated against schemas in config.py)
client.log_strict("gps", "dome_01", {
    "lat": 37.0,
    "lon": -121.0,
    "satellites": 8
})

# 2. Flexible Logging (Any dictionary)
client.log_flexible("weather_station", "roof_sensor", {
    "temp_c": 22.5,
    "humidity": 45.0,
    "status": "nominal"
})

```