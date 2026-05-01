# DaqData v2 Service (Next-Gen Aggregator)

## Architecture Overview
DaqData v2 adopts a "Push Forwarder" model to minimize resource consumption on DAQ nodes and optimize network bandwidth.

```
Hashpipe (DAQ Node)
    │ (UDS)
    ▼
Forwarder (DAQ Node Script)
    │ (gRPC UploadImages - Push)
    ▼
DaqDataV2 Aggregator (Headnode)
    │ (gRPC StreamImages - Pub/Sub)
    ▼
End-User Clients
```

### Key Differences from v1
- **No `InitHpIo`:** The lifecycle is managed by the system (or `DaqControl`). No explicit initialization is required by the client.
- **Centralized:** Clients connect to a single Headnode IP for all modules, rather than multiple DAQ node IPs.
- **Lightweight DAQ Nodes:** DAQ nodes no longer host a gRPC server for data; they run a simple, robust forwarder.

## Deployment

### On DAQ Nodes
The `DaqControl` service handles starting the forwarder. When Hashpipe is started via `DaqControl.StartDaq`, set `enable_v2_forwarder=True` and provide the `headnode_target`.

### On Headnode
Enable `daq_data_v2` in the unified server configuration (`server.toml`):

```toml
[server.services]
daq_data_v2 = true

[daq_data_v2]
log_level = "INFO"
```

## Client Usage

Use the simplified `AioDaqDataV2Client`:

```python
from panoseti_grpc.daq_data_v2.client import AioDaqDataV2Client

async with AioDaqDataV2Client("headnode:50051") as client:
    async for response in client.stream_images(update_interval=1.0):
        img = response.pano_image
        print(f"Received module {img.module_id} frame {img.frame_number}")
```

## Development & Testing

A standalone simulator is provided for testing the pipeline without live hardware:

```bash
# Run simulator (acts as Hashpipe)
python -m panoseti_grpc.daq_data_v2.simulator

# Run forwarder (pushes to headnode)
python -m panoseti_grpc.daq_data_v2.forwarder --headnode localhost:50051
```
