# ML Inference Service

The ML Inference service provides a real-time pub-sub broker for cloud-detection
predictions produced by the `pa-stream-cloud` streaming pipeline. It is a thin
pass-through: all ML logic lives in `panoseti_analysis`, not here.

**Status:** Beta (`feat/ml-inference-service` branch)  
**Proto package:** `panoseti.ml`  
**Source:** `src/panoseti_grpc/ml_inference/`

---

## Overview

```
pa-stream-cloud (Ray Serve on gaming node)
        │
        │  EmitPrediction (one call per 60-second window)
        ▼
MLInference servicer  ──── broadcaster ────► StreamPredictions subscribers
                      ──── alert filter ────► SubscribeAlerts subscribers (score ≥ threshold)
```

The servicer holds all subscriber queues in memory. Predictions are fanned out
immediately via `asyncio.Queue`; slow subscribers have their oldest item dropped
(backpressure policy: drop oldest, not block).

---

## Enabling the Service

Edit `grpc/src/panoseti_grpc/config/server.toml`:

```toml
[server.services]
ml_inference = true   # false by default

[ml_inference]
alert_threshold = 0.5     # cloud_score >= threshold → SubscribeAlerts fires
max_subscribers = 100     # max concurrent subscribers
subscriber_queue_size = 256
```

Then start the server:

```bash
pseti-grpc server                                # uses default server.toml
# or:
pseti-grpc server --config /path/to/custom.toml
```

The ML Inference service is designed to run alongside the DAQ Data service on the
head node (`digilab-receiver`). Enable both:

```toml
[server.services]
daq_data = true
ml_inference = true
```

---

## RPC Reference

| RPC | Direction | Description |
|-----|-----------|-------------|
| `EmitPrediction(Prediction) → EmitAck` | `pa-stream-cloud` → servicer | Push one scored 60-second window. Fans out to all `StreamPredictions` subscribers and fires `SubscribeAlerts` if `cloud_score >= alert_threshold`. |
| `StreamPredictions(PredictionRequest) → stream Prediction` | client ← servicer | Live stream of all scored windows. Filterable by `module_id` and `model_name`. |
| `SubscribeAlerts(AlertRequest) → stream Alert` | client ← servicer | Filtered stream: only windows with `cloud_score >= alert_threshold`. |

### `Prediction` message fields

| Field | Type | Description |
|-------|------|-------------|
| `module_id` | uint32 | PANOSETI detector module ID |
| `data_product` | string | e.g. `"img16"` |
| `model_name` | string | e.g. `"cloud_detector"` |
| `model_version` | string | e.g. `"1.0"` |
| `recipe_hash` | string | `"sha256:<hex>"` of the recipe YAML bytes |
| `git_sha` | string | git commit of the code that produced the prediction |
| `cloud_score` | double | ∈ [0, 1]; higher = more likely cloudy |
| `cloud_label` | bool | `cloud_score >= threshold` |
| `t_start_ns` | int64 | Window start, Unix nanoseconds |
| `t_end_ns` | int64 | Window end, Unix nanoseconds |
| `calibration_maturity` | float | 0.0 = cold start, 1.0 = fully warmed |
| `emitted_at` | Timestamp | When `EmitPrediction` was called |

### `Alert` message fields

Same as `Prediction` plus:

| Field | Type | Description |
|-------|------|-------------|
| `alert_threshold` | float | The threshold that triggered this alert |

---

## Client Usage

### Synchronous client

```python
from panoseti_grpc.ml_inference.client import MLInferenceClient

with MLInferenceClient(host="localhost", port=50051) as c:
    for prediction in c.stream_predictions():
        print(
            f"module={prediction.module_id}  "
            f"score={prediction.cloud_score:.3f}  "
            f"label={'CLOUDY' if prediction.cloud_label else 'clear'}  "
            f"maturity={prediction.calibration_maturity:.2f}"
        )
```

### Async client

```python
import asyncio
from panoseti_grpc.ml_inference.client import AioMLInferenceClient

async def main():
    async with AioMLInferenceClient() as c:
        async for prediction in c.stream_predictions():
            print(prediction.cloud_score)

asyncio.run(main())
```

### Subscribe to alerts only

```python
from panoseti_grpc.ml_inference.client import MLInferenceClient

with MLInferenceClient() as c:
    for alert in c.subscribe_alerts(threshold=0.7):
        print(f"ALERT: module={alert.module_id} score={alert.cloud_score:.3f}")
```

### Emit a prediction (from `pa-stream-cloud`)

```python
from panoseti_grpc.ml_inference.client import MLInferenceClient
from panoseti_grpc.generated import ml_inference_pb2 as pb

with MLInferenceClient() as c:
    ack = c.emit_prediction(pb.Prediction(
        module_id=1,
        data_product="img16",
        model_name="cloud_detector",
        model_version="1.0",
        cloud_score=0.87,
        cloud_label=True,
        t_start_ns=1700000000_000_000_000,
        t_end_ns=1700000060_000_000_000,
        calibration_maturity=1.0,
    ))
    print(ack.accepted, ack.n_subscribers)
```

---

## Integration with `pa-stream-cloud`

The streaming pipeline (`panoseti_analysis`) calls `EmitPrediction` after each
60-second window is scored. Enable the ML Inference service before starting `pa-stream-cloud`:

```bash
# 1. Start the gRPC server with ML inference enabled
pseti-grpc server  # (ml_inference = true in server.toml)

# 2. Start the streaming pipeline
pa-stream-cloud \
    --model-path assets/models/cloud_detector_v1.pt \
    --recipe ml/cloud-detection/recipes/stream_cloud_v1.yml \
    --grpc-host localhost

# 3. Subscribe from another terminal
python -c "
from panoseti_grpc.ml_inference.client import MLInferenceClient
with MLInferenceClient() as c:
    for p in c.stream_predictions():
        print(p.module_id, p.cloud_score, p.cloud_label)
"
```

---

## Seams (Future)

The proto defines seams for hardware integration (commented, unimplemented):

| RPC | Description |
|-----|-------------|
| `TriggerCapture` | Signal the DAQ system to start/stop recording based on an ML event |
| `MLInterrupt` | Pause inference while hardware reconfigures |
| `MountControl` | Command the telescope mount (e.g., close dome) on cloudy detection |

These are intended for the future "scope (c)" hardware-in-the-loop integration.

---

## Architecture Notes

- The servicer holds no state beyond the in-memory subscriber queues (no Redis, no DB).
  Predictions are ephemeral: a subscriber that connects after a window is scored will
  not receive it.
- The `SubscribeAlerts` threshold is configurable per-request (overrides the server-wide
  `alert_threshold` if provided).
- `EmitPrediction` is idempotent in effect: re-emitting the same prediction just fans
  it out again to current subscribers. There is no deduplication.
- The `calibration_maturity` field (0=cold, 1=warm) allows downstream consumers to
  discount early-night windows when the pedestal estimate is still converging.
