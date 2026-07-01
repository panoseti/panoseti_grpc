# grpc_utils — Shared gRPC Client & Server Machinery

Shared, service-agnostic infrastructure used by every PANOSETI gRPC service.
Import anything you need from the package root:

```python
from panoseti_grpc.grpc_utils import grpc_call, PanosetiRpcError, UnavailableError
from panoseti_grpc.grpc_utils.channel import AsyncChannelManager
from panoseti_grpc.grpc_utils.health import register_health, HealthClient
from panoseti_grpc.grpc_utils.retries import build_retry_service_config
```

---

## Modules

| Module | Responsibility |
|---|---|
| `exceptions.py` | `PanosetiRpcError` base class + 8 typed subclasses; `from_rpc_error(e, target)` factory |
| `decorators.py` | `@grpc_call` — maps `grpc.RpcError → PanosetiRpcError`; supports async generators, coroutines, and sync calls |
| `channel.py` | `AsyncChannelManager` — owns channel lifecycle with keepalive options |
| `retries.py` | `build_retry_service_config()` — declarative retry policy JSON for the `service_config` channel option |
| `health.py` | `register_health(server, service_names)` and `HealthClient` wrapping `grpc.health.v1` |
| `interceptors.py` | Lightweight client/server interceptor stubs |

---

## Concurrency Decision Framework

When fanning out async calls across multiple DAQ nodes, choose the right pattern based on whether **one failure should abort the rest**.

### `asyncio.TaskGroup` — all-or-nothing fan-out

Use when **every task must succeed** or the whole operation should be abandoned:

- Startup sequences (`StartTransaction` DAQ node initialisation)
- Teardown rollback ladders (must attempt every rollback step)
- Manifest generation (every node must produce a manifest before transfer begins)
- Stream merge failures

```python
async with asyncio.TaskGroup() as tg:
    for node in daq_nodes:
        tg.create_task(start_one_node(node))
# First exception cancels siblings; ExceptionGroup raised at __aexit__
```

### Outcome-collection under `TaskGroup` — best-effort fan-out

Use when **tasks are independent and partial failure is tolerable**:

- Status probes (report per-node status even if some nodes are down)
- Cleanup attempts (try to clean all nodes; log failures; don't abort)
- Rollback stop-all (attempt every node; surface a summary of failures)

Each task captures its own exception and returns a typed outcome:

```python
from dataclasses import dataclass

@dataclass
class NodeOutcome:
    host: str
    ok: bool
    error: str = ""

async def stop_one_node(node) -> NodeOutcome:
    try:
        await client.stop(node)
        return NodeOutcome(host=node.host, ok=True)
    except Exception as exc:
        logger.warning("stop failed for %s: %s", node.host, exc)
        return NodeOutcome(host=node.host, ok=False, error=str(exc))

outcomes: list[NodeOutcome] = []
async with asyncio.TaskGroup() as tg:
    tasks = [tg.create_task(stop_one_node(n)) for n in daq_nodes]

outcomes = [t.result() for t in tasks]
failed = [o for o in outcomes if not o.ok]
if failed:
    logger.error("Stop failed on %d node(s): %s", len(failed), failed)
```

**Never** use `asyncio.gather(..., return_exceptions=True)` as an exception-swallower.
If you need `gather` for channel cleanup, log the exceptions explicitly:

```python
results = await asyncio.gather(*tasks, return_exceptions=True)
for r in results:
    if isinstance(r, BaseException):
        logger.error("cleanup error: %s", r)
```

### Summary table

| Pattern | Failure semantics | Use case |
|---|---|---|
| `TaskGroup` | First raise cancels siblings; `ExceptionGroup` at exit | Startup, manifest gen, all-or-nothing |
| Outcome-collection + `TaskGroup` | No raises; driver reduces outcomes | Stop, cleanup, probe, rollback |
| `asyncio.gather(return_exceptions=True)` + log | No raises; results inspected in-place | Final channel teardown only |
| `asyncio.gather(return_exceptions=True)` silently discarded | **Never** | — |

---

## Exception Hierarchy

```
PanosetiRpcError
├── UnavailableError          (UNAVAILABLE)
├── DeadlineExceededError     (DEADLINE_EXCEEDED)
├── ResourceExhaustedError    (RESOURCE_EXHAUSTED)
├── FailedPreconditionError   (FAILED_PRECONDITION)
├── NotFoundError             (NOT_FOUND)
├── AlreadyExistsError        (ALREADY_EXISTS)
├── InvalidArgumentError      (INVALID_ARGUMENT)
└── InternalError             (INTERNAL)
```

All subclasses carry `.code` (grpc.StatusCode), `.details` (str), and `.target` (str host:port).
Use `from_rpc_error(exc, target)` to convert a raw `grpc.RpcError`:

```python
from panoseti_grpc.grpc_utils.exceptions import from_rpc_error, FailedPreconditionError

try:
    await client.CleanupData(params)
except FailedPreconditionError as exc:
    logger.error("Cleanup refused — manifest digest mismatch: %s", exc.details)
```

---

## Health Checks (`grpc.health.v1`)

`register_health` is called automatically by `PanosetiServer.run()` after all services are added.
Every active service is marked `SERVING`. On the client side:

```python
from panoseti_grpc.grpc_utils.health import HealthClient

client = HealthClient(host="daqnode-1", port=50051)
alive = client.check("panoseti.daq_control")  # bool
```

This replaces the old `daq_data.Ping` RPC and the `StatusDaq`-as-heartbeat pattern.
Compatible with `grpc_health_probe`:

```bash
grpc_health_probe -addr=daqnode-1:50051 -service=panoseti.daq_control
```
