# Plan: daq_data Service Quality Improvement

## Context

A thorough audit of the `daq_data` gRPC service (`src/panoseti_grpc/daq_data/`) and the upstream Hashpipe snapshot C code (`panoseti_daq_ssh/snapshot.c`, `net_thread.c`) identified correctness bugs, absent config validation, and several performance issues. The user also requested: adopting the shared PANOSETI telemetry logger, a performance analysis, a module-count review, documentation updates, and an expanded test suite.

---

## Part 0 — Architecture Assessment: UDS-Only Is Correct

Hashpipe **always** runs co-located with the daq_data gRPC server on the same DAQ node. The snapshot C code uses non-blocking `writev()` and drops frames on a full socket buffer — correct for a surveillance workload where frame loss is better than blocking the DAQ pipeline. All alternatives (TCP loopback, named pipes, shared memory, POSIX MQs) are worse for this use case.

**One gap**: sockets created with `os.chmod(0o777)` (world-writable). Should be `0o600`.

**Module count assessment**: The current 7-module layout (`server.py`, `client.py`, `data_sources.py`, `hp_io_manager.py`, `managers.py`, `state.py`, `resources.py`, `simulate.py`) is appropriate for the functionality. No consolidation needed. One new file (`config.py`) will be added for Pydantic models.

---

## Part 1 — Replace `make_rich_logger` with Shared Telemetry Logger

### Problem
`resources.py` contains a custom `make_rich_logger()` with a single-date `FileHandler` and no gRPC remote logging. The telemetry service already provides `get_logger()` (`telemetry/logger.py`) — a production-grade 3-in-1 factory (console, rotating filesystem, async gRPC to Loki) with Pydantic-validated config, shared gRPC client pooling, and auto-injected metadata (Git commit, hostname, PID).

### Change
**Remove** `make_rich_logger()` from `resources.py`.

**Replace all usages** with `from panoseti_grpc.telemetry.logger import get_logger`:

```python
# server.py — DaqDataServicer.__init__
self.logger = get_logger(
    "daq_data.server",
    level=logging_level,
    log_dir=server_cfg.log_dir,           # new field in DaqDataServerConfig
    grpc_enabled=server_cfg.grpc_logging, # new field, default True
)
```

For the `serve()` function's pre-servicer logger:
```python
logger = get_logger("daq_data.serve", console=True)
```

The telemetry logger's `grpc_enabled=True` path is non-blocking and fails gracefully (`fail_fast=False` default) when the telemetry server is down.

**Files to modify**: `resources.py` (remove `make_rich_logger`), `server.py`, and anywhere else `make_rich_logger` is imported.

**Config additions** (in `DaqDataServerConfig`, Part 3):
```python
log_dir: str | None = None           # None = file logging disabled
grpc_logging: bool = True            # Send logs to telemetry server
```

---

## Part 2 — Performance Analysis & Real-Time Bug Fixes

### 2A — DEAD CODE: `asyncio.TimeoutError` in `StreamImages` — `server.py:101`

**Problem**: The `except asyncio.TimeoutError` block at line 101 can never execute because `asyncio.sleep()` does not raise `TimeoutError`. The `reader_state.dequeue_timeouts` counter therefore never increments. The idle-stream abort (DEADLINE_EXCEEDED) is **permanently disabled**.

**Fix**: Replace with an explicit empty-frame counter. After each sleep, if no fresh data was sent, increment the counter:

```python
# After sleep:
if not fresh_images:
    reader_state.dequeue_timeouts += 1
    if reader_state.dequeue_timeouts >= self.server_cfg.max_reader_dequeue_timeouts:
        await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "No data received within timeout window.")
        return
else:
    reader_state.dequeue_timeouts = 0
```

Remove the entire `except asyncio.TimeoutError` block.

### 2B — CORRECTNESS: Missing `asyncio.TimeoutError` in `_handle_client` — `data_sources.py:146`

**Problem**: `asyncio.wait_for(reader.readexactly(2), self.read_timeout)` raises `asyncio.TimeoutError` when Hashpipe is idle for `read_timeout` seconds (60s default). Hashpipe has a 15s idle timeout (`UDS_CONNECTION_TIMEOUT_US`), so this happens legitimately during observation gaps. The exception propagates to `run()` which catches it as a generic `Exception` and exits the handler — the data source then sits idle until the server restarts.

**Fix**: Catch `asyncio.TimeoutError` and log + return from the handler (the server will re-accept on the next Hashpipe reconnect):

```python
except asyncio.TimeoutError:
    self.logger.info(
        f"Read timeout on {self.socket_path} (>{self.read_timeout}s idle). "
        "Closing; Hashpipe will reconnect on next frame."
    )
```

### 2C — ROBUSTNESS: `listen(1)` → `listen(5)` — `data_sources.py:109`

With `listen(1)`, Hashpipe reconnects during the brief window when the old handler has not yet returned could be refused. `listen(5)` adds a connection queue at no cost.

### 2D — PERFORMANCE: `_cache_pano_image` is `async def` with no awaits — `hp_io_manager.py:132`

```python
async def _cache_pano_image(self, cached_image: CachedPanoImage):
    ...  # no awaits inside
    self.latest_data_cache[pano_image.module_id][cache_key] = cached_image
```

Calling an `async def` with no `await` inside creates a coroutine object and schedules it via the event loop — unnecessary overhead per frame. At 100 Hz × 4 data products = 400 frames/second, this creates 400 coroutine objects/second for no reason.

**Fix**: Change to `def _cache_pano_image(...)` and call it directly (no `await`).

### 2E — PERFORMANCE: `StreamImages` sleep does not account for processing time — `server.py:99`

```python
await asyncio.sleep(interval)  # sleeps for a FULL interval regardless of how long processing took
```

At high update rates (e.g., 100 Hz = 10ms interval), the time spent in `_get_fresh_images_for_client` and `yield` reduces the effective rate. Fix with:

```python
elapsed = time.monotonic() - now
await asyncio.sleep(max(0.0, interval - elapsed))
```

### 2F — PERFORMANCE: Loop condition creates a list per iteration — `server.py:85`

```python
while not any([context.cancelled(), ...]):  # creates a list every iteration
```

**Fix**:
```python
while not (context.cancelled() or reader_state.cancel_reader_event.is_set() or reader_state.shutdown_event.is_set()):
```

### 2G — PERFORMANCE: `data_queue.task_done()` called without `join()` — `hp_io_manager.py:125`

`task_done()` is only meaningful when paired with `queue.join()`. Since `join()` is never awaited, `task_done()` is a no-op that adds overhead per frame. Remove it.

### 2H — PERFORMANCE: `_get_fresh_images_for_client` iterates all cached modules — `server.py:129`

When a client specifies a `module_ids` whitelist, the current code still iterates all modules in `cache.items()`, skipping non-subscribed ones in the inner loop. With many modules, this wastes cycles.

**Fix**: Iterate only subscribed modules when a whitelist is set:

```python
def _get_fresh_images_for_client(self, rs: ReaderState) -> list[PanoImage]:
    if not self.task_manager.hp_io_manager:
        return []
    cache = self.task_manager.hp_io_manager.latest_data_cache
    subscribed = set(rs.config['module_ids'])
    module_ids = subscribed if subscribed else cache.keys()
    images = []
    for mid in module_ids:
        data = cache.get(mid)  # use .get() instead of defaultdict trigger
        if data is None:
            continue
        if rs.config['stream_movie_data']:
            cached_movie = data.get('movie')
            if cached_movie and cached_movie.frame_id > rs.last_sent_movie_id:
                images.append(cached_movie.pano_image)
                rs.last_sent_movie_id = cached_movie.frame_id
        if rs.config['stream_pulse_height_data']:
            cached_ph = data.get('ph')
            if cached_ph and cached_ph.frame_id > rs.last_sent_ph_id:
                images.append(cached_ph.pano_image)
                rs.last_sent_ph_id = cached_ph.frame_id
    return images
```

Note the `cache.get(mid)` — avoids triggering `defaultdict`'s factory for non-existent entries.

### 2I — SECURITY: Socket world-writable — `data_sources.py:108`

`os.chmod(self.socket_path, 0o777)` → `os.chmod(self.socket_path, 0o600)`

Only Hashpipe (same user) needs access.

### 2J — PERFORMANCE: `asyncio.wait_for` per read creates tasks — `data_sources.py:155-170`

Each call to `asyncio.wait_for(coro, timeout)` internally creates a Task. With 4 data products × 3 reads/frame × 100 Hz = 1200 task creations/second. On Python 3.11+, `asyncio.timeout()` context manager is more efficient. Since we target Python 3.9+, we should wrap all reads for a single frame in one `wait_for`:

```python
async with asyncio.timeout(self.read_timeout):   # Python 3.11+
    module_id_bytes = await reader.readexactly(2)
    header_with_sep = await reader.readuntil(b'\n\n') if header_size is None else await reader.readexactly(header_size)
    img_data = await reader.readexactly(1 + self.dp_config.bytes_per_image)
```

For Python 3.9 compatibility, use a single outer `wait_for` with an inner coroutine:

```python
async def _read_one_frame(reader, header_size):
    module_id_bytes = await reader.readexactly(2)
    ...
    return module_id, header_bytes, img_data

frame_data = await asyncio.wait_for(_read_one_frame(reader, header_size), self.read_timeout)
```

This reduces task overhead from 3 tasks/frame to 1.

---

## Part 3 — `DataProduct` Enum

Add to `state.py`:

```python
from enum import Enum

class DataProduct(str, Enum):
    IMG16  = "img16"
    IMG8   = "img8"
    PH256  = "ph256"
    PH1024 = "ph1024"

    @property
    def image_shape(self) -> tuple[int, int]:
        return (16, 16) if self == DataProduct.PH256 else (32, 32)

    @property
    def bytes_per_pixel(self) -> int:
        return 1 if self == DataProduct.IMG8 else 2

    @property
    def is_ph(self) -> bool:
        return self in (DataProduct.PH256, DataProduct.PH1024)

    @property
    def bytes_per_image(self) -> int:
        r, c = self.image_shape
        return r * c * self.bytes_per_pixel
```

`DataProduct(str, Enum)` means `DataProduct.IMG16 == "img16"` → backward-compatible.

`get_dp_config()` validates via `DataProduct(dp)` (raises `ValueError` on unknown name). `get_dp_name_from_props()` in `resources.py` is rewritten to iterate `DataProduct` members instead of nested `if/elif`.

Remove unused `ReaderState.queue` and `enqueue_timeouts` fields (the pub/sub design uses `latest_data_cache` polling, not per-reader queues).

---

## Part 4 — Pydantic Config Validation

Create `src/panoseti_grpc/daq_data/config.py`:

```python
from pydantic import BaseModel, Field, field_validator
from pathlib import Path

class UdsAcquisitionConfig(BaseModel):
    enabled: bool = True
    data_products: list[str] = ["img8", "img16", "ph256", "ph1024"]
    socket_path_template: str = "/tmp/hashpipe_grpc.dp_{dp_name}.sock"
    read_timeout: float = Field(60.0, gt=0)

    @field_validator("socket_path_template")
    @classmethod
    def must_have_placeholder(cls, v):
        if "{dp_name}" not in v:
            raise ValueError("socket_path_template must contain '{dp_name}'")
        return v

    @field_validator("data_products")
    @classmethod
    def valid_dp_names(cls, v):
        from .state import DataProduct
        for dp in v:
            DataProduct(dp)  # raises ValueError on unknown name
        return v

class SimSourceDataConfig(BaseModel):
    real_module_id: int
    movie_pff_path: str
    ph_pff_path: str

class UdsSimStrategyConfig(BaseModel):
    data_products: list[str] = ["ph256", "img16"]

class SimulateDaqConfig(BaseModel):
    simulation_mode: str = "uds"
    sim_module_ids: list[int]
    movie_type: str = "img16"
    ph_type: str = "ph256"
    source_data: SimSourceDataConfig
    strategies: dict[str, UdsSimStrategyConfig]

class AcquisitionMethodsConfig(BaseModel):
    uds: UdsAcquisitionConfig = Field(default_factory=UdsAcquisitionConfig)

class DaqDataServerConfig(BaseModel):
    init_from_default: bool = False
    default_hp_io_config_file: str = "hp_io_config_simulate.json"
    unix_domain_socket: str | None = None
    max_concurrent_rpcs: int = Field(100, ge=1)
    max_read_queue_size: int = Field(50, ge=1)
    min_hp_io_update_interval_seconds: float = Field(0.001, gt=0)
    max_client_update_interval_seconds: float = Field(60.0, gt=0)
    max_reader_enqueue_timeouts: int = Field(2, ge=1)
    max_reader_dequeue_timeouts: int = Field(3, ge=1)
    reader_timeout: float = Field(5.0, gt=0)
    shutdown_grace_period: float = Field(5.0, ge=0)
    hp_io_stop_timeout: float = Field(5.0, gt=0)
    valid_data_products: list[str] = ["img8", "img16", "ph256", "ph1024"]
    acquisition_methods: AcquisitionMethodsConfig = Field(default_factory=AcquisitionMethodsConfig)
    simulate_daq_cfg: SimulateDaqConfig | None = None
    # Logging
    log_dir: str | None = None
    grpc_logging: bool = True
```

In `server.py __main__`:
```python
from .config import DaqDataServerConfig
raw = load_package_json(...)
server_cfg = DaqDataServerConfig.model_validate(raw)
```

Replace all `server_cfg['key']` dict accesses with `server_cfg.key` attribute access throughout `server.py`, `managers.py`, `hp_io_manager.py`.

---

## Part 5 — Modern Python Type Hints

In all files replace:
- `from typing import Dict, List, Optional, Tuple` → use `dict[...]`, `list[...]`, `T | None`, `tuple[...]`
- `Optional[T]` → `T | None`
- `Dict[K, V]` → `dict[K, V]`

Target files: `state.py`, `managers.py`, `hp_io_manager.py`, `data_sources.py`, `resources.py`, `simulate.py`.

---

## Part 6 — Documentation Update

Update `src/panoseti_grpc/daq_data/README.md`:
- Replace server config JSON example to reflect new `log_dir` / `grpc_logging` fields
- Add "Logging" section: explains `get_logger()` integration and how to configure the three log destinations
- Add "Performance Notes" section: documents the pub/sub polling model, update rate limits, and frame-dropping semantics
- Update "Wire Protocol" section: document the exact frame format from `snapshot.c` (`[2-byte module_id][JSON]\n\n*[binary]`)
- Add "Configuration Reference" section with description of every field in `DaqDataServerConfig` (auto-validate on startup)

---

## Part 7 — Tests

### 7A — Unit tests for Pydantic config (`tests/daq_data/unit/test_config.py`)

```python
def test_missing_required_field_raises():
    with pytest.raises(ValidationError):
        DaqDataServerConfig.model_validate({})  # missing nothing (all have defaults) —
        # but test with bad values:

def test_negative_max_concurrent_rpcs_raises():
    with pytest.raises(ValidationError):
        DaqDataServerConfig.model_validate({"max_concurrent_rpcs": 0})

def test_socket_path_template_without_placeholder_raises():
    with pytest.raises(ValidationError):
        UdsAcquisitionConfig(socket_path_template="/tmp/no_placeholder.sock")

def test_unknown_data_product_raises():
    with pytest.raises(ValidationError):
        UdsAcquisitionConfig(data_products=["img16", "bad_product"])

def test_full_valid_config_parses(server_config_base):
    cfg = DaqDataServerConfig.model_validate(server_config_base)
    assert cfg.max_concurrent_rpcs == 100
    assert cfg.acquisition_methods.uds.enabled is True

def test_data_product_enum_validates():
    assert DataProduct("img16") == DataProduct.IMG16
    assert DataProduct.IMG16.image_shape == (32, 32)
    assert DataProduct.PH256.is_ph is True
    assert DataProduct.PH1024.bytes_per_image == 32 * 32 * 2
    with pytest.raises(ValueError):
        DataProduct("bad_dp")
```

### 7B — gRPC server isolation tests (`tests/daq_data/integration/test_server_isolation.py`)

These tests use the UDS simulation (simulated Hashpipe) without any real hardware, testing the gRPC server logic directly:

```python
# Test: DEADLINE_EXCEEDED abort fires when no fresh data arrives
async def test_idle_stream_aborts_with_deadline_exceeded(sim_server_process):
    """After stream starts and simulation stops, stream must DEADLINE_EXCEEDED."""
    # Start server in sim mode; receive first frame; stop simulation;
    # assert stream aborts with DEADLINE_EXCEEDED within dequeue_timeout window.

# Test: concurrent readers see same frame_ids
async def test_two_concurrent_readers_same_frame_ids(sim_server_process):
    """Two simultaneous StreamImages calls see the same frame IDs (±2 tolerance)."""

# Test: InitHpIo with force=True cancels active readers
async def test_force_reinit_cancels_readers(sim_server_process):
    """Force re-init while readers are active; all readers get CANCELLED."""

# Test: StreamImages fails FAILED_PRECONDITION before InitHpIo
async def test_stream_fails_before_init(default_server_process):
    """Already exists in test_server_logic.py — verify it stays passing."""

# Test: InitHpIo without force fails while readers are active
async def test_init_without_force_fails_with_active_readers(sim_server_process):

# Test: StreamImages with module_ids whitelist filters correctly
async def test_stream_module_id_whitelist(sim_server_process):
    """With module_ids=[224], stream only delivers frames from module 224."""

# Test: Ping succeeds before and after InitHpIo
async def test_ping_always_succeeds(default_server_process):

# Test: Max concurrent readers enforced
async def test_max_concurrent_readers_rejected(sim_server_process):
    """After filling reader slots, next StreamImages gets RESOURCE_EXHAUSTED."""

# Test: frame_id is monotonically increasing
async def test_frame_ids_are_monotonic(sim_server_process):
    """Collect 20 frames and assert frame_id strictly increases."""
```

### 7C — Extended Hashpipe CI integration tests (`tests/daq_data_hashpipe/integration/`)

Add to `test_real_daq_robustness.py` or new `test_daq_pipeline_e2e.py`:

```python
@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_frame_content_validates_shape_and_dtype(default_server_process):
    """Frames from real Hashpipe have expected shape, dtype, and non-zero data."""
    # Init, get 5 frames, assert each:
    # - img16 frames: shape=(32,32), dtype=uint16
    # - ph256 frames: shape=(16,16), dtype=int16
    # - header fields present: pkt_num, pkt_tai, pkt_nsec, tv_sec, tv_usec
    # - at least one frame has non-zero pixel values

@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_frame_rate_meets_snapshot_interval(default_server_process):
    """Frames arrive at roughly ssint=100ms rate (±50% tolerance)."""
    # Collect 10 frames; compare arrival timestamps; assert mean interval near 0.1s

@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_uds_reconnect_after_server_restart(hashpipe_pcap_runner, docker_compose_services):
    """Stop and restart the gRPC server; Hashpipe reconnects automatically within 20s."""
    # 1. Get frames from initial server
    # 2. Kill gRPC server process
    # 3. Restart gRPC server
    # 4. Init again; assert frames resume within 20s (Hashpipe idle timeout = 15s)

@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_frame_continuity_across_output_block_boundary(default_server_process):
    """Frame IDs don't skip unexpectedly when Hashpipe rolls to a new output buffer block."""
    # Collect 100 frames; assert no gap > 2 in frame_id (scheduling jitter tolerance)
```

### 7D — Unit tests for `DataProduct` enum (`tests/daq_data/unit/test_resources.py`)

Add to existing `test_resources.py`:
```python
def test_get_dp_name_from_props_all_products():
    assert get_dp_name_from_props(PanoImage.Type.MOVIE, [32, 32], 2) == "img16"
    assert get_dp_name_from_props(PanoImage.Type.MOVIE, [32, 32], 1) == "img8"
    assert get_dp_name_from_props(PanoImage.Type.PULSE_HEIGHT, [16, 16], 2) == "ph256"
    assert get_dp_name_from_props(PanoImage.Type.PULSE_HEIGHT, [32, 32], 2) == "ph1024"

def test_get_dp_name_from_props_unknown_raises():
    with pytest.raises(ValueError):
        get_dp_name_from_props(PanoImage.Type.MOVIE, [8, 8], 2)
```

---

## Implementation Order

1. **Part 3** — `DataProduct` enum (enables Parts 2H and 4)
2. **Part 2A** — Fix `StreamImages` dequeue timeout (correctness)
3. **Part 2B** — Add `asyncio.TimeoutError` to `_handle_client` (correctness)
4. **Part 2C/D/G/I** — Quick mechanical fixes (listen(5), sync cache fn, remove task_done, socket perms)
5. **Part 2E/F/H/J** — Performance improvements (sleep compensation, loop condition, module filter, wait_for reduction)
6. **Part 1** — Replace `make_rich_logger` with `get_logger` (isolated change)
7. **Part 4** — Pydantic `DaqDataServerConfig` (larger, wire through all files)
8. **Part 5** — Type hint modernization (bulk sed-like replacements)
9. **Part 6** — Documentation update
10. **Part 7** — Tests

---

## Verification

```bash
# After each batch of changes:
./scripts/run-ci-tests/run-daq-data-ci-test.sh          # must stay at 0 failures
pytest tests/daq_data/ -q --tb=short                    # in-process tests
pytest tests/daq_data_hashpipe/ -q --tb=short           # hashpipe CI (Docker required)

# Verify startup with new Pydantic config:
python -m panoseti_grpc.daq_data.server
```

---

## Files to Modify

| File | Change type |
|---|---|
| `src/panoseti_grpc/daq_data/config.py` | **New** — Pydantic config models |
| `src/panoseti_grpc/daq_data/state.py` | Add `DataProduct` enum; rewrite `get_dp_config()`; remove unused `ReaderState.queue`/`enqueue_timeouts` |
| `src/panoseti_grpc/daq_data/resources.py` | Remove `make_rich_logger`; fix `get_dp_name_from_props()`; modernize types |
| `src/panoseti_grpc/daq_data/server.py` | Fix `StreamImages` timeout/sleep/loop; use `get_logger`; use Pydantic config |
| `src/panoseti_grpc/daq_data/data_sources.py` | Fix `TimeoutError`; `listen(5)`; `0o600`; batched `wait_for` |
| `src/panoseti_grpc/daq_data/hp_io_manager.py` | `def _cache_pano_image`; remove `task_done`; Pydantic config |
| `src/panoseti_grpc/daq_data/managers.py` | Pydantic config (typed attributes); modern type hints |
| `src/panoseti_grpc/daq_data/simulate.py` | Modern type hints |
| `src/panoseti_grpc/daq_data/README.md` | Full update (logging, perf notes, wire protocol, config reference) |
| `tests/daq_data/unit/test_config.py` | **New** — Pydantic + DataProduct enum unit tests |
| `tests/daq_data/unit/test_resources.py` | Extend with `get_dp_name_from_props` tests |
| `tests/daq_data/integration/test_server_isolation.py` | **New** — gRPC server isolation tests (sim mode) |
| `tests/daq_data_hashpipe/integration/test_daq_pipeline_e2e.py` | **New** — Hashpipe CI e2e tests |
