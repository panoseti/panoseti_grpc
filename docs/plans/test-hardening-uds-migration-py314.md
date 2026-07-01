# Plan: Test Suite Hardening, UDS Migration, and Python 3.14 Readiness

## Context

The existing test suites for `daq_data`, `daq_data_hashpipe`, `daq_control`, and `telemetry` were surveyed. Three things need to happen together:

1. **Prune non-UDS data paths from `daq_data` tests** — the team has decided UDS (unix-domain socket) is the only supported Hashpipe → gRPC data path going forward. Tests for `RPC`, `filesystem_pipe`, and `filesystem_poll` simulation modes are dead weight and should be removed from the test suite (and their corresponding simulation configs/fixtures).

2. **Add comprehensive new tests** across all three services to close known coverage gaps.

3. **Audit Python 3.9 → 3.14 incompatibilities** so the upcoming stability refactor starts with a clear migration checklist.

Separately, once implementation is complete the two CI scripts (`run-daq-data-ci-test.sh`, `run-telemetry-ci-test.sh`) must pass cleanly as the acceptance criterion.

---

## Part 1 — Remove Non-UDS Tests from daq_data

### Files to modify

| File | Change |
|---|---|
| `tests/daq_data/conftest.py` | Remove `rpc_sim_server_config`, `filesystem_pipe_sim_server_config`, `filesystem_poll_sim_server_config` fixtures and their entries in the `sim_server_process` parametrize list. Keep only `uds_sim_server_config`. |
| `tests/daq_data/test_simulation.py` | Remove parameterized variants for `rpc`, `filesystem_pipe`, `filesystem_poll` modes. Keep only the `uds` mode test case. |
| `tests/daq_data/test_multi_server_multi_client.py` | Delete `test_rpc_multi_rate_streams_single_client`, `test_rpc_multi_servers_single_client`, `test_rpc_many_concurrent_streams`. |

The source-level simulation strategies (`src/panoseti_grpc/daq_data/simulate.py`: `FilesystemPollStrategy`, `FilesystemPipeStrategy`, `RpcStrategy`) and corresponding `data_sources.py` classes are **not** removed yet — that is scope for the planned larger refactor. The tests just stop exercising them.

---

## Part 2 — New Tests to Add

### 2A — daq_data (UDS only)

**New file: `tests/daq_data/test_uds_socket_lifecycle.py`**

| Test | What it covers |
|---|---|
| `test_stale_socket_file_cleaned_up_on_server_start` | Server removes leftover `.sock` files from a previous run before binding. Simulates crash scenario. |
| `test_uds_receive_buffer_overflow_does_not_crash` | Flood the UDS socket faster than the consumer drains it; server must not crash or hang. |
| `test_uds_client_abrupt_disconnect_mid_frame` | Close the raw socket mid-write; server must detect `IncompleteReadError` and continue serving other clients. |
| `test_uds_socket_permissions` | Socket file created with mode `0o600`; confirm other users (simulated) cannot connect. |
| `test_frame_id_monotonic_across_reinit` | After a forced re-init (`force=True`), frame IDs reset to -1 and the first new frame delivered to a new reader has a higher ID than the last frame from the old session. |
| `test_module_discovery_from_uds_stream` | Start server with no pre-configured `module_ids`; verify server auto-discovers module IDs from the UDS stream. |

**New file: `tests/daq_data/test_uds_error_recovery.py`**

| Test | What it covers |
|---|---|
| `test_server_recovers_after_uds_producer_restart` | UDS producer disconnects then reconnects; server must re-accept and resume streaming without client restart. |
| `test_slow_consumer_backpressure` | Reader sleeps for 2 s per frame; server must not accumulate unbounded queue memory (verify queue size stays bounded). |
| `test_stream_deadline_exceeded_on_idle_source` | No data arrives for longer than `DEQUEUE_TIMEOUT * MAX_TIMEOUTS`; StreamImages should abort with `DEADLINE_EXCEEDED`. |

### 2B — daq_data_hashpipe

**Extend `tests/daq_data_hashpipe/test_snapshot_grpc_robustness.py`**

| Test | What it covers |
|---|---|
| `test_module_id_filter_with_real_data` | Init with a strict `module_ids` whitelist; verify frames from other modules are not delivered. |
| `test_frame_continuity_across_output_block_boundary` | Validate that frame numbers don't skip when Hashpipe rolls to a new output buffer block (detects off-by-one in `hp_io_manager`). |
| `test_concurrent_clients_receive_same_frames` | Two clients, both initialized with same module, compare frame IDs: no frame delivered to one but not the other (within ±1 tolerance for timing). |

### 2C — daq_control

**New file: `tests/daq_control/test_concurrent_requests.py`**

| Test | What it covers |
|---|---|
| `test_concurrent_start_daq_rejected` | Two simultaneous `StartDaq` RPCs; second must fail with `success=False` (already-running check). Uses `ThreadPoolExecutor`. |
| `test_stop_then_start_idempotent` | `StopDaq` when not running returns `success=True`; immediately calling `StartDaq` again succeeds. |
| `test_cleanup_while_start_in_progress` | `CleanupData` during a brief window before `StartDaq` confirms hashpipe is alive must fail gracefully. |

**New file: `tests/daq_control/test_process_edge_cases.py`**

| Test | What it covers |
|---|---|
| `test_hashpipe_crash_detection` | Send `SIGKILL` to the hashpipe process after `StartDaq`; `StatusDaq(check_hashpipe_running=True)` must return `hashpipe_running=False`. |
| `test_stop_daq_with_stale_pid` | Manually corrupt `servicer.hashpipe_pid` to a non-existent PID; `StopDaq` must handle `NoSuchProcess` and return `success=True`. |
| `test_log_files_written_to_correct_run_dir` | After `StartDaq`, wait 1 s; verify `hp_stdout.log` and `hp_stderr.log` are under `{data_dir}/{run_dir}/`, not the wrong path. |
| `test_disk_usage_keys_present` | `StatusDaq(check_disk_usage=True)` response struct must contain `total_disk_space`, `used_disk_space`, `free_disk_space` with positive values and `total >= used + free`. |

### 2D — telemetry

**New file: `tests/telemetry/test_ttl_enforcement.py`**

| Test | What it covers |
|---|---|
| `test_experimental_key_has_positive_ttl` | After `log_flexible` for a `DEV_` device, `redis.ttl(key) > 0`. |
| `test_production_key_has_no_ttl` | After `log_strict` for a registered production device, `redis.ttl(key) == -1` (persists forever). |
| `test_sandbox_key_has_ttl` | Unknown device_type routes to `SANDBOX:` prefix; key has TTL > 0. |

**New file: `tests/telemetry/test_batch_flusher.py`**

| Test | What it covers |
|---|---|
| `test_batch_flush_delivers_all_logs` | Send exactly 250 `Log` RPCs in burst; poll Redis `LLEN logs:ingress` until stable; assert count == 250. |
| `test_batch_flush_survives_redis_disconnect` | Momentarily disconnect Redis mid-burst (kill + restart container); after reconnect, queue must drain without data loss for logs sent after reconnect. |

**New file: `tests/telemetry/test_concurrent_field_merging.py`**

| Test | What it covers |
|---|---|
| `test_two_threads_different_fields_no_cross_contamination` | Thread A updates `lat/lon`, thread B updates `satellites` on same device_id simultaneously; final Redis hash must have both fields. |
| `test_rapid_field_overwrite_last_writer_wins` | 20 sequential updates to `fix_mode` field; final Redis value must match the last write (no stale values). |

---

## Part 3 — Python 3.14 Migration Checklist

### Severity key: 🔴 Breaking · 🟡 Deprecated/Warning · 🟢 Opportunity

### 3.1 asyncio

| Issue | Severity | Files to audit |
|---|---|---|
| `asyncio.get_event_loop()` — raises `DeprecationWarning` in 3.12 and may raise `RuntimeError` in 3.14 when called outside a running loop | 🔴 | Grep for `get_event_loop()` across `src/` and `tests/`; replace with `asyncio.get_running_loop()` inside coroutines, or `asyncio.new_event_loop()` in `__main__` blocks |
| `asyncio.coroutine` decorator — removed in 3.11 | 🔴 | Unlikely to be present but verify with `grep -r "asyncio.coroutine"` |
| `asyncio.wait_for(coro, timeout=N)` — still works but `asyncio.timeout()` (3.11+) is preferred | 🟢 | Migrate opportunistically |
| `asyncio.TaskGroup` (3.11+) — structured replacement for manual `gather` / `create_task` patterns | 🟢 | Refactor multi-task fanout in `hp_io_manager.py` and `client.py` |
| `pytest-asyncio` — version ≥ 0.23 requires explicit `asyncio_mode = "auto"` in `pyproject.toml` `[tool.pytest.ini_options]`; older implicit mode removed | 🔴 | Verify `pyproject.toml` has `asyncio_mode = "auto"` under `[tool.pytest.ini_options]` |

### 3.2 Type annotations

| Issue | Severity | Notes |
|---|---|---|
| `from typing import Dict, List, Tuple, Optional` — these still work but are deprecated aliases; Python 3.14 emits `DeprecationWarning` | 🟡 | Replace with `dict[...]`, `list[...]`, `tuple[...]`, `X \| None` |
| `from __future__ import annotations` — deferred evaluation may interact with Pydantic v2's `model_rebuild()` | 🟡 | Test with Pydantic after annotation migration |
| `collections.Callable` → `collections.abc.Callable` (removed in 3.10) | 🔴 | Grep for `collections.Callable` |

### 3.3 Dependency compatibility (as of March 2026)

| Package | Current pin | Python 3.14 status | Action |
|---|---|---|---|
| `grpcio==1.70.0` | Pinned | 3.14 wheels not yet published for 1.70.0; requires ≥ 1.63 for 3.13 support | Unpin upper bound: `grpcio>=1.70.0`; test with latest |
| `grpcio-tools==1.70.0` | Pinned | Same as above | Unpin to match grpcio |
| `protobuf>=5.26.1,<6.0.0` | Range | Compatible | Widen to `<7.0.0` once tested |
| `pydantic>=2.12` | Range | Pydantic 2.x supports 3.14 | No change needed |
| `pytest-asyncio` | Unpinned | Need ≥ 0.23 for 3.12+ asyncio mode | Pin `pytest-asyncio>=0.23` |
| `numpy` | Unpinned | NumPy 2.x supports 3.12+; 3.14 builds expected | Pin `numpy>=2.0` |
| `redis>=7` | Range | Redis-py 5.x supports 3.12+; 3.14 expected | No change needed |
| `watchfiles` | Unpinned | Rust-backed; wheel availability TBD for 3.14 | Monitor; may need source build |

### 3.4 Standard library opportunities

| Addition | Version | Opportunity |
|---|---|---|
| `tomllib` in stdlib | 3.11 | Replace the TOML dependency in `telemetry/config.py` if one is used |
| `asyncio.timeout()` | 3.11 | Replace `asyncio.wait_for` timeouts in stream handlers |
| Exception groups (`except*`) | 3.11 | Replace multi-task exception gathering patterns |
| `type X = ...` type alias syntax | 3.12 | Clean up `TypeAlias` usages |

### 3.5 Free-threaded Python (3.13+ experimental, 3.14 more stable)

The GIL-optional build (`python3.14t`) may expose latent thread-safety bugs. Known risk areas:

- `HpIoManager`'s shared `latest_data_cache` dict — multiple reader tasks access it; verify all writes are under the existing asyncio lock before enabling no-GIL mode
- `ClientManager` reader/writer slot tracking — review all `threading.Lock` vs `asyncio.Lock` usage
- `RedisBatcher` queue — uses `asyncio.Queue` which is GIL-safe but needs verification in free-threaded context

**Recommendation:** run the full test suite with `python3.13t` (free-threaded) as a canary before 3.14 migration.

---

## Part 4 — Verification

1. Run `./scripts/run-ci-tests/run-daq-data-ci-test.sh` — must exit 0 with no `FAILED` tests after UDS-only pruning.
2. Run `./scripts/run-ci-tests/run-telemetry-ci-test.sh` — must exit 0.
3. Run `./scripts/run-ci-tests/run-daq-control-test.sh` — must exit 0 (existing + new concurrent tests).
4. Run `./scripts/run-ci-tests/run-hashpipe-daq-data-ci.sh` — must exit 0 (RUN_REAL_DATA_TESTS=1 path).
5. After 3.14 audit changes: `pip install -e . && pytest tests/ -q --tb=short` under Python 3.12 (available today) to catch `DeprecationWarning`s surfaced as errors with `-W error::DeprecationWarning`.

---

## Part 5 — Where to Save This Plan in the Repo

Create `docs/plans/test-hardening-uds-migration-py314.md` and copy this plan there. The `docs/` directory is already used for architecture diagrams; `docs/plans/` is a natural home for decision records and implementation plans.

---

## Files to Modify

| File | Change type |
|---|---|
| `tests/daq_data/conftest.py` | Remove non-UDS fixtures and parametrize entries |
| `tests/daq_data/test_simulation.py` | Remove non-UDS test variants |
| `tests/daq_data/test_multi_server_multi_client.py` | Delete 3 RPC-specific test functions |
| `tests/daq_data/test_uds_socket_lifecycle.py` | New file |
| `tests/daq_data/test_uds_error_recovery.py` | New file |
| `tests/daq_data_hashpipe/test_snapshot_grpc_robustness.py` | Add 3 tests |
| `tests/daq_control/test_concurrent_requests.py` | New file |
| `tests/daq_control/test_process_edge_cases.py` | New file |
| `tests/telemetry/test_ttl_enforcement.py` | New file |
| `tests/telemetry/test_batch_flusher.py` | New file |
| `tests/telemetry/test_concurrent_field_merging.py` | New file |
| `pyproject.toml` | Add `asyncio_mode = "auto"` to `[tool.pytest.ini_options]` if missing; unpin grpcio upper bound |
| `docs/plans/test-hardening-uds-migration-py314.md` | Copy of this plan |
