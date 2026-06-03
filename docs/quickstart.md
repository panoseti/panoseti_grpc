# Quick Start — panoseti_grpc Development

## Setup

```bash
conda create -n grpc-py314 python=3.14
conda activate grpc-py314
pip install -e ".[dev]"
```

## Build / Code Generation

```bash
# Recompile all .proto files → generates src/panoseti_grpc/generated/*_pb2.py and *_pb2_grpc.py
python scripts/compile_protos.py
```

After editing any `.proto` file, recompile before testing. Never edit the generated files directly.

## Testing

Unified QA runner (recommended):

```bash
python tests/qa.py all                # Run all linters and test suites
python tests/qa.py lint               # Run linters (Ruff, MyPy)
python tests/qa.py telemetry          # Run specific test suite (e.g. telemetry)
```

Individual service CI shell scripts (legacy):

```bash
./scripts/run-ci-tests/run-daq-data-ci-test.sh
./scripts/run-ci-tests/run-daq-control-test.sh
./scripts/run-ci-tests/run-telemetry-ci-test.sh
./scripts/run-ci-tests/run-unified-server-ci-test.sh
./scripts/run-ci-tests/run-ublox-ci-test.sh
./scripts/run-ci-tests/run-hashpipe-daq-data-ci.sh
```

Run unified server tests locally (unit tests require no services; integration tests skip gracefully without Redis):

```bash
pytest tests/unified_server/unit/ -v
pytest tests/unified_server/integration/ -v --timeout=90
```

Run a single test file directly:

```bash
pytest tests/daq_data/test_server.py
pytest tests/telemetry/ -k "test_log_flexible"
```

## Lint / Format

```bash
black src/ tests/ scripts/    # line-length 120
flake8 src/ tests/ scripts/   # max-line-length 120, ignores E203, W503
```

## Run a service locally

Unified server (recommended):

```bash
pseti-grpc server                                   # all services (default config)
pseti-grpc server --profile daq_node                # daq_data + daq_control
pseti-grpc server --profile headnode                # telemetry only
pseti-grpc server --config /path/to/server.toml    # custom config file
pseti-grpc server --list-services                   # print registered services and exit
python -m panoseti_grpc                             # equivalent to pseti-grpc server
```

Observatory CLI (`pseti-grpc`) for a running server:

```bash
pseti-grpc status                      # probe all services and print table
pseti-grpc reflect                     # list services via gRPC reflection
pseti-grpc telemetry log --message '{"event":"test"}'
pseti-grpc daq-data ping
pseti-grpc daq-data init-sim           # init simulation mode
pseti-grpc daq-data stream --seconds 5
pseti-grpc daq-control status
pseti-grpc --host mynode --port 50051 status  # connect to remote
```

Individual service entry points (standalone):

```bash
panoseti-daq-data
panoseti-daq-control
panoseti-telemetry
# or via python -m:
python -m panoseti_grpc.daq_data.server
python -m panoseti_grpc.daq_control.server
python -m panoseti_grpc.telemetry.server
```
