# Unified QA Infrastructure & gRPC Linter Plan

## Objective
Create a unified QA runner `tests/qa.py` driven by `tests/qa.toml` that executes linters (Ruff, MyPy) and test suites in containerized environments. Enhance the CI pipeline to enforce code formatting and type safety, addressing gRPC-specific Python quirks by generating robust type stubs.

## Key Files & Context
- `tests/qa.py`: The new unified Python QA runner.
- `tests/qa.toml`: The configuration file mapping QA commands to Docker execution.
- `pyproject.toml`: To be updated with `ruff`, `mypy`, `mypy-protobuf`, and `types-protobuf` dependencies.
- `scripts/compile_protos.py`: To be updated to generate `.pyi` stubs via `mypy-protobuf`.
- `Dockerfile.ci`: To be augmented with a `qa-linter` stage to check code formatting in a CI environment.
- `.github/workflows/ci.yml`: Will invoke the new QA linters (assumed path based on standard CI setups).

## Scope & Impact
This change establishes a strong foundation for code quality and testing. It unifies scattered bash scripts into a single, intuitive Python CLI tool. By adopting `mypy-protobuf`, we grant the entire codebase type safety for gRPC interactions while completely ignoring the PEP-8 violations intrinsic to the generated `.py` source code.

## Proposed Solution

### 1. gRPC Complication & Type Safety
- **Dependency Update**: Add `mypy-protobuf` and `types-protobuf` to the `dev` dependencies in `pyproject.toml`. Add `ruff` and `mypy` as well.
- **Protobuf Compilation**: Modify `scripts/compile_protos.py` to pass the `--mypy_out` and `--mypy_grpc_out` flags to the `protoc` compiler. This will generate `.pyi` type stubs alongside the `.py` files.
- **Relative Imports Patch**: Update the `fix_relative_imports` function in `scripts/compile_protos.py` to also patch the newly generated `.pyi` files to ensure they resolve imports correctly.
- **Linter Exclusion**: Configure `[tool.ruff]` and `[tool.mypy]` in `pyproject.toml` to explicitly exclude the `src/panoseti_grpc/generated/` directory from analysis. MyPy will automatically rely on the `.pyi` stubs when those modules are imported by our source code.

### 2. Containerized Linting (`Dockerfile.ci`)
- Add a new BuildKit stage named `qa-linter` to `Dockerfile.ci` that inherits from `base`.
- Set the default command for this stage to run `ruff check`, `ruff format --check`, and `mypy` sequentially across `src/`, `tests/`, and `scripts/`. This provides a "simple way to check the code formatting within a CI environment".

### 3. Unified QA Runner (`tests/qa.py` & `tests/qa.toml`)
- Create `tests/qa.toml` to fully encapsulate the QA environment configuration:
    - Define concurrent tasks in `[lint]` for `ruff` and `mypy` utilizing `docker build` and `docker run` targeting the new `qa-linter` stage.
    - Define sequential tasks in `[test]` for each service (`daq_data`, `daq_control`, `telemetry`, `ublox`, `unified_server`, `hashpipe_daq_data`). Instead of simply wrapping the existing `.sh` scripts, we will embed the equivalent underlying `docker compose up --build --abort-on-container-exit` or `docker run --rm` functionality directly into `qa.toml`. This allows `qa.py` to function as an independent, fully capable test runner, while leaving the existing bash scripts intact for backward compatibility.
- Create `tests/qa.py` modeling the provided golden example:
    - Implement a `QARunner` class with `run_parallel` (for linters) and `run_sequential` (for individual/all test suites) execution modes.
    - Implement the exact colorized, line-by-line streaming functionality from the golden example, ensuring each parallel task gets a distinct color prefix so interleaved streams remain legible.
    - Expose a simple command-line interface using Python's `argparse` module. It will feature subcommands for `lint` and `all`, as well as dedicated subcommands for each individual test suite (e.g., `daq_data`, `daq_control`, `telemetry`, `ublox`, `unified_server`, `hashpipe_daq_data`) instead of a single `test` subcommand.

## Implementation Steps
1. **Update `pyproject.toml`**: Inject the new linting and typing dependencies and their configuration blocks.
2. **Update `scripts/compile_protos.py`**: Inject the `mypy-protobuf` plugin execution and patch the `.pyi` file imports.
3. **Update `Dockerfile.ci`**: Add the `qa-linter` stage.
4. **Create `tests/qa.toml`**: Define the linting and testing execution mappings.
5. **Create `tests/qa.py`**: Implement the Python runner logic, heavily inspired by the Jump 2.0 golden architecture.

## Verification & Testing
- Run `python scripts/compile_protos.py` and manually verify the presence and correctness of `.pyi` files in `src/panoseti_grpc/generated/`.
- Run `python tests/qa.py lint` to verify that `ruff` and `mypy` execute successfully inside Docker and appropriately ignore the generated Python code while correctly typing the gRPC imports.
- Run `python tests/qa.py all` to ensure the entire suite (linters + tests) passes.