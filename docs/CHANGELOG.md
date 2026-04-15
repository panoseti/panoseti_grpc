# Changelog

All notable changes to the PANOSETI gRPC project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Unified QA Infrastructure**: Introduced `tests/qa.py` and `tests/qa.toml` for containerized linting and testing.
- **Python 3.14 Modernization**: Updated `src/panoseti_grpc/util/` and `src/panoseti_grpc/telemetry/` to leverage Python 3.14 features.
    - Used PEP 695 type parameters for generic functions (e.g., `def foo[T](...)`).
    - Adopted `Type | None` syntax for optional types (PEP 604).
    - Integrated `from __future__ import annotations` across all modernized modules.

### Changed
- **Strict Typing Compliance**: Achieved 100% MyPy `strict` mode compliance for `daq_control`, `telemetry`, and `util` subdirectories.
- **Ruff Linting**: Standardized code style and formatting using Ruff (120-character line limit).
- **gRPC Message Renaming**: (Breaking) Renamed `StatusDaqRequest/Response` to `DaqStatusRequest/Response` in `daq_control.proto` for naming consistency.
- **Telemetry Configuration**: Updated `TelemetryConfig.load` and `validate_and_flatten` with stricter type hints and Pydantic 2.x validation.
- **Logging Pipeline**: Refactored `AsyncGrpcHandler` and `PanosetiLogFactory` for better type safety and resource management.

### Fixed
- Fixed numerous "Missing return type annotation" and "Incompatible types in assignment" errors identified by MyPy.
- Resolved "Implicit Optional" warnings by explicitly using `Type | None`.
- Corrected invalid `isinstance` checks against parameterized generics.
- Fixed `_get_timestamp` and `_proto_to_dict` methods that were missing return type hints despite returning values.

## [0.4.5] - 2026-04-14

### Added
- Initial implementation of the Telemetry service with Loki integration.
- Unified server main entry point.
- Support for U-blox GNSS timing modules.
