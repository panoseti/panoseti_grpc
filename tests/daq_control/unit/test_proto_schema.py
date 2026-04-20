"""
Red tests: assert new proto fields/RPCs exist in daq_control_pb2.
These tests FAIL until the proto is updated and recompiled.
"""

import pytest

from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc


class TestCleanupModeEnum:
    def test_cleanup_mode_enum_exists(self) -> None:
        assert hasattr(daq_control_pb2, "CleanupMode")

    def test_cleanup_full_value(self) -> None:
        assert daq_control_pb2.CleanupMode.Value("CLEANUP_FULL") == 0

    def test_cleanup_selective_value(self) -> None:
        assert daq_control_pb2.CleanupMode.Value("CLEANUP_SELECTIVE") == 1


class TestCleanupDataRequestExtensions:
    def test_mode_field_exists(self) -> None:
        req = daq_control_pb2.CleanupDataRequest()
        assert hasattr(req, "mode")

    def test_delete_patterns_field_exists(self) -> None:
        req = daq_control_pb2.CleanupDataRequest()
        assert hasattr(req, "delete_patterns")

    def test_preserve_patterns_field_exists(self) -> None:
        req = daq_control_pb2.CleanupDataRequest()
        assert hasattr(req, "preserve_patterns")

    def test_delete_patterns_is_repeated(self) -> None:
        req = daq_control_pb2.CleanupDataRequest(delete_patterns=["*.pff", "*.tmp"])
        assert list(req.delete_patterns) == ["*.pff", "*.tmp"]

    def test_preserve_patterns_is_repeated(self) -> None:
        req = daq_control_pb2.CleanupDataRequest(preserve_patterns=["manifest.json"])
        assert list(req.preserve_patterns) == ["manifest.json"]


class TestCleanupDataResponseExtensions:
    def test_deleted_count_field_exists(self) -> None:
        resp = daq_control_pb2.CleanupDataResponse()
        assert hasattr(resp, "deleted_count")

    def test_freed_bytes_field_exists(self) -> None:
        resp = daq_control_pb2.CleanupDataResponse()
        assert hasattr(resp, "freed_bytes")

    def test_preserved_paths_field_exists(self) -> None:
        resp = daq_control_pb2.CleanupDataResponse()
        assert hasattr(resp, "preserved_paths")

    def test_deleted_count_is_uint32(self) -> None:
        resp = daq_control_pb2.CleanupDataResponse(deleted_count=42)
        assert resp.deleted_count == 42

    def test_freed_bytes_is_uint64(self) -> None:
        resp = daq_control_pb2.CleanupDataResponse(freed_bytes=2**40)
        assert resp.freed_bytes == 2**40

    def test_preserved_paths_is_repeated(self) -> None:
        resp = daq_control_pb2.CleanupDataResponse(preserved_paths=["/data/foo.pff"])
        assert list(resp.preserved_paths) == ["/data/foo.pff"]


class TestGenerateManifestRequest:
    def test_message_exists(self) -> None:
        assert hasattr(daq_control_pb2, "GenerateManifestRequest")

    def test_data_dir_field(self) -> None:
        req = daq_control_pb2.GenerateManifestRequest(data_dir="/data")
        assert req.data_dir == "/data"

    def test_run_dir_field(self) -> None:
        req = daq_control_pb2.GenerateManifestRequest(run_dir="run001.pffd")
        assert req.run_dir == "run001.pffd"

    def test_module_id_field(self) -> None:
        req = daq_control_pb2.GenerateManifestRequest(module_id=42)
        assert req.module_id == 42

    def test_algorithm_field(self) -> None:
        req = daq_control_pb2.GenerateManifestRequest(algorithm="blake3")
        assert req.algorithm == "blake3"

    def test_include_patterns_field_is_repeated(self) -> None:
        req = daq_control_pb2.GenerateManifestRequest(include_patterns=["*.pff"])
        assert list(req.include_patterns) == ["*.pff"]


class TestGenerateManifestResponse:
    def test_message_exists(self) -> None:
        assert hasattr(daq_control_pb2, "GenerateManifestResponse")

    def test_success_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(success=True)
        assert resp.success is True

    def test_message_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(message="ok")
        assert resp.message == "ok"

    def test_manifest_path_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(manifest_path="/data/manifest.json")
        assert resp.manifest_path == "/data/manifest.json"

    def test_file_count_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(file_count=7)
        assert resp.file_count == 7

    def test_total_bytes_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(total_bytes=1024)
        assert resp.total_bytes == 1024

    def test_elapsed_seconds_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(elapsed_seconds=0.5)
        assert pytest.approx(resp.elapsed_seconds) == 0.5

    def test_algorithm_field(self) -> None:
        resp = daq_control_pb2.GenerateManifestResponse(algorithm="blake3")
        assert resp.algorithm == "blake3"


class TestGetManifestRequest:
    def test_message_exists(self) -> None:
        assert hasattr(daq_control_pb2, "GetManifestRequest")

    def test_data_dir_field(self) -> None:
        req = daq_control_pb2.GetManifestRequest(data_dir="/data")
        assert req.data_dir == "/data"

    def test_run_dir_field(self) -> None:
        req = daq_control_pb2.GetManifestRequest(run_dir="run001.pffd")
        assert req.run_dir == "run001.pffd"

    def test_module_id_field(self) -> None:
        req = daq_control_pb2.GetManifestRequest(module_id=10)
        assert req.module_id == 10


class TestManifestEntry:
    def test_message_exists(self) -> None:
        assert hasattr(daq_control_pb2, "ManifestEntry")

    def test_relative_path_field(self) -> None:
        entry = daq_control_pb2.ManifestEntry(relative_path="run001.pffd/foo.pff")
        assert entry.relative_path == "run001.pffd/foo.pff"

    def test_digest_hex_field(self) -> None:
        entry = daq_control_pb2.ManifestEntry(digest_hex="deadbeef")
        assert entry.digest_hex == "deadbeef"

    def test_size_bytes_field(self) -> None:
        entry = daq_control_pb2.ManifestEntry(size_bytes=4096)
        assert entry.size_bytes == 4096

    def test_mtime_ns_field(self) -> None:
        entry = daq_control_pb2.ManifestEntry(mtime_ns=1_700_000_000_000_000_000)
        assert entry.mtime_ns == 1_700_000_000_000_000_000


class _FakeChannel:
    """Minimal fake channel for stub instantiation in unit tests."""

    def unary_unary(self, *a: object, **kw: object) -> object:
        return None

    def unary_stream(self, *a: object, **kw: object) -> object:
        return None

    def stream_unary(self, *a: object, **kw: object) -> object:
        return None

    def stream_stream(self, *a: object, **kw: object) -> object:
        return None


class TestDaqControlStubNewMethods:
    """gRPC stub methods are instance attributes set in __init__, not class attributes.
    Instantiate with a fake channel to verify they exist."""

    def test_generate_manifest_method_exists(self) -> None:
        stub = daq_control_pb2_grpc.DaqControlStub(_FakeChannel())
        assert hasattr(stub, "GenerateManifest")

    def test_get_manifest_method_exists(self) -> None:
        stub = daq_control_pb2_grpc.DaqControlStub(_FakeChannel())
        assert hasattr(stub, "GetManifest")
