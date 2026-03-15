import pytest
from pydantic import ValidationError
from panoseti_grpc.telemetry.config import LogSchema, LogSeverity


class TestSchemaGuards:

    def test_valid_log_packet(self):
        """Happy path should pass."""
        log = LogSchema(
            host="node-01",
            service_name="capture_service",
            severity=LogSeverity.INFO,
            payload_json='{"status": "ok"}'
        )
        assert log.host == "node-01"

    def test_hostname_validation(self):
        """
        Hostnames with spaces or special chars should fail.
        This protects Loki from high-cardinality/invalid labels.
        """
        # Invalid: Space
        with pytest.raises(ValidationError) as exc:
            LogSchema(
                host="node 01",
                service_name="svc",
                payload_json="{}"
            )
        assert "pattern" in str(exc.value) or "String should match" in str(exc.value)

        # Invalid: SQL Injection-like chars
        with pytest.raises(ValidationError):
            LogSchema(
                host="node-01; DROP TABLE",
                service_name="svc",
                payload_json="{}"
            )

    def test_payload_size_limit(self):
        """
        Ensure we don't accidentally accept 100MB logs which would choke Redis/Loki.
        """
        # Create 1.1 MB string
        massive_payload = "a" * 1_100_000

        with pytest.raises(ValidationError) as exc:
            LogSchema(
                host="node-01",
                service_name="svc",
                payload_json=massive_payload
            )
        assert "max_length" in str(exc.value) or "at most" in str(exc.value)

    def test_service_name_length(self):
        """Service name should be reasonable."""
        with pytest.raises(ValidationError):
            LogSchema(
                host="node-01",
                service_name="a",  # Too short
                payload_json="{}"
            )

    def test_empty_payload(self):
        """Empty payload is useless and shouldn't be stored."""
        with pytest.raises(ValidationError):
            LogSchema(
                host="node-01",
                service_name="svc",
                payload_json=""
            )