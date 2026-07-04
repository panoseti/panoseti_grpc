"""
Unit tests for daq_control/client.py response parsing.

Regression coverage for a real bug this session: StatusDaq's dict-conversion
built the result with a hardcoded field list that predated the
hashpipe_thread_count/hashpipe_healthy proto fields, so the server computed
and sent the correct values but the client silently dropped them -- pseti
stat showed "[0/4 threads]" for a perfectly healthy Hashpipe. The server-side
computation and the raw proto fields were both covered by existing tests;
only the client's dict conversion was not, which is exactly how this slipped
through.
"""

from unittest.mock import MagicMock

from panoseti_grpc.daq_control.client import DaqControlClient
from panoseti_grpc.generated import daq_control_pb2


class TestStatusDaqResponseParsing:
    def _client_with_mocked_stub(self, response: daq_control_pb2.DaqStatusResponse) -> DaqControlClient:
        client = DaqControlClient.__new__(DaqControlClient)  # skip __init__ (no real channel)
        client.stub = MagicMock()
        client.stub.StatusDaq.return_value = response
        return client

    def test_surfaces_thread_count_and_healthy(self) -> None:
        response = daq_control_pb2.DaqStatusResponse(
            success=True,
            hashpipe_running=True,
            hashpipe_pid=96,
            hashpipe_thread_count=4,
            hashpipe_healthy=True,
        )
        client = self._client_with_mocked_stub(response)

        ok, status = client.StatusDaq({"data_dir": "/data"})

        assert ok is True
        assert status["hashpipe_pid"] == 96
        assert status["hashpipe_thread_count"] == 4
        assert status["hashpipe_healthy"] is True

    def test_surfaces_stuck_hashpipe_as_unhealthy(self) -> None:
        """The exact symptom this feature exists to catch: a live PID with
        only its main thread, never having spawned the pipeline workers."""
        response = daq_control_pb2.DaqStatusResponse(
            success=True,
            hashpipe_running=True,
            hashpipe_pid=96,
            hashpipe_thread_count=1,
            hashpipe_healthy=False,
        )
        client = self._client_with_mocked_stub(response)

        ok, status = client.StatusDaq({"data_dir": "/data"})

        assert ok is True
        assert status["hashpipe_running"] is True
        assert status["hashpipe_thread_count"] == 1
        assert status["hashpipe_healthy"] is False

    def test_not_running_reports_zero_threads_and_healthy(self) -> None:
        response = daq_control_pb2.DaqStatusResponse(
            success=True,
            hashpipe_running=False,
            hashpipe_thread_count=0,
            hashpipe_healthy=True,
        )
        client = self._client_with_mocked_stub(response)

        ok, status = client.StatusDaq({"data_dir": "/data"})

        assert ok is True
        assert status["hashpipe_running"] is False
        assert status["hashpipe_thread_count"] == 0
        assert status["hashpipe_healthy"] is True
