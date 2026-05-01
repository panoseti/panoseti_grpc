"""
Unit tests to verify that DaqControlServicer correctly uses its lifecycle lock
to serialize StartDaq, StopDaq, and CleanupData operations.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from panoseti_grpc.daq_control.server import DaqControlServicer
from panoseti_grpc.generated import daq_control_pb2

@pytest.fixture
def servicer():
    """Return a DaqControlServicer with patched process discovery."""
    with (
        patch("panoseti_grpc.daq_control.server.psutil.process_iter", return_value=[]),
        patch("panoseti_grpc.daq_control.server.get_logger", return_value=MagicMock()),
    ):
        return DaqControlServicer()

def make_mock_context():
    ctx = MagicMock()
    ctx.abort = AsyncMock()
    return ctx

@pytest.mark.asyncio
async def test_start_daq_acquires_lock(servicer, tmp_path):
    """StartDaq must acquire the lifecycle lock."""
    # Mock StartDaq dependencies to succeed quickly
    servicer._get_pids_by_name = MagicMock(return_value=(0, []))
    servicer._create_module_config = MagicMock()
    servicer._setup_data_directories = MagicMock()
    
    # Mock asyncio.create_subprocess_exec
    mock_proc = AsyncMock()
    mock_proc.pid = 1234
    
    # Mock is_hashpipe_running
    with (
        patch("asyncio.create_subprocess_exec", return_value=mock_proc),
        patch("panoseti_grpc.daq_control.server.is_hashpipe_running", return_value=True),
        patch("panoseti_grpc.daq_control.server.get_logger", return_value=MagicMock()),
    ):
        lock_spy = servicer._lifecycle_lock
        
        request = daq_control_pb2.StartDaqRequest(
            data_dir=str(tmp_path),
            daq_ip_addr="127.0.0.1",
            bindhost="lo",
            run_dir="test.pffd",
            obs="test",
            module_id=[1]
        )
        
        assert not lock_spy.locked()
        await servicer.StartDaq(request, make_mock_context())
        assert not lock_spy.locked()

@pytest.mark.asyncio
async def test_lifecycle_serialization(servicer, tmp_path):
    """Verify that multiple lifecycle calls are serialized by the lock."""
    
    servicer._get_pids_by_name = MagicMock(return_value=(0, []))
    servicer._create_module_config = MagicMock()
    servicer._setup_data_directories = MagicMock()
    
    mock_proc = AsyncMock()
    mock_proc.pid = 1234
    
    request = daq_control_pb2.StartDaqRequest(
        data_dir=str(tmp_path),
        daq_ip_addr="127.0.0.1",
        bindhost="lo",
        run_dir="test.pffd",
        obs="test",
        module_id=[1]
    )

    concurrent_tasks = 0
    max_concurrent = 0

    async def slow_start(*args, **kwargs):
        nonlocal concurrent_tasks, max_concurrent
        concurrent_tasks += 1
        max_concurrent = max(max_concurrent, concurrent_tasks)
        await asyncio.sleep(0.05) 
        concurrent_tasks -= 1
        return mock_proc

    with (
        patch("asyncio.create_subprocess_exec", side_effect=slow_start),
        patch("panoseti_grpc.daq_control.server.is_hashpipe_running", return_value=True),
    ):
        t1 = asyncio.create_task(servicer.StartDaq(request, make_mock_context()))
        t2 = asyncio.create_task(servicer.StartDaq(request, make_mock_context()))
        
        await asyncio.gather(t1, t2)
        assert max_concurrent == 1, "StartDaq calls were not serialized"

@pytest.mark.asyncio
async def test_stop_daq_acquires_lock(servicer):
    """StopDaq must acquire the lifecycle lock."""
    servicer._get_pids_by_name = MagicMock(side_effect=[(1, [1234]), (0, []), (0, []), (0, [])])
    
    with patch("psutil.Process") as mock_psutil:
        p_mock = MagicMock()
        mock_psutil.return_value = p_mock
        
        request = daq_control_pb2.StopDaqRequest(data_dir="/tmp", run_dir="test")
        
        task = asyncio.create_task(servicer.StopDaq(request, make_mock_context()))
        
        acquired = False
        for _ in range(100):
            if servicer._lifecycle_lock.locked():
                acquired = True
                break
            await asyncio.sleep(0.001)
            
        assert acquired, "Lock was never acquired by StopDaq"
        await task
        assert not servicer._lifecycle_lock.locked()

@pytest.mark.asyncio
async def test_cleanup_data_acquires_lock(servicer, tmp_path):
    """CleanupData must acquire the lifecycle lock."""
    servicer._get_pids_by_name = MagicMock(return_value=(0, []))
    servicer._cleanup_dir = MagicMock(return_value=True)
    
    request = daq_control_pb2.CleanupDataRequest(
        data_dir=str(tmp_path),
        run_dir="test",
        module_id=[1]
    )
    
    assert not servicer._lifecycle_lock.locked()
    await servicer.CleanupData(request, make_mock_context())
    assert not servicer._lifecycle_lock.locked()
