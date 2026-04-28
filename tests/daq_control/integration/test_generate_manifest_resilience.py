import asyncio
import os
import shutil
from pathlib import Path
from typing import Any

import pytest
from panoseti_grpc.daq_control.client import AsyncDaqControlClient

@pytest.mark.asyncio
async def test_generate_manifest_late_arrival(tmp_path: Path) -> None:
    """
    Verify that GenerateManifest retries and succeeds if the directory arrives late.
    This simulates VirtioFS/lag in a Docker fleet environment.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    run_dir_name = "late_run.pffd"
    module_id = 100
    
    # Target path that the server will look for
    target_dir = data_dir / f"module_{module_id}" / run_dir_name
    
    # We will call GenerateManifest, which has a 5s retry loop.
    # We will wait 2s, then create the directory and a .pff file.
    
    async def delayed_create():
        await asyncio.sleep(2.0)
        target_dir.mkdir(parents=True)
        (target_dir / "test_data.pff").write_text("dummy data")
        
    # Start the delayed creation task
    creation_task = asyncio.create_task(delayed_create())
    
    try:
        async with AsyncDaqControlClient(host="localhost", port=50051) as client:
            params = {
                "data_dir": str(data_dir),
                "run_dir": run_dir_name,
                "module_id": module_id,
                "algorithm": "blake3",
                "include_patterns": ["*.pff"],
            }
            
            # This should block for ~2s then succeed
            resp = await client.GenerateManifest(params, timeout=10.0)
            
            assert resp["success"] is True
            assert resp["file_count"] == 1
            assert "manifest." in resp["manifest_path"]
    finally:
        await creation_task

@pytest.mark.asyncio
async def test_generate_manifest_symlink_resilience(tmp_path: Path) -> None:
    """
    Verify that GenerateManifest correctly handles symlinked directories via its resolve() logic.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    actual_data_root = tmp_path / "actual_data"
    actual_data_root.mkdir()
    
    run_dir_name = "symlink_run.pffd"
    module_id = 101
    
    # 1. Create actual data in a different location
    actual_target = actual_data_root / f"module_{module_id}" / run_dir_name
    actual_target.mkdir(parents=True)
    (actual_target / "data.pff").write_text("some pff data")
    
    # 2. Create a symlink in the expected data_dir location
    # /data/module_101/symlink_run.pffd -> /actual_data/module_101/symlink_run.pffd
    expected_module_root = data_dir / f"module_{module_id}"
    expected_module_root.mkdir(parents=True)
    
    os.symlink(actual_target, expected_module_root / run_dir_name)
    
    async with AsyncDaqControlClient(host="localhost", port=50051) as client:
        params = {
            "data_dir": str(data_dir),
            "run_dir": run_dir_name,
            "module_id": module_id,
            "algorithm": "blake3",
            "include_patterns": ["*.pff"],
        }
        
        resp = await client.GenerateManifest(params)
        
        assert resp["success"] is True
        assert resp["file_count"] == 1
        # The manifest path should ideally be relative to the resolved path or preserved as per server logic
        assert "manifest." in resp["manifest_path"]
        # Verify it actually exists in the target
        assert os.path.exists(resp["manifest_path"])
        assert str(actual_target) in resp["manifest_path"]
