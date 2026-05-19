import asyncio

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient
from panoseti_grpc.grpc_utils.exceptions import FailedPreconditionError, PanosetiRpcError

pytestmark = pytest.mark.asyncio


# Helpers
async def _collect_n(stream, n):
    out = []
    for _ in range(n):
        out.append(await stream.__anext__())
    return out


# 1. UDS mode: multi-rate streams from a single client
async def test_uds_multi_rate_streams_single_client(default_server_process):
    """
    Ensures multiple stream_images connections created from the same client over UDS
    can run concurrently at different update rates without starving or blocking each other.
    """
    host, port = default_server_process["host"], default_server_process["port"]
    async with AioDaqDataClient(host, port) as client:
        assert await client.init_sim() is True

        fast = client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.02,  # fast
        )
        slow = client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.2,  # slow
        )

        got_fast, got_slow = 0, 0

        async def read_fast():
            nonlocal got_fast
            for _ in range(5):
                await fast.__anext__()
                got_fast += 1

        async def read_slow():
            nonlocal got_slow
            for _ in range(2):
                await slow.__anext__()
                got_slow += 1

        await asyncio.gather(read_fast(), read_slow())
        assert got_fast >= 5 and got_slow >= 2


# 3. UDS mode: multiple clients per server with independent pacing
async def test_uds_multi_clients_independent_pacing(default_server_process):
    """Validates that separate client connections maintain their own pacing/timers."""
    host, port = default_server_process["host"], default_server_process["port"]

    async with (
        AioDaqDataClient(host, port) as client_a,
        AioDaqDataClient(host, port) as client_b,
    ):
        assert await client_a.init_sim() is True

        stream_a_fast = client_a.stream_images(
            stream_movie_data=True, stream_pulse_height_data=False, update_interval_seconds=0.05
        )
        stream_b_slow = client_b.stream_images(
            stream_movie_data=False, stream_pulse_height_data=True, update_interval_seconds=0.2
        )

        a_count, b_count = 0, 0

        async def consume_a():
            nonlocal a_count
            for _ in range(4):
                img = await stream_a_fast.__anext__()
                assert img["type"] == "MOVIE"
                a_count += 1

        async def consume_b():
            nonlocal b_count
            for _ in range(2):
                img = await stream_b_slow.__anext__()
                assert img["type"] == "PULSE_HEIGHT"
                b_count += 1

        await asyncio.gather(consume_a(), consume_b())
        assert a_count >= 4 and b_count >= 2


# 4. Gateway mode: multiple edges, single client, verify each host produces data
@pytest.mark.parametrize("num_servers", [2, 3])
async def test_uds_multi_servers_single_client(gateway_factory, num_servers):
    """
    Uses gateway_factory to launch N edge servers + gateway, then verifies the
    merged stream delivers images from all expected module_ids.
    """
    gw = await gateway_factory(num_servers)
    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_sim() is True

        expected_modules = {d["module_id"] for d in gw["edge_details"]}
        seen = set()
        async for img in client.stream_images(
            stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
        ):
            seen.add(img["module_id"])
            if seen == expected_modules:
                break

        assert seen == expected_modules, f"Expected data from all modules: {expected_modules}, got {seen}"


# 5. Gateway mode: module_ids filtering on multi-edge setup
@pytest.mark.parametrize("num_servers", [3])
async def test_uds_multi_servers_module_filtering(gateway_factory, num_servers):
    """Confirms that StreamImages respects per-request module whitelist through the gateway."""
    gw = await gateway_factory(num_servers)
    edge_details = gw["edge_details"]

    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_sim() is True

        target = edge_details[1]["module_id"]
        # Collect frames with a single-module whitelist
        for _ in range(6):
            async for img in client.stream_images(
                stream_movie_data=True,
                stream_pulse_height_data=True,
                update_interval_seconds=0.05,
                module_ids=(target,),
            ):
                assert img["module_id"] == target
                break  # one frame per stream call is enough


# 6. Gateway mode: mixed rates across multiple edges and clients
@pytest.mark.parametrize("num_servers", [2])
async def test_uds_multi_servers_multi_clients_mixed_rates(gateway_factory, num_servers):
    """
    One client subscribes at fast rate for movies only; another at slow rate for PH only.
    Both should see data from all edges without cross-interference.
    """
    gw = await gateway_factory(num_servers)
    expected_modules = {d["module_id"] for d in gw["edge_details"]}

    async with (
        AioDaqDataClient(gw["host"], gw["port"]) as client_a,
        AioDaqDataClient(gw["host"], gw["port"]) as client_b,
    ):
        assert await client_a.init_sim() is True

        seen_movies, seen_ph = set(), set()

        async def read_movies():
            async for img in client_a.stream_images(
                stream_movie_data=True, stream_pulse_height_data=False, update_interval_seconds=0.03
            ):
                assert img["type"] == "MOVIE"
                seen_movies.add(img["module_id"])
                if expected_modules.issubset(seen_movies):
                    break

        async def read_ph():
            async for img in client_b.stream_images(
                stream_movie_data=False, stream_pulse_height_data=True, update_interval_seconds=0.2
            ):
                assert img["type"] == "PULSE_HEIGHT"
                seen_ph.add(img["module_id"])
                if expected_modules.issubset(seen_ph):
                    break

        await asyncio.gather(read_movies(), read_ph())

        assert expected_modules.issubset(seen_movies)
        assert expected_modules.issubset(seen_ph)


# 7. UDS mode: enforce StreamImages precondition (must init first)
async def test_uds_stream_requires_init(default_server_process):
    """StreamImages should fail with FAILED_PRECONDITION when hp_io task is not valid."""
    host, port = default_server_process["host"], default_server_process["port"]
    async with AioDaqDataClient(host, port) as client:
        with pytest.raises(FailedPreconditionError):
            async for _ in client.stream_images(
                stream_movie_data=True, stream_pulse_height_data=False, update_interval_seconds=0.1
            ):
                break


# 8. Gateway mode: many concurrent streams per client
@pytest.mark.parametrize("num_servers", [2])
async def test_uds_many_concurrent_streams(gateway_factory, num_servers):
    """Validates scalability of gateway streaming w.r.t. concurrent stream_images per client."""
    gw = await gateway_factory(num_servers)

    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_sim() is True

        rates = [0.03, 0.05, 0.07, 0.10]
        streams = [
            client.stream_images(
                stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=r
            )
            for r in rates
        ]

        async def read_some(s):
            got = 0
            for _ in range(3):
                await s.__anext__()
                got += 1
            return got

        results = await asyncio.gather(*(read_some(s) for s in streams))
        assert all(x >= 3 for x in results)


# 9. Gateway mode: empty module_ids streams data from all edges
@pytest.mark.parametrize("num_servers", [2])
async def test_uds_module_ids_empty_means_all(gateway_factory, num_servers):
    """Empty module_ids tuple should yield frames from all edges through the gateway."""
    gw = await gateway_factory(num_servers)

    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_sim() is True

        expected_modules = {d["module_id"] for d in gw["edge_details"]}
        seen = set()
        async for img in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.05,
            module_ids=(),  # all modules
        ):
            seen.add(img["module_id"])
            if seen == expected_modules:
                break

        assert seen == expected_modules


# 10. Gateway mode: partial edge failure tolerance
@pytest.mark.parametrize("num_servers", [3])
async def test_uds_partial_server_failure_tolerance(gateway_factory, num_servers):
    """
    When one edge in a multi-edge setup dies, other edges still produce data
    via the gateway, and the stopped module's frames eventually stop appearing.
    """
    gw = await gateway_factory(num_servers)
    edge_details = gw["edge_details"]
    stopped = edge_details[0]

    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_sim() is True

        stream = client.stream_images(
            stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
        )

        # First, prove all modules are active
        expected_modules = {d["module_id"] for d in edge_details}
        seen_before = set()
        for _ in range(num_servers * 6):
            img = await stream.__anext__()
            seen_before.add(img["module_id"])
            if seen_before == expected_modules:
                break
        assert seen_before == expected_modules

        # Stop one edge
        stopped["stop_event"].set()
        await asyncio.wait_for(stopped["task"], timeout=5.0)

        # Drain: collect frames from remaining edges to clear the buffer
        remaining_modules = {d["module_id"] for d in edge_details if d["module_id"] != stopped["module_id"]}
        drain_counts = {mid: 0 for mid in remaining_modules}
        for _ in range(100):
            try:
                img = await stream.__anext__()
                if img["module_id"] in drain_counts:
                    drain_counts[img["module_id"]] += 1
                if all(c >= 3 for c in drain_counts.values()):
                    break
            except (StopAsyncIteration, PanosetiRpcError):
                break

        # Collect post-failure frames
        seen_after = set()
        for _ in range((num_servers - 1) * 5):
            try:
                img = await stream.__anext__()
                seen_after.add(img["module_id"])
            except (StopAsyncIteration, PanosetiRpcError):
                break

        assert stopped["module_id"] not in seen_after
        assert seen_after.issubset(remaining_modules)
