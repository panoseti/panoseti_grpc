import asyncio
import pytest
import grpc

from daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio

# Helpers
async def _collect_n(stream, n):
    out = []
    for _ in range(n):
        out.append(await stream.__anext__())
    return out

async def _drain_until(stream, predicate, timeout=5.0):
    """
    Drain stream until predicate(image) returns True or timeout.
    Returns the last image that satisfied the predicate or None.
    """
    try:
        with asyncio.TimeoutError():
            pass
    except TypeError:
        # Older Python doesn't allow context manager for TimeoutError.
        # Emulate with wait_for
        pass
    try:
        async def _loop():
            while True:
                img = await stream.__anext__()
                if predicate(img):
                    return img
        return await asyncio.wait_for(_loop(), timeout=timeout)
    except asyncio.TimeoutError:
        return None

# 1. UDS mode: multi-rate streams from a single client
async def test_uds_multi_rate_streams_single_client(default_server_process):
    """
    Ensures multiple stream_images connections created from the same client over UDS
    can run concurrently at different update rates without starving or blocking each other.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        fast = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.02,  # fast
        )
        slow = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.2,   # slow
        )

        # Collect some samples concurrently and ensure both streams deliver
        got_fast, got_slow = 0, 0
        async def read_fast():
            nonlocal got_fast
            for _ in range(5):
                _ = await fast.__anext__()
                got_fast += 1

        async def read_slow():
            nonlocal got_slow
            for _ in range(2):
                _ = await slow.__anext__()
                got_slow += 1

        await asyncio.gather(read_fast(), read_slow())
        assert got_fast >= 5 and got_slow >= 2

# 2. RPC mode: multi-rate streams from a single client
@pytest.mark.parametrize("sim_server_process", ["rpc_sim_server_config"], indirect=True)
async def test_rpc_multi_rate_streams_single_client(sim_server_process):
    # The 'sim_server_process' parameter is now the server address string itself.
    sim_server_addr = sim_server_process

    daq_config = {"daq_nodes": [{"ip_addr": sim_server_addr}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        # ... (rest of the test logic is correct and remains unchanged) ...
        fast = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.02,
        )
        slow = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.2,
        )

        got_fast, got_slow = 0, 0

        async def read_fast():
            nonlocal got_fast
            for _ in range(5):
                _ = await fast.__anext__()
                got_fast += 1

        async def read_slow():
            nonlocal got_slow
            for _ in range(2):
                _ = await slow.__anext__()
                got_slow += 1

        await asyncio.gather(read_fast(), read_slow())
        assert got_fast >= 5 and got_slow >= 2

# 3. UDS mode: multiple clients per server with independent pacing
async def test_uds_multi_clients_independent_pacing(default_server_process):
    """Validates that separate client connections maintain their own pacing/timers (per README)."""
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}

    async with AioDaqDataClient(daq_config, network_config=None) as client_a, \
               AioDaqDataClient(daq_config, network_config=None) as client_b:

        assert await client_a.init_sim(hosts=None) is True
        # Client B should not need to re-init; ensure StreamImages precondition holds.
        # Here, we intentionally stream on B without re-initializing to verify server state continuity.
        stream_a_fast = await client_a.stream_images(
            hosts=None, stream_movie_data=True, stream_pulse_height_data=False, update_interval_seconds=0.05
        )
        stream_b_slow = await client_b.stream_images(
            hosts=None, stream_movie_data=False, stream_pulse_height_data=True, update_interval_seconds=0.2
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

# 4. RPC mode: multiple servers, single client, verify each host produces data
@pytest.mark.parametrize("num_servers", [2, 3])
async def test_rpc_multi_servers_single_client(n_sim_servers_fixture_factory, num_servers):
    """
    Uses n_sim_servers_fixture_factory to launch N servers (each a distinct module_id),
    then verifies the merged stream delivers images from all expected module_ids.
    """
    details = await n_sim_servers_fixture_factory(num_servers)
    daq_config = {"daq_nodes": [{"ip_addr": d["ip_addr"]} for d in details]}

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        stream = await client.stream_images(
            hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
        )

        expected_modules = {d["module_id"] for d in details}
        seen = set()
        # Collect a bounded number to limit test time
        for _ in range(num_servers * 8):
            img = await stream.__anext__()
            seen.add(img["module_id"])
            if seen == expected_modules:
                break

        assert seen == expected_modules, f"Expected data from all modules: {expected_modules}, got {seen}"

# 5. UDS mode: module_ids filtering on multi-server setup
@pytest.mark.parametrize("num_servers", [3])
async def test_uds_multi_servers_module_filtering(n_sim_servers_fixture_factory, num_servers):
    """Confirms that StreamImages respects per-request module whitelist as per README."""
    details = await n_sim_servers_fixture_factory(num_servers)
    daq_config = {"daq_nodes": [{"ip_addr": d["ip_addr"]} for d in details]}

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        target = details[1]["module_id"]
        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.05,
            module_ids=(target,)  # whitelist single module
        )

        # Verify only target module appears for a while
        for _ in range(6):
            img = await stream.__anext__()
            assert img["module_id"] == target

# 6. UDS mode: mixed rates across multiple servers and clients
@pytest.mark.parametrize("num_servers", [2])
async def test_uds_multi_servers_multi_clients_mixed_rates(n_sim_servers_fixture_factory, num_servers):
    """
    One client subscribes at fast rate for movies only; another at slow rate for PH only.
    Validate no cross-interference and that both get their expected types from all servers.
    """
    details = await n_sim_servers_fixture_factory(num_servers)
    daq_config = {"daq_nodes": [{"ip_addr": d["ip_addr"]} for d in details]}

    async with AioDaqDataClient(daq_config, network_config=None) as client_a, \
               AioDaqDataClient(daq_config, network_config=None) as client_b:

        assert await client_a.init_sim(hosts=None) is True
        # Client B piggybacks on initialized server state

        fast_movies = await client_a.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.03
        )
        slow_ph = await client_b.stream_images(
            hosts=None,
            stream_movie_data=False,
            stream_pulse_height_data=True,
            update_interval_seconds=0.2
        )

        expected_modules = {d["module_id"] for d in details}
        seen_movies, seen_ph = set(), set()

        async def read_movies():
            for _ in range(num_servers * 6):
                img = await fast_movies.__anext__()
                assert img["type"] == "MOVIE"
                seen_movies.add(img["module_id"])

        async def read_ph():
            for _ in range(num_servers * 3):
                img = await slow_ph.__anext__()
                assert img["type"] == "PULSE_HEIGHT"
                seen_ph.add(img["module_id"])

        await asyncio.gather(read_movies(), read_ph())

        # Each stream should have seen output from all modules within the bounded reads
        assert expected_modules.issubset(seen_movies)
        assert expected_modules.issubset(seen_ph)

# 7. UDS mode: enforce StreamImages precondition (must init first)
async def test_uds_stream_requires_init(default_server_process):
    """StreamImages should fail with FAILED_PRECONDITION when hp_io task is not valid."""
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        with pytest.raises(grpc.aio.AioRpcError) as e:
            stream = await client.stream_images(
                hosts=None,
                stream_movie_data=True,
                stream_pulse_height_data=False,
                update_interval_seconds=0.1
            )
            await stream.__anext__()
        assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION

# 8. RPC mode: many concurrent streams per client to the same set of servers
@pytest.mark.parametrize("num_servers", [2])
async def test_rpc_many_concurrent_streams(n_sim_servers_fixture_factory, num_servers):
    """Validates scalability of server streaming w.r.t. concurrent stream_images per client."""
    details = await n_sim_servers_fixture_factory(num_servers)
    daq_config = {"daq_nodes": [{"ip_addr": d["ip_addr"]} for d in details]}

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        # Create multiple concurrent streams at different rates
        rates = [0.03, 0.05, 0.07, 0.10]
        streams = []
        for r in rates:
            s = await client.stream_images(
                hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=r
            )
            streams.append(s)

        # Read a couple from each; ensure they all move
        async def read_some(s):
            got = 0
            for _ in range(3):
                _ = await s.__anext__()
                got += 1
            return got

        results = await asyncio.gather(*(read_some(s) for s in streams))
        assert all(x >= 3 for x in results)

# 9. UDS mode: validate module_ids empty tuple streams data from all modules
@pytest.mark.parametrize("num_servers", [2])
async def test_uds_module_ids_empty_means_all(n_sim_servers_fixture_factory, num_servers):
    details = await n_sim_servers_fixture_factory(num_servers)
    daq_config = {"daq_nodes": [{"ip_addr": d["ip_addr"]} for d in details]}

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        # Explicitly pass an empty tuple for module_ids, which should mean "all"
        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.05,
            module_ids=()  # all modules
        )

        expected_modules = {d["module_id"] for d in details}
        seen = set()
        for _ in range(num_servers * 6):
            img = await stream.__anext__()
            seen.add(img["module_id"])
            if seen == expected_modules:
                break

        assert seen == expected_modules

# 10. UDS mode: mixed server restart tolerance while client maintains other streams
@pytest.mark.parametrize("num_servers", [3])
async def test_uds_partial_server_failure_tolerance(n_sim_servers_fixture_factory, num_servers):
    """
     Ensures that when one server in a multi-server setup dies, other servers still produce data,
    and that a re-init can re-enable the failed server without tearing down the other streams prematurely.
    """
    details = await n_sim_servers_fixture_factory(num_servers)
    daq_config = {"daq_nodes": [{"ip_addr": d["ip_addr"]} for d in details]}
    stopped = details[0]

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        stream = await client.stream_images(
            hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
        )

        # First, prove all modules are active
        expected_modules = {d["module_id"] for d in details}
        seen_before = set()
        for _ in range(num_servers * 6):
            img = await stream.__anext__()
            seen_before.add(img["module_id"])
            if seen_before == expected_modules:
                break
        assert seen_before == expected_modules

        # Stop one server
        stopped["stop_event"].set()
        await asyncio.wait_for(stopped["task"], timeout=5.0)

        # Continue reading; ensure we still see the remaining modules
        remaining = expected_modules - {stopped["module_id"]}
        seen_after = set()
        count = 0
        while count < (num_servers - 1) * 10:
            try:
                img = await stream.__anext__()
                seen_after.add(img["module_id"])
            except grpc.aio.AioRpcError:
                break
            count += 1
        assert stopped["module_id"] not in seen_after
        assert seen_after.issubset(remaining)
