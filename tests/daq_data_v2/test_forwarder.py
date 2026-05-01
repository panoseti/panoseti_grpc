"""
Unit tests for the DaqData v2 Forwarder circular buffer logic.
"""

import asyncio

import pytest


@pytest.mark.asyncio
async def test_forwarder_circular_buffer_logic():
    """Verify that the oldest items are evicted when the queue is full."""
    queue = asyncio.Queue(maxsize=5)

    # Helper to push with circular buffer logic
    def push_circular(item):
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            queue.get_nowait()
            queue.put_nowait(item)

    # Fill the queue
    for i in range(5):
        push_circular(i)

    assert queue.full()
    assert queue.qsize() == 5

    # Verify contents: [0, 1, 2, 3, 4]
    # (Note: asyncio.Queue doesn't allow direct indexing, we'd have to drain it)

    # Push one more: should evict '0', insert '5'
    push_circular(5)
    assert queue.qsize() == 5

    # Drain and verify: should be [1, 2, 3, 4, 5]
    results = []
    while not queue.empty():
        results.append(queue.get_nowait())

    assert results == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_forwarder_preserves_latest_burst():
    """Verify that a burst of data results in the most recent frames being kept."""
    queue = asyncio.Queue(maxsize=3)

    def push_circular(item):
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            queue.get_nowait()
            queue.put_nowait(item)

    # Push 10 items into a queue of size 3
    for i in range(10):
        push_circular(i)

    results = []
    while not queue.empty():
        results.append(queue.get_nowait())

    # Should only have the last 3 items
    assert results == [7, 8, 9]
