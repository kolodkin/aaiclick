"""Tests for the shared "event set, or timeout?" wait primitive."""

import asyncio

from aaiclick.async_wait import wait_or_timeout


async def test_returns_true_when_event_already_set():
    event = asyncio.Event()
    event.set()
    assert await wait_or_timeout(event, 5.0) is True


async def test_returns_true_when_event_set_during_wait():
    event = asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, event.set)
    assert await wait_or_timeout(event, 5.0) is True


async def test_returns_false_when_timeout_elapses_first():
    assert await wait_or_timeout(asyncio.Event(), 0.01) is False
