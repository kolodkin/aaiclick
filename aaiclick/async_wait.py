"""Shared asyncio wait primitives.

Neutral module: the poll loops that use these live in
``orchestration/background``, ``orchestration/execution``,
``orchestration/events`` and the CLI, so the helper belongs to none of them.
"""

from __future__ import annotations

import asyncio


async def wait_or_timeout(event: asyncio.Event, timeout: float) -> bool:
    """True once ``event`` is set, False when ``timeout`` elapses first.

    Lets a poll loop express "sleep, but wake early on shutdown" as its
    ``while`` condition instead of a ``try`` / ``except TimeoutError`` block.
    """
    try:
        await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        return False
    return True
