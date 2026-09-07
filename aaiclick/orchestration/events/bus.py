"""Process-local pub/sub for UI change signals."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Iterator
from contextlib import contextmanager
from contextvars import ContextVar


class EventBus:
    """Fan a payload-less signal out to every open subscription.

    Each subscriber owns a queue of depth one, so however many commits land
    while it is busy, it sees a single pending signal. ``close`` ends every
    subscription (server shutdown) and turns later publishes into no-ops.
    """

    def __init__(self) -> None:
        self._queues: set[asyncio.Queue[bool]] = set()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def publish(self) -> None:
        if self._closed:
            return
        for queue in list(self._queues):
            _offer(queue, True)

    def close(self) -> None:
        self._closed = True
        for queue in list(self._queues):
            _offer(queue, False)

    async def subscribe(self) -> AsyncGenerator[None, None]:
        """Yield once per pending signal until the bus closes."""
        queue: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)
        self._queues.add(queue)
        try:
            while not (self._closed and queue.empty()):
                if not await queue.get():
                    return
                yield None
        finally:
            self._queues.discard(queue)


def _offer(queue: asyncio.Queue[bool], item: bool) -> None:
    """Enqueue without blocking; a full queue already holds a pending wake-up."""
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        pass


# Process-wide default on purpose: the bus is the rendezvous between the
# server's SSE streams and local-mode workers running in the same process.
# Tests scope their own with ``event_bus()``.
_DEFAULT_BUS = EventBus()
_event_bus_var: ContextVar[EventBus | None] = ContextVar("event_bus", default=None)


def get_event_bus() -> EventBus:
    return _event_bus_var.get() or _DEFAULT_BUS


@contextmanager
def event_bus(bus: EventBus) -> Iterator[None]:
    token = _event_bus_var.set(bus)
    try:
        yield
    finally:
        _event_bus_var.reset(token)
