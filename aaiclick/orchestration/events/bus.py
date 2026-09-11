"""Process-local pub/sub for UI change signals."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from contextvars import ContextVar


class Subscription:
    """One subscriber's depth-1 mailbox.

    Depth one means a burst of publishes while the subscriber is busy leaves a
    single pending signal, not a backlog: the signal says "something changed",
    and one refetch answers any number of them.

    A queue rather than an async generator, so a consumer may abandon a
    pending :meth:`wait` — a browser closing its stream cancels one mid-wait —
    with no finalization protocol to observe. Cancelling ``Queue.get`` leaves
    any queued signal in place for the next waiter.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._queue: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)

    async def wait(self) -> bool:
        """``True`` on a change signal, ``False`` once the bus is closed and drained."""
        if self._bus.closed and self._queue.empty():
            return False
        return await self._queue.get()

    async def __aiter__(self) -> AsyncIterator[None]:
        """Yield once per signal until the bus closes."""
        while await self.wait():
            yield None

    def _offer(self, live: bool) -> None:
        """Enqueue without blocking; a full queue already holds a pending wake-up."""
        try:
            self._queue.put_nowait(live)
        except asyncio.QueueFull:
            pass


class EventBus:
    """Fan a payload-less signal out to every open subscription.

    ``close`` ends every subscription (server shutdown) and turns later
    publishes into no-ops.
    """

    def __init__(self) -> None:
        self._subs: set[Subscription] = set()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def publish(self) -> None:
        if self._closed:
            return
        for sub in list(self._subs):
            sub._offer(True)

    def close(self) -> None:
        self._closed = True
        for sub in list(self._subs):
            sub._offer(False)

    @contextmanager
    def subscription(self) -> Iterator[Subscription]:
        """Register a subscriber for the block, dropping it on any exit."""
        sub = Subscription(self)
        self._subs.add(sub)
        try:
            yield sub
        finally:
            self._subs.discard(sub)


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
