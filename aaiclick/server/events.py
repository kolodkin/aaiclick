"""``GET /events`` — server-sent change signals for the UI.

The stream carries one event kind, ``changed``, with no payload: the browser
invalidates its query cache and refetches through REST. Signals come from
:mod:`aaiclick.orchestration.events` — an in-process bus in local mode, fed
by Postgres ``LISTEN`` in distributed mode (:func:`live_events` starts that
listener with the app). Each stream forwards at most one frame per
:data:`MIN_FRAME_INTERVAL`; a burst of commits collapses into a single
refetch round, and an idle UI costs nothing.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import StreamingResponse

from aaiclick.backend import is_postgres
from aaiclick.orchestration.events import EventBus, event_bus, listen_postgres

MIN_FRAME_INTERVAL = 0.5
KEEPALIVE_INTERVAL = 15.0
CHANGED_FRAME = "event: changed\ndata: {}\n\n"
KEEPALIVE_FRAME = ": keepalive\n\n"

router = APIRouter(prefix="/events", tags=["events"])


def current_bus(request: Request) -> EventBus:
    return request.app.state.event_bus


async def event_frames(bus: EventBus) -> AsyncIterator[str]:
    """SSE frames for one subscriber: ``changed`` per signal, keepalive
    comments while idle, end when the bus closes.

    The pending ``anext`` is kept across keepalive timeouts — cancelling it
    (as ``wait_for`` would) closes the subscription generator.
    """
    subscription = bus.subscribe()
    pending = asyncio.ensure_future(anext(subscription))
    try:
        while True:
            done, _ = await asyncio.wait({pending}, timeout=KEEPALIVE_INTERVAL)
            if not done:
                yield KEEPALIVE_FRAME
                continue
            try:
                pending.result()
            except StopAsyncIteration:
                return
            yield CHANGED_FRAME
            await asyncio.sleep(MIN_FRAME_INTERVAL)
            pending = asyncio.ensure_future(anext(subscription))
    finally:
        pending.cancel()
        await subscription.aclose()


@router.get(
    "",
    summary="Stream change signals",
    description="`text/event-stream` of `changed` events; refetch after each one. Sends a keepalive comment while idle.",
    response_class=StreamingResponse,
)
async def stream_events(bus: EventBus = Depends(current_bus)) -> StreamingResponse:
    return StreamingResponse(
        event_frames(bus),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@asynccontextmanager
async def live_events(app: FastAPI) -> AsyncIterator[None]:
    """Install the app's bus for the lifespan and feed it.

    The bus sits on ``app.state`` (request handlers do not inherit the
    lifespan's contextvars) and is also entered as the context bus so
    local-mode workers started inside this block publish to it. In Postgres
    mode a ``LISTEN`` task forwards notifications. Closing the bus on exit
    ends every open stream so shutdown does not wait on them.
    """
    bus = EventBus()
    app.state.event_bus = bus
    stop = asyncio.Event()
    listener = asyncio.create_task(listen_postgres(bus, stop=stop)) if is_postgres() else None
    try:
        with event_bus(bus):
            yield
    finally:
        stop.set()
        if listener is not None:
            await listener
        bus.close()
