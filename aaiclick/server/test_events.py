"""HTTP plumbing tests for the ``/events`` SSE stream."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from aaiclick.auth import config
from aaiclick.orchestration.events import EventBus

from . import events
from .app import API_PREFIX, app

EVENTS_URL = f"{API_PREFIX}/events"


@pytest.fixture
async def bus() -> AsyncIterator[EventBus]:
    """A fresh bus installed where the endpoint looks for it; closed on exit
    so an open stream ends and ``ASGITransport`` can hand back the body."""
    scoped = EventBus()
    app.state.event_bus = scoped
    yield scoped
    scoped.close()
    del app.state.event_bus


async def _stream(client: httpx.AsyncClient, bus: EventBus, settle: float = 0.2) -> asyncio.Task[httpx.Response]:
    """Open the stream and give the handler time to subscribe."""
    request = asyncio.create_task(client.get(EVENTS_URL))
    await asyncio.sleep(settle)
    return request


async def _finish(request: asyncio.Task[httpx.Response], bus: EventBus) -> httpx.Response:
    bus.close()
    return await asyncio.wait_for(request, 5)


@pytest.mark.skipif(not config.auth_enabled(), reason="auth is open in local mode")
async def test_events_requires_principal(anon_client):
    response = await anon_client.get(EVENTS_URL)
    assert response.status_code == 401


async def test_events_stream_frames_a_change(app_client, bus):
    request = await _stream(app_client, bus)
    bus.publish()
    await asyncio.sleep(0.1)
    response = await _finish(request, bus)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text.count("event: changed") == 1


async def test_events_stream_rate_floor_collapses_bursts(app_client, bus, monkeypatch):
    """Signals that land while a stream sits out the floor become one frame."""
    monkeypatch.setattr(events, "MIN_FRAME_INTERVAL", 0.2)
    request = await _stream(app_client, bus)
    bus.publish()
    await asyncio.sleep(0.05)
    for _ in range(5):
        bus.publish()
    await asyncio.sleep(0.4)
    response = await _finish(request, bus)
    assert response.text.count("event: changed") == 2


async def test_events_stream_sends_keepalive_comments(app_client, bus, monkeypatch):
    monkeypatch.setattr(events, "KEEPALIVE_INTERVAL", 0.05)
    request = await _stream(app_client, bus)
    response = await _finish(request, bus)
    assert ": keepalive" in response.text
    assert "event: changed" not in response.text
