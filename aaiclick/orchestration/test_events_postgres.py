"""Postgres-only transport tests: need asyncpg and a live ``LISTEN`` connection.

Kept apart from ``test_events.py`` because importing the Postgres transport
needs asyncpg; ``conftest.py`` skips collecting this module when the
``distributed`` extra is absent.
"""

import asyncio

import pytest
from sqlalchemy import text

from aaiclick.backend import is_postgres

from .events import EventBus, get_transport
from .events import postgres as postgres_transport
from .events.postgres import PostgresTransport
from .orch_context import get_sql_session
from .test_events import SETTLE, _wait_listening, recording

pytestmark = pytest.mark.skipif(not is_postgres(), reason="Postgres backend only")


def test_get_transport_is_postgres():
    assert isinstance(get_transport(), PostgresTransport)


async def test_feed_resyncs_after_reconnect(orch_ctx, monkeypatch):
    """Notifications sent while the LISTEN connection is down are lost, so a
    reconnect must publish one signal for open streams to catch up on."""
    monkeypatch.setattr(postgres_transport, "PING_INTERVAL", 0.1)
    monkeypatch.setattr(postgres_transport, "RECONNECT_MIN", 0.1)
    transport = PostgresTransport()
    bus = EventBus()
    stop = asyncio.Event()
    feed = asyncio.create_task(transport.feed(bus, stop=stop))
    await _wait_listening(transport)
    async with recording(bus) as signals:
        async with get_sql_session() as session:
            await session.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                )
            )
        await _wait_listening(transport)
        await asyncio.sleep(SETTLE)
    stop.set()
    await feed
    assert len(signals) == 1
