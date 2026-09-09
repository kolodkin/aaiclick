"""Distributed mode (Postgres): ``NOTIFY`` on commit, one ``LISTEN`` per API host.

``pg_notify`` runs inside the committing transaction, so Postgres delivers
the signal only if the write commits and a client that refetches on it
always sees the committed row. Postgres fans each ``NOTIFY`` out to every
connection that has issued ``LISTEN``, so N API hosts hold N connections
and no broker is needed.

The sender side goes through the writer's SQLAlchemy session because it must
share that transaction. The receiver side talks to asyncpg directly: a
``LISTEN`` connection is a driver feature, and SQLAlchemy would only be
unwrapped to reach it. This module is imported only when the backend is
Postgres (see ``transport.get_transport``), so asyncpg is a hard import here.
"""

from __future__ import annotations

import asyncio
import logging

import asyncpg
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from ..env import get_db_url
from .bus import EventBus
from .state import STATE_IDLE, STATE_LISTENING, STATE_RECONNECTING, TransportState

logger = logging.getLogger(__name__)

EVENTS_CHANNEL = "aaiclick_events"
PING_INTERVAL = 30.0
RECONNECT_MIN = 1.0
RECONNECT_MAX = 30.0


def _dsn() -> str:
    """``AAICLICK_SQL_URL`` without the SQLAlchemy driver suffix, for asyncpg."""
    return make_url(get_db_url()).set(drivername="postgresql").render_as_string(hide_password=False)


async def _wait_or_timeout(stop: asyncio.Event, timeout: float) -> bool:
    """True once ``stop`` is set, False when ``timeout`` elapses first."""
    try:
        await asyncio.wait_for(stop.wait(), timeout)
    except asyncio.TimeoutError:
        return False
    return True


class PostgresTransport:
    def __init__(self) -> None:
        self._state: TransportState = STATE_IDLE
        self._backoff = RECONNECT_MIN

    @property
    def state(self) -> TransportState:
        return self._state

    def before_commit(self, session: Session) -> None:
        session.execute(text("SELECT pg_notify(:channel, '')"), {"channel": EVENTS_CHANNEL})

    def after_commit(self, session: Session) -> None:
        return None

    async def feed(self, bus: EventBus, *, stop: asyncio.Event) -> None:
        """Forward ``NOTIFY`` on :data:`EVENTS_CHANNEL` to ``bus`` until ``stop`` is set.

        Supervisor: one :meth:`_listen` per connection lifetime and a backoff
        wait between failures.
        """
        try:
            while not stop.is_set():
                try:
                    await self._listen(bus, stop)
                except Exception:
                    if await self._back_off(stop):
                        return
        finally:
            self._state = STATE_IDLE

    async def _listen(self, bus: EventBus, stop: asyncio.Event) -> None:
        """Hold one ``LISTEN`` connection until ``stop``; raise on any failure.

        asyncpg connections are autocommit outside an explicit transaction,
        which is what a long-lived ``LISTEN`` session needs. The listener
        callback publishes on every notification; the loop only keeps the
        link alive.
        """
        conn = await asyncpg.connect(_dsn())
        try:
            await conn.add_listener(EVENTS_CHANNEL, lambda *_: bus.publish())
            self._connected(bus)
            await self._keep_alive(conn, stop)
        finally:
            conn.terminate()

    def _connected(self, bus: EventBus) -> None:
        """Mark listening; after a gap, publish once so open streams resync,
        since notifications sent while the link was down are lost."""
        if self._state == STATE_RECONNECTING:
            bus.publish()
        self._state = STATE_LISTENING
        self._backoff = RECONNECT_MIN

    async def _keep_alive(self, conn: asyncpg.Connection, stop: asyncio.Event) -> None:
        """Ping every :data:`PING_INTERVAL` so a dead socket surfaces as an
        exception instead of a silent wait; return when ``stop`` is set."""
        while not await _wait_or_timeout(stop, PING_INTERVAL):
            await conn.execute("SELECT 1")

    async def _back_off(self, stop: asyncio.Event) -> bool:
        """Wait out the current backoff, doubling it up to :data:`RECONNECT_MAX`.
        True if ``stop`` was set during the wait."""
        self._state = STATE_RECONNECTING
        logger.warning("Postgres event listener lost; reconnecting in %.1fs", self._backoff, exc_info=True)
        stopped = await _wait_or_timeout(stop, self._backoff)
        self._backoff = min(self._backoff * 2, RECONNECT_MAX)
        return stopped
