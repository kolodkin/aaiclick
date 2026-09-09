"""Distributed mode (Postgres): ``NOTIFY`` on commit, one ``LISTEN`` per API host.

``pg_notify`` runs inside the committing transaction, so Postgres delivers
the signal only if the write commits and a client that refetches on it
always sees the committed row. Postgres fans each ``NOTIFY`` out to every
connection that has issued ``LISTEN``, so N API hosts hold N connections
and no broker is needed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Protocol, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from ..env import get_db_url
from .bus import EventBus
from .state import STATE_IDLE, STATE_LISTENING, STATE_RECONNECTING, TransportState

logger = logging.getLogger(__name__)

EVENTS_CHANNEL = "aaiclick_events"
PING_INTERVAL = 30.0
RECONNECT_MIN = 1.0
RECONNECT_MAX = 30.0


# The asyncpg surface used below, so the module needs no asyncpg import (the
# ``distributed`` extra is optional; SQLAlchemy hands back the driver
# connection untyped).
class _Listenable(Protocol):
    async def add_listener(self, channel: str, callback: Callable[[object, int, str, str], object]) -> None: ...


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

        Supervisor: one :meth:`_listen` per connection lifetime, a backoff
        wait between failures, and the engine disposed on the way out.
        """
        engine = create_async_engine(get_db_url(), poolclass=NullPool)
        try:
            while not stop.is_set():
                try:
                    await self._listen(engine, bus, stop)
                except Exception:
                    if await self._back_off(stop):
                        return
        finally:
            self._state = STATE_IDLE
            await engine.dispose()

    async def _listen(self, engine: AsyncEngine, bus: EventBus, stop: asyncio.Event) -> None:
        """Hold one ``LISTEN`` connection until ``stop``; raise on any failure.

        Autocommit, because a ``LISTEN`` session must not sit inside a
        long-open transaction. The asyncpg listener publishes on every
        notification; the loop only keeps the link alive.
        """
        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            raw = await conn.get_raw_connection()
            driver = cast(_Listenable, raw.driver_connection)
            await driver.add_listener(EVENTS_CHANNEL, lambda *_: bus.publish())
            self._connected(bus)
            await self._keep_alive(conn, stop)

    def _connected(self, bus: EventBus) -> None:
        """Mark listening; after a gap, publish once so open streams resync,
        since notifications sent while the link was down are lost."""
        if self._state == STATE_RECONNECTING:
            bus.publish()
        self._state = STATE_LISTENING
        self._backoff = RECONNECT_MIN

    async def _keep_alive(self, conn: AsyncConnection, stop: asyncio.Event) -> None:
        """Ping every :data:`PING_INTERVAL` so a dead socket surfaces as an
        exception instead of a silent wait; return when ``stop`` is set."""
        while not await _wait_or_timeout(stop, PING_INTERVAL):
            await conn.execute(text("SELECT 1"))

    async def _back_off(self, stop: asyncio.Event) -> bool:
        """Wait out the current backoff, doubling it up to :data:`RECONNECT_MAX`.
        True if ``stop`` was set during the wait."""
        self._state = STATE_RECONNECTING
        logger.warning("Postgres event listener lost; reconnecting in %.1fs", self._backoff, exc_info=True)
        stopped = await _wait_or_timeout(stop, self._backoff)
        self._backoff = min(self._backoff * 2, RECONNECT_MAX)
        return stopped
