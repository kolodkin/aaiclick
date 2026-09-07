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
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from ..env import get_db_url
from .bus import EventBus

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
    def before_commit(self, session: Session) -> None:
        session.execute(text("SELECT pg_notify(:channel, '')"), {"channel": EVENTS_CHANNEL})

    def after_commit(self, session: Session) -> None:
        return None

    async def feed(self, bus: EventBus, *, stop: asyncio.Event) -> None:
        """Forward ``NOTIFY`` on :data:`EVENTS_CHANNEL` to ``bus`` until ``stop`` is set.

        Holds one dedicated autocommit connection (a ``LISTEN`` session must
        not sit inside a long-open transaction) and pings it every
        :data:`PING_INTERVAL` so a dead link is noticed. Reconnects with
        capped backoff and publishes one signal on every (re)connect so
        streams that lived through a gap resync.
        """
        engine = create_async_engine(get_db_url(), poolclass=NullPool)
        backoff = RECONNECT_MIN
        try:
            while not stop.is_set():
                try:
                    async with engine.connect() as conn:
                        await conn.execution_options(isolation_level="AUTOCOMMIT")
                        raw = await conn.get_raw_connection()
                        driver = cast(_Listenable, raw.driver_connection)
                        await driver.add_listener(EVENTS_CHANNEL, lambda *_: bus.publish())
                        bus.publish()
                        backoff = RECONNECT_MIN
                        while not await _wait_or_timeout(stop, PING_INTERVAL):
                            await conn.execute(text("SELECT 1"))
                except Exception:
                    logger.warning("Postgres event listener lost; reconnecting in %.0fs", backoff, exc_info=True)
                    if await _wait_or_timeout(stop, backoff):
                        return
                    backoff = min(backoff * 2, RECONNECT_MAX)
        finally:
            await engine.dispose()
