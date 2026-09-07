"""The per-backend contract for carrying a change signal from a commit to a bus.

Two hooks straddle the commit because the backends act at different
phases: Postgres must notify *inside* the transaction so the signal is
delivered only if the write commits, while local mode must publish *after*
the commit so a subscriber that refetches immediately sees the row.
"""

from __future__ import annotations

import asyncio
from typing import Protocol

from sqlalchemy.orm import Session

from aaiclick.backend import is_postgres

from .bus import EventBus
from .local import LocalTransport
from .postgres import PostgresTransport


class SignalTransport(Protocol):
    def before_commit(self, session: Session) -> None:
        """Runs inside the flagged transaction, before it commits."""
        ...

    def after_commit(self, session: Session) -> None:
        """Runs once the flagged transaction is durable."""
        ...

    async def feed(self, bus: EventBus, *, stop: asyncio.Event) -> None:
        """Server-side receiver: deliver signals onto ``bus`` until ``stop``.

        Publishes one signal as soon as it is ready so subscribers (and the
        server lifespan) resync without knowing which backend is active.
        """
        ...


_LOCAL = LocalTransport()
_POSTGRES = PostgresTransport()


def get_transport() -> SignalTransport:
    """The transport for the active SQL backend (``AAICLICK_SQL_URL``)."""
    return _POSTGRES if is_postgres() else _LOCAL
