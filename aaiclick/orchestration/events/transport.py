"""The per-backend contract for carrying a change signal from a commit to a bus.

Two hooks straddle the commit because the backends act at different
phases: Postgres must notify *inside* the transaction so the signal is
delivered only if the write commits, while local mode must publish *after*
the commit so a subscriber that refetches immediately sees the row.
"""

from __future__ import annotations

import asyncio
from functools import cache
from typing import Protocol

from sqlalchemy.orm import Session

from aaiclick.backend import is_postgres

from .bus import EventBus
from .local import LocalTransport
from .state import TransportState


class SignalTransport(Protocol):
    @property
    def state(self) -> TransportState:
        """Whether ``feed`` is currently delivering signals."""
        ...

    def before_commit(self, session: Session) -> None:
        """Runs inside the flagged transaction, before it commits."""
        ...

    def after_commit(self, session: Session) -> None:
        """Runs once the flagged transaction is durable."""
        ...

    async def feed(self, bus: EventBus, *, stop: asyncio.Event) -> None:
        """Server-side receiver: deliver signals onto ``bus`` until ``stop``.

        ``state`` reports ``"listening"`` while signals flow. A transport
        that can lose signals in transit publishes one on recovery so open
        streams resync.
        """
        ...


_LOCAL = LocalTransport()


@cache
def _postgres() -> SignalTransport:
    """One process-wide Postgres transport, built on first use.

    The module imports asyncpg at top level, which only the ``distributed``
    extra installs, so it is loaded here rather than at package import: a
    local-mode install never touches it.
    """
    from .postgres import PostgresTransport  # Optional dep: asyncpg is absent in local-mode installs.

    return PostgresTransport()


def get_transport() -> SignalTransport:
    """The transport for the active SQL backend (``AAICLICK_SQL_URL``)."""
    return _postgres() if is_postgres() else _LOCAL
