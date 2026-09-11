"""The per-backend contract for carrying a change signal from a commit to a bus.

Two hooks straddle the commit because the backends act at different
phases: Postgres must notify *inside* the transaction so the signal is
delivered only if the write commits, while local mode must publish *after*
the commit so a subscriber that refetches immediately sees the row.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
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


_transport_var: ContextVar[SignalTransport | None] = ContextVar("signal_transport", default=None)


def _backend_transport() -> SignalTransport:
    """A transport for the active SQL backend (``AAICLICK_SQL_URL``).

    The Postgres module imports asyncpg at top level, which only the
    ``distributed`` extra installs, so it is loaded here rather than at
    package import: a local-mode install never touches it.
    """
    if is_postgres():
        from .postgres import PostgresTransport

        return PostgresTransport()
    return LocalTransport()


def get_transport() -> SignalTransport:
    """The transport in effect: the one scoped by :func:`signal_transport`,
    else a fresh one for the active backend.

    The commit hooks need no instance state, so an unscoped caller gets a
    throwaway. A process that runs ``feed`` scopes its instance so
    everything inside sees that instance's ``state``.
    """
    return _transport_var.get() or _backend_transport()


@contextmanager
def signal_transport(transport: SignalTransport) -> Iterator[None]:
    token = _transport_var.set(transport)
    try:
        yield
    finally:
        _transport_var.reset(token)
