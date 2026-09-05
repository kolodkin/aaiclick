"""UI change signals: an in-process bus plus the session listeners that feed it.

Any transaction that writes ``jobs``, ``tasks`` or ``groups`` publishes one
signal when it commits — ``pg_notify`` inside the same transaction on
Postgres, the context bus after commit on SQLite. Local mode runs the
workers inside the server process, so the in-process bus is exact there;
distributed mode fans out through Postgres: every API host holds one
``LISTEN`` connection (:func:`listen_postgres`) and forwards notifications
onto its own bus.

Signals carry no payload. The browser invalidates its query cache and REST
supplies authoritative state, so a burst of writes collapses into one
pending signal per subscriber and nothing tenant-specific ever crosses the
channel. Detection hooks the SQLAlchemy ``Session`` rather than each write
site: roughly twenty call sites mutate these tables, many through raw SQL,
and a hook covers the ones not written yet.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncGenerator, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Protocol, cast

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import ORMExecuteState, Session, UOWTransaction
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import TableClause
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import TextClause

from aaiclick.backend import is_postgres

from .env import get_db_url
from .models import Group, Job, Task

logger = logging.getLogger(__name__)

EVENTS_CHANNEL = "aaiclick_events"
WATCHED_TABLES = ("jobs", "tasks", "groups")
PING_INTERVAL = 30.0
RECONNECT_MIN = 1.0
RECONNECT_MAX = 30.0

_WATCHED_MODELS = (Job, Task, Group)
_WRITE_RE = re.compile(
    r"^\s*(?:insert\s+into|update|delete\s+from)\s+\"?(?:" + "|".join(WATCHED_TABLES) + r")\b",
    re.IGNORECASE,
)
_DIRTY_KEY = "aaiclick_events_dirty"


class EventBus:
    """Process-local pub/sub for change signals.

    Each subscriber owns a queue of depth one, so however many commits land
    while it is busy, it sees a single pending signal. ``close`` ends every
    subscription (server shutdown) and turns later publishes into no-ops.
    """

    def __init__(self) -> None:
        self._queues: set[asyncio.Queue[bool]] = set()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def publish(self) -> None:
        if self._closed:
            return
        for queue in list(self._queues):
            _offer(queue, True)

    def close(self) -> None:
        self._closed = True
        for queue in list(self._queues):
            _offer(queue, False)

    async def subscribe(self) -> AsyncGenerator[None, None]:
        """Yield once per pending signal until the bus closes."""
        queue: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)
        self._queues.add(queue)
        try:
            while not (self._closed and queue.empty()):
                if not await queue.get():
                    return
                yield None
        finally:
            self._queues.discard(queue)


def _offer(queue: asyncio.Queue[bool], item: bool) -> None:
    """Enqueue without blocking; a full queue already holds a pending wake-up."""
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        pass


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


def statement_touches_watched(sql: str) -> bool:
    """True for a textual INSERT / UPDATE / DELETE against a watched table."""
    return _WRITE_RE.match(sql) is not None


def _statement_writes_watched(statement: object) -> bool:
    """Raw ``text()`` and Core ``insert/update/delete`` statements both bypass
    the unit of work, so they are inspected here rather than at flush."""
    if isinstance(statement, TextClause):
        return statement_touches_watched(statement.text)
    if isinstance(statement, UpdateBase):
        table = statement.table
        return isinstance(table, TableClause) and table.name in WATCHED_TABLES
    return False


@event.listens_for(Session, "do_orm_execute")
def _flag_statement_writes(state: ORMExecuteState) -> None:
    if _statement_writes_watched(state.statement):
        state.session.info[_DIRTY_KEY] = True


@event.listens_for(Session, "before_flush")
def _flag_orm_writes(session: Session, flush_context: UOWTransaction, instances: object) -> None:
    pending = (*session.new, *session.dirty, *session.deleted)
    if any(isinstance(obj, _WATCHED_MODELS) for obj in pending):
        session.info[_DIRTY_KEY] = True


@event.listens_for(Session, "before_commit")
def _notify_postgres(session: Session) -> None:
    # Flush first so ORM writes still pending in this commit set the flag.
    session.flush()
    if session.info.get(_DIRTY_KEY) and is_postgres():
        session.execute(text("SELECT pg_notify(:channel, '')"), {"channel": EVENTS_CHANNEL})


@event.listens_for(Session, "after_commit")
def _publish_local(session: Session) -> None:
    if session.info.pop(_DIRTY_KEY, False) and not is_postgres():
        get_event_bus().publish()


@event.listens_for(Session, "after_rollback")
def _discard_flag(session: Session) -> None:
    session.info.pop(_DIRTY_KEY, None)


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


async def listen_postgres(bus: EventBus, *, stop: asyncio.Event) -> None:
    """Forward ``NOTIFY`` on :data:`EVENTS_CHANNEL` to ``bus`` until ``stop`` is set.

    Holds one dedicated autocommit connection (a ``LISTEN`` session must not
    sit inside a long-open transaction) and pings it every
    :data:`PING_INTERVAL` so a dead link is noticed. Reconnects with capped
    backoff and publishes one signal on every (re)connect so streams that
    lived through a gap resync.
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
