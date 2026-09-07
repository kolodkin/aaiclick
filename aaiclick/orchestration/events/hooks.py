"""Session listeners that turn a job/task/group write into a change signal.

Detection hooks the SQLAlchemy ``Session`` rather than each write site:
roughly twenty call sites mutate these tables, many through raw SQL, and a
hook covers the ones not written yet. A flagged transaction hands off to the
active :class:`~.transport.SignalTransport` on either side of its commit.

The listeners are process-global (SQLAlchemy listens on the ``Session``
class), so :func:`register_session_hooks` is idempotent and is called from
the two entry points every writer passes through: ``orch_context()`` and
``BackgroundWorker.start()``.
"""

from __future__ import annotations

import re

from sqlalchemy import event
from sqlalchemy.orm import ORMExecuteState, Session, UOWTransaction
from sqlalchemy.sql import TableClause
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import TextClause

from ..models import Group, Job, Task
from .transport import get_transport

WATCHED_TABLES = ("jobs", "tasks", "groups")

_WATCHED_MODELS = (Job, Task, Group)
_WRITE_RE = re.compile(
    r"^\s*(?:insert\s+into|update|delete\s+from)\s+\"?(?:" + "|".join(WATCHED_TABLES) + r")\b",
    re.IGNORECASE,
)
_DIRTY_KEY = "aaiclick_events_dirty"


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


def _flag_statement_writes(state: ORMExecuteState) -> None:
    if _statement_writes_watched(state.statement):
        state.session.info[_DIRTY_KEY] = True


def _flag_orm_writes(session: Session, flush_context: UOWTransaction, instances: object) -> None:
    pending = (*session.new, *session.dirty, *session.deleted)
    if any(isinstance(obj, _WATCHED_MODELS) for obj in pending):
        session.info[_DIRTY_KEY] = True


def _before_commit(session: Session) -> None:
    # Flush first so ORM writes still pending in this commit set the flag.
    session.flush()
    if session.info.get(_DIRTY_KEY):
        get_transport().before_commit(session)


def _after_commit(session: Session) -> None:
    if session.info.pop(_DIRTY_KEY, False):
        get_transport().after_commit(session)


def _discard_flag(session: Session) -> None:
    session.info.pop(_DIRTY_KEY, None)


_LISTENERS = (
    ("do_orm_execute", _flag_statement_writes),
    ("before_flush", _flag_orm_writes),
    ("before_commit", _before_commit),
    ("after_commit", _after_commit),
    ("after_rollback", _discard_flag),
)


def register_session_hooks() -> None:
    """Attach the listeners to every ``Session`` in this process. Idempotent."""
    for name, fn in _LISTENERS:
        if not event.contains(Session, name, fn):
            event.listen(Session, name, fn)


def unregister_session_hooks() -> None:
    """Detach the listeners (test isolation). Idempotent."""
    for name, fn in _LISTENERS:
        if event.contains(Session, name, fn):
            event.remove(Session, name, fn)
