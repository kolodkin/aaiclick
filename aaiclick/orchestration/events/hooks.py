"""Session listeners that turn a job/task/group write into a change signal.

Detection hooks the SQLAlchemy ``Session`` rather than each write site:
roughly twenty call sites mutate these tables, many through raw SQL, and a
hook covers the ones not written yet. A flagged transaction hands off to the
active :class:`~.transport.SignalTransport` on either side of its commit.
Importing this module registers the listeners.
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
def _before_commit(session: Session) -> None:
    # Flush first so ORM writes still pending in this commit set the flag.
    session.flush()
    if session.info.get(_DIRTY_KEY):
        get_transport().before_commit(session)


@event.listens_for(Session, "after_commit")
def _after_commit(session: Session) -> None:
    if session.info.pop(_DIRTY_KEY, False):
        get_transport().after_commit(session)


@event.listens_for(Session, "after_rollback")
def _discard_flag(session: Session) -> None:
    session.info.pop(_DIRTY_KEY, None)
