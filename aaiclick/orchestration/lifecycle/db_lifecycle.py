"""
aaiclick.orchestration.db_lifecycle - Database models for distributed lifecycle tracking.

DBLifecycleOp, DBLifecycleMessage, and TableContextRef define the data structures
used by OrchLifecycleHandler (in context.py) for distributed table reference counting
and oplog recording.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import ClassVar

from sqlalchemy import BigInteger, Column, DateTime, String, Text, delete, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Field, SQLModel, col, select

from aaiclick.orchestration.models import LIVE_TASK_STATUSES, Task


class DBLifecycleOp(Enum):
    """Operations for the distributed lifecycle handler."""

    INCREF = auto()
    DECREF = auto()
    PIN = auto()
    UNPIN = auto()
    OPLOG_RECORD = auto()
    OPLOG_TABLE = auto()
    FLUSH = auto()
    SHUTDOWN = auto()


@dataclass
class OplogPayload:
    """Payload for OPLOG_RECORD messages."""

    result_table: str
    operation: str
    kwargs: dict[str, str]
    sql: str | None = None
    task_id: int | None = None
    job_id: int | None = None
    run_id: int | None = None


@dataclass
class OplogTablePayload:
    """Payload for OPLOG_TABLE messages."""

    table_name: str
    task_id: int | None = None
    job_id: int | None = None
    run_id: int | None = None
    schema_doc: str | None = None


@dataclass
class DBLifecycleMessage:
    """Message passed to handler via queue."""

    op: DBLifecycleOp
    table_name: str = ""
    pin_task_id: int | None = None
    oplog: OplogPayload | None = None
    oplog_table: OplogTablePayload | None = None
    flush_event: asyncio.Event | None = None  # signalled after FLUSH reaches here


class TableContextRef(SQLModel, table=True):
    """Registry of tracked ClickHouse tables.

    Composite PK (table_name, context_id) allows multiple contexts to
    register the same table independently.

    advisory_id is a 64-bit Snowflake ID used as the pg_advisory_lock key
    that serializes concurrent inserts into the same shared CH table in
    distributed mode.  All rows sharing the same table_name MUST carry the
    same advisory_id; the invariant is enforced in OrchLifecycleHandler's
    INCREF handler, not in the DB schema.
    """

    __tablename__: ClassVar[str] = "table_context_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    context_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    advisory_id: int = Field(sa_column=Column(BigInteger, nullable=False))


class TablePinRef(SQLModel, table=True):
    """Junction table: which consumer tasks hold a pin on which tables.

    Producer inserts one row per downstream consumer task. Each consumer
    deletes its own row during deserialization (after incref commits the
    run_ref). Table is droppable when no pin_refs AND no run_refs remain.
    """

    __tablename__: ClassVar[str] = "table_pin_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    task_id: int = Field(sa_column=Column(BigInteger, primary_key=True))


class TableRunRef(SQLModel, table=True):
    """Junction table: which run_ids hold a reference to which tables.

    incref inserts a row; decref deletes it.  clean_task_run deletes all
    rows for a given run_id (crash recovery).  When no rows remain for a
    table (and no pin in table_context_refs), the background worker drops
    the ClickHouse table.
    """

    __tablename__: ClassVar[str] = "table_run_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    run_id: str = Field(sa_column=Column(String, primary_key=True))


class TaskNameLock(SQLModel, table=True):
    """One row per ``(job_id, name)`` held by a live task.

    Acquired by ``task_scope()`` when a task creates ``j_<id>_<name>`` for a
    name not in the job's ``preserve`` list. Released on task exit (success or
    failure) and by the BackgroundWorker dead-worker sweep.
    """

    __tablename__: ClassVar[str] = "task_name_locks"

    job_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    name: str = Field(sa_column=Column(String, primary_key=True))
    task_id: int = Field(sa_column=Column(BigInteger, nullable=False, index=True))
    acquired_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, nullable=False),
    )


class TableNameCollision(ValueError):
    """Raised when a task takes a non-preserved name held by another live task in the same job."""

    def __init__(self, name: str, held_by_task_id: int):
        self.name = name
        self.held_by_task_id = held_by_task_id
        super().__init__(f"non-preserved table name {name!r} is held by live task id={held_by_task_id}")


async def acquire_task_name_lock(
    session: AsyncSession,
    *,
    job_id: int,
    name: str,
    task_id: int,
) -> None:
    """Take the ``(job_id, name)`` lock for ``task_id``.

    Idempotent for the same task. Raises :class:`TableNameCollision` if a
    different task currently holds the lock. Atomic via ``ON CONFLICT DO NOTHING``
    — concurrent acquirers serialize on the primary key without a SELECT/INSERT
    race window.
    """
    result = await session.execute(
        text(
            "INSERT INTO task_name_locks (job_id, name, task_id, acquired_at) "
            "VALUES (:job_id, :name, :task_id, :acquired_at) "
            "ON CONFLICT (job_id, name) DO NOTHING "
            "RETURNING task_id"
        ),
        {
            "job_id": job_id,
            "name": name,
            "task_id": task_id,
            "acquired_at": datetime.utcnow(),
        },
    )
    if result.scalar_one_or_none() is not None:
        return
    holder_id = (
        await session.execute(
            select(TaskNameLock.task_id).where(
                TaskNameLock.job_id == job_id,
                TaskNameLock.name == name,
            )
        )
    ).scalar_one()
    if holder_id == task_id:
        return
    raise TableNameCollision(name=name, held_by_task_id=holder_id)


async def release_task_name_locks_for_task(session: AsyncSession, *, task_id: int) -> None:
    """Drop every lock held by ``task_id``."""
    await session.execute(delete(TaskNameLock).where(TaskNameLock.task_id == task_id))


async def release_task_name_locks_for_dead_tasks(session: AsyncSession) -> None:
    """Drop locks whose holding task is no longer in a live status."""
    live_task_ids = select(Task.id).where(col(Task.status).in_(LIVE_TASK_STATUSES))
    await session.execute(delete(TaskNameLock).where(col(TaskNameLock.task_id).notin_(live_task_ids)))


class TableRegistry(SQLModel, table=True):
    """Ownership metadata for every ClickHouse data table aaiclick creates.

    One row per table, keyed by ``table_name`` (strict 1:1). Written once
    when the table is created; deleted by the background worker when the
    owning job TTL-expires or (for orphans with ``job_id IS NULL``) when
    the orphan TTL expires.

    Previously lived in ClickHouse as an append-only MergeTree table.
    Moved to SQL because every consumer is a keyed lookup or owner join
    during background cleanup — not append-only audit.
    """

    __tablename__: ClassVar[str] = "table_registry"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    job_id: int | None = Field(sa_column=Column(BigInteger, nullable=True, index=True))
    task_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    run_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, nullable=False, index=True),
    )
    schema_doc: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
    )
