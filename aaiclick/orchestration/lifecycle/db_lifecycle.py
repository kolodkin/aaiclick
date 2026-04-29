"""
aaiclick.orchestration.db_lifecycle — SQL models and helpers for the task lifecycle.

DBLifecycleOp / DBLifecycleMessage drive TaskLifecycleHandler's queue.
TablePinRef tracks downstream consumer pins. TableRegistry is the
ownership map every cleanup path joins on.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import ClassVar

from sqlalchemy import BigInteger, Column, DateTime, String, Text
from sqlmodel import Field, SQLModel


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


class TablePinRef(SQLModel, table=True):
    """One row per downstream consumer task that holds a pin on a table.

    Producer inserts one row per consumer; each consumer deletes its own
    row during deserialization. Table is droppable at job completion or
    via the orphan-scratch sweep once all pin_refs are gone.
    """

    __tablename__: ClassVar[str] = "table_pin_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    task_id: int = Field(sa_column=Column(BigInteger, primary_key=True))


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
    advisory_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, nullable=False, index=True),
    )
    schema_doc: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
    )
