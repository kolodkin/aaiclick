"""
aaiclick.orchestration.db_lifecycle - Database models for distributed lifecycle tracking.

DBLifecycleOp, DBLifecycleMessage, and TableContextRef define the data structures
used by OrchLifecycleHandler (in context.py) for distributed table reference counting
and oplog recording.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from sqlalchemy import BigInteger, Column, String
from sqlmodel import JSON, Column as SmColumn, Field, SQLModel


class DBLifecycleOp(Enum):
    """Operations for the distributed lifecycle handler."""

    INCREF = auto()
    DECREF = auto()
    PIN = auto()
    OPLOG_RECORD = auto()
    OPLOG_SAMPLE = auto()
    OPLOG_TABLE = auto()
    SHUTDOWN = auto()


@dataclass
class OplogPayload:
    """Payload for OPLOG_RECORD and OPLOG_SAMPLE messages."""

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


@dataclass
class DBLifecycleMessage:
    """Message passed to handler via queue."""

    op: DBLifecycleOp
    table_name: str = ""
    oplog: OplogPayload | None = None
    oplog_table: OplogTablePayload | None = None


class TableContextRef(SQLModel, table=True):
    """Tracks ClickHouse table references in PostgreSQL via run_id arrays.

    Composite PK (table_name, context_id) allows multiple contexts to hold
    independent refs to the same table.

    run_ids is a JSON array of run-ID strings.  incref appends the current
    run_id; decref removes it.  When the array is empty (and no pin), the
    background worker drops the ClickHouse table.

    - Task refs: context_id = task_id, job_id = NULL
    - Pin refs: context_id = task_id, job_id = job_id (non-NULL marks a pin)
    """

    __tablename__ = "table_context_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    context_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    run_ids: list[str] = Field(default_factory=list, sa_column=SmColumn(JSON, nullable=False, server_default="[]"))
    job_id: int | None = Field(default=None, sa_column=Column(BigInteger, nullable=True))
