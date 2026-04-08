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
from sqlmodel import Field, SQLModel


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
    """Registry of tracked ClickHouse tables.

    Composite PK (table_name, context_id) allows multiple contexts to
    register the same table independently.
    """

    __tablename__ = "table_context_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    context_id: int = Field(sa_column=Column(BigInteger, primary_key=True))


class TablePinRef(SQLModel, table=True):
    """Junction table: which jobs hold a pin on which tables.

    pin inserts a row; the background worker deletes rows for
    completed/failed/cancelled jobs.  Tables with any pin_ref row
    are skipped during cleanup, even when no run_refs remain.  This
    bridges the gap between a producer task finishing and all consumer
    tasks starting (at which point their run_refs protect the table).
    """

    __tablename__ = "table_pin_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    job_id: int = Field(sa_column=Column(BigInteger, primary_key=True))


class TableRunRef(SQLModel, table=True):
    """Junction table: which run_ids hold a reference to which tables.

    incref inserts a row; decref deletes it.  clean_task_run deletes all
    rows for a given run_id (crash recovery).  When no rows remain for a
    table (and no pin in table_context_refs), the background worker drops
    the ClickHouse table.
    """

    __tablename__ = "table_run_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    run_id: str = Field(sa_column=Column(String, primary_key=True))
