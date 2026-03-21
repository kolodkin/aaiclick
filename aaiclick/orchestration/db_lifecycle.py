"""
aaiclick.orchestration.db_lifecycle - Database models for distributed lifecycle tracking.

PgLifecycleOp, PgLifecycleMessage, and TableContextRef define the data structures
used by _OrchLifecycleView (in context.py) for distributed table reference counting.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from sqlalchemy import BigInteger, Column, String
from sqlmodel import Field, SQLModel


class PgLifecycleOp(Enum):
    """Operations for the PG lifecycle handler."""

    INCREF = auto()
    DECREF = auto()
    PIN = auto()
    SHUTDOWN = auto()


@dataclass
class PgLifecycleMessage:
    """Message passed to handler via queue."""

    op: PgLifecycleOp
    table_name: str


class TableContextRef(SQLModel, table=True):
    """Tracks ClickHouse table reference counts in PostgreSQL.

    Composite PK (table_name, context_id) allows multiple contexts to hold
    independent refs to the same table. context_id is either a handler's
    auto-generated snowflake ID (for execution refs) or a job_id (for pinned
    result refs).
    """

    __tablename__ = "table_context_refs"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    context_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    refcount: int = Field(default=0, sa_column=Column(BigInteger, nullable=False))
