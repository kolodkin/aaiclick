"""
aaiclick.orchestration.pg_lifecycle - Distributed lifecycle handler via PostgreSQL.

PgLifecycleHandler tracks Object table reference counts in PostgreSQL using
context-scoped refs. Each handler instance gets a unique context_id (snowflake)
for grouping its refs. Pin operations use the job_id as context_id, so pinned
results survive stop() which only deletes refs for the handler's own context_id.

Owns its own PG engine, fully independent of OrchContext.
"""

from __future__ import annotations

import asyncio
import queue
from dataclasses import dataclass
from enum import Enum, auto

from sqlalchemy import BigInteger, Column, String, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlmodel import Field, SQLModel

from aaiclick.data.lifecycle import LifecycleHandler
from aaiclick.snowflake_id import get_snowflake_id

from .env import get_pg_url


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


# Module-level shared PG engine for claim_table() standalone function.
_pg_engine: AsyncEngine | None = None


def _get_pg_engine() -> AsyncEngine:
    """Get or create the shared PG engine for standalone operations."""
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_async_engine(get_pg_url(), echo=False)
    return _pg_engine


async def claim_table(table_name: str, job_id: int) -> None:
    """Release a job-scoped pinned ref (ownership transfer to consumer).

    Called during deserialization when a consuming task takes ownership
    of a result table. The consumer already has its own context-scoped
    ref from incref, so the job-scoped pin ref can be safely removed.

    Args:
        table_name: ClickHouse table name to release
        job_id: Job ID that was used as context_id for the pin
    """
    engine = _get_pg_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "DELETE FROM table_context_refs "
                "WHERE table_name = :table AND context_id = :ctx"
            ),
            {"table": table_name, "ctx": job_id},
        )
        await session.commit()


class PgLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle via PostgreSQL reference tracking.

    Each handler instance gets a unique context_id (snowflake ID) for grouping
    its execution refs. Pin operations use job_id as context_id, so pinned
    result tables survive stop() which only deletes the handler's own refs.

    Uses thread-safe queue.Queue for sync incref/decref calls from Object.__del__.
    Asyncio task drains queue and writes to PG.

    Args:
        job_id: Job ID used as context_id for pin operations.
    """

    def __init__(self, job_id: int):
        self._context_id = get_snowflake_id()
        self._job_id = job_id
        self._queue: queue.Queue[PgLifecycleMessage] = queue.Queue()
        self._task: asyncio.Task | None = None
        self._engine: AsyncEngine | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_pg_url(), echo=False)
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        """Drain queue then unconditionally delete all refs for this context_id.

        Pin refs use job_id as context_id, so they survive this cleanup.
        Only execution-time refs (incref/decref) are removed.
        """
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.SHUTDOWN, ""))
        if self._task:
            await self._task
        if self._engine:
            async with AsyncSession(self._engine) as session:
                await session.execute(
                    text("DELETE FROM table_context_refs WHERE context_id = :ctx"),
                    {"ctx": self._context_id},
                )
                await session.commit()
            await self._engine.dispose()
            self._engine = None

    def incref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.DECREF, table_name))

    def pin(self, table_name: str) -> None:
        """Mark table as result — inserts a job-scoped ref that survives stop()."""
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.PIN, table_name))

    async def _process_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self._queue.get)
            if msg.op == PgLifecycleOp.SHUTDOWN:
                break
            async with AsyncSession(self._engine) as session:
                if msg.op == PgLifecycleOp.INCREF:
                    await session.execute(
                        text(
                            "INSERT INTO table_context_refs (table_name, context_id, refcount) "
                            "VALUES (:table_name, :context_id, 1) "
                            "ON CONFLICT (table_name, context_id) "
                            "DO UPDATE SET refcount = table_context_refs.refcount + 1"
                        ),
                        {"table_name": msg.table_name, "context_id": self._context_id},
                    )
                elif msg.op == PgLifecycleOp.DECREF:
                    await session.execute(
                        text(
                            "UPDATE table_context_refs "
                            "SET refcount = refcount - 1 "
                            "WHERE table_name = :table_name AND context_id = :context_id"
                        ),
                        {"table_name": msg.table_name, "context_id": self._context_id},
                    )
                elif msg.op == PgLifecycleOp.PIN:
                    await session.execute(
                        text(
                            "INSERT INTO table_context_refs (table_name, context_id, refcount) "
                            "VALUES (:table_name, :context_id, 1) "
                            "ON CONFLICT (table_name, context_id) "
                            "DO UPDATE SET refcount = table_context_refs.refcount + 1"
                        ),
                        {"table_name": msg.table_name, "context_id": self._job_id},
                    )
                await session.commit()
