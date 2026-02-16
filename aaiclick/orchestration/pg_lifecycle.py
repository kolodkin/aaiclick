"""
aaiclick.orchestration.pg_lifecycle - Distributed lifecycle handler via PostgreSQL.

PgLifecycleHandler tracks Object table reference counts in PostgreSQL.
It only handles incref/decref — table cleanup is a separate background worker.
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

from .env import get_pg_url


class PgLifecycleOp(Enum):
    """Operations for the PG lifecycle handler."""

    INCREF = auto()
    DECREF = auto()
    SHUTDOWN = auto()


@dataclass
class PgLifecycleMessage:
    """Message passed to handler via queue."""

    op: PgLifecycleOp
    table_name: str


class TableRefcount(SQLModel, table=True):
    """Tracks ClickHouse table reference counts in PostgreSQL."""

    __tablename__ = "table_refcounts"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    refcount: int = Field(default=0, sa_column=Column(BigInteger, nullable=False))


class PgLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle via PostgreSQL reference tracking.

    Only handles incref/decref. Table cleanup is a separate background worker.
    Owns its own PG engine — independent of OrchContext.

    Uses thread-safe queue.Queue for sync incref/decref calls from Object.__del__.
    Asyncio task drains queue and writes to PG.
    """

    def __init__(self):
        self._queue: queue.Queue[PgLifecycleMessage] = queue.Queue()
        self._task: asyncio.Task | None = None
        self._engine: AsyncEngine | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_pg_url(), echo=False)
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.SHUTDOWN, ""))
        if self._task:
            await self._task
        if self._engine:
            await self._engine.dispose()
            self._engine = None

    def incref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.DECREF, table_name))

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
                            "INSERT INTO table_refcounts (table_name, refcount) "
                            "VALUES (:table_name, 1) "
                            "ON CONFLICT (table_name) "
                            "DO UPDATE SET refcount = table_refcounts.refcount + 1"
                        ),
                        {"table_name": msg.table_name},
                    )
                elif msg.op == PgLifecycleOp.DECREF:
                    await session.execute(
                        text(
                            "UPDATE table_refcounts "
                            "SET refcount = refcount - 1 "
                            "WHERE table_name = :table_name"
                        ),
                        {"table_name": msg.table_name},
                    )
                await session.commit()
