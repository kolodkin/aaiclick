"""
aaiclick.orchestration.pg_cleanup - Background worker for dropping unreferenced CH tables.

PgCleanupWorker polls PostgreSQL every N seconds for tables with refcount <= 0,
drops them in ClickHouse, and removes the refcount rows. Completely independent
of DataContext and OrchContext — has its own PG engine and CH client.
"""

from __future__ import annotations

import asyncio
import logging

from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from aaiclick.data.env import get_ch_creds

from .env import get_pg_url

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 10.0


class PgCleanupWorker:
    """Background worker that drops CH tables with refcount <= 0.

    Polls PostgreSQL every N seconds. Completely independent of
    DataContext and OrchContext. Has own PG engine and CH client.
    """

    def __init__(self, poll_interval: float = DEFAULT_POLL_INTERVAL):
        self._poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._engine: AsyncEngine | None = None
        self._ch_client: AsyncClient | None = None
        self._shutdown: asyncio.Event | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_pg_url(), echo=False)
        creds = get_ch_creds()
        self._ch_client = await get_async_client(
            host=creds.host,
            port=creds.port,
            username=creds.user,
            password=creds.password,
            database=creds.database,
        )
        self._shutdown = asyncio.Event()
        self._task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        if self._shutdown:
            self._shutdown.set()
        if self._task:
            await self._task
        if self._engine:
            await self._engine.dispose()
            self._engine = None
        self._ch_client = None

    async def _cleanup_loop(self) -> None:
        while not self._shutdown.is_set():
            await self._do_cleanup()
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self._poll_interval,
                )
            except asyncio.TimeoutError:
                pass

    async def _do_cleanup(self) -> None:
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text("SELECT table_name FROM table_refcounts WHERE refcount <= 0")
            )
            rows = result.fetchall()

            for (table_name,) in rows:
                try:
                    await self._ch_client.command(
                        f"DROP TABLE IF EXISTS {table_name}"
                    )
                except Exception:
                    logger.warning("Failed to drop CH table %s", table_name, exc_info=True)
                    continue

                await session.execute(
                    text("DELETE FROM table_refcounts WHERE table_name = :table_name"),
                    {"table_name": table_name},
                )

            await session.commit()
