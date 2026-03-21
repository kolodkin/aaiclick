"""
aaiclick.orchestration.pg_cleanup - Background worker for dropping unreferenced CH tables.

PgCleanupWorker polls the database for:
1. Completed/failed jobs — deletes their job-scoped pin refs
2. Tables with total refcount <= 0 — drops them in ClickHouse
3. Dead workers — marks their running tasks as FAILED

Completely independent of DataContext and OrchContext — has its own DB engine and CH client.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from aaiclick.backend import is_chdb, is_sqlite, parse_ch_url

from .env import get_db_url

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 10.0
DEFAULT_WORKER_TIMEOUT = 90.0


class PgCleanupWorker:
    """Background worker that cleans up unreferenced CH tables.

    Performs three cleanup operations on each poll:
    1. Job cleanup: deletes pin refs for completed/failed jobs
    2. Table cleanup: drops CH tables with total refcount <= 0
    3. Dead worker detection: marks tasks from expired workers as FAILED

    Completely independent of DataContext and OrchContext.
    Has own DB engine and CH client.
    """

    def __init__(
        self,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        worker_timeout: float = DEFAULT_WORKER_TIMEOUT,
    ):
        self._poll_interval = poll_interval
        self._worker_timeout = worker_timeout
        self._task: asyncio.Task | None = None
        self._engine: AsyncEngine | None = None
        self._ch_client: object | None = None
        self._shutdown: asyncio.Event | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_db_url(), echo=False)

        if is_chdb():
            from aaiclick.data.chdb_client import create_chdb_client

            self._ch_client = create_chdb_client()
        else:
            from clickhouse_connect import get_async_client

            self._ch_client = await get_async_client(**parse_ch_url())
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
        await self._sample_completed_job_tables()
        await self._cleanup_completed_jobs()
        await self._cleanup_unreferenced_tables()
        await self._cleanup_dead_workers()

    async def _sample_completed_job_tables(self) -> None:
        """Replace ephemeral tables from completed jobs with 10-row samples.

        Preserves operation_log references by keeping tables under the same
        name but truncated to 10 rows. Removes sampled tables from
        table_context_refs so the lifecycle cleanup won't drop them.
        """
        # Check if table_registry exists (oplog may not have been enabled yet)
        try:
            exists = await self._ch_client.command("EXISTS TABLE table_registry")
            if not exists:
                return
        except Exception:
            return

        # Find completed/failed/cancelled jobs
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT id FROM jobs "
                    "WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')"
                )
            )
            job_ids = [row[0] for row in result.fetchall()]

        if not job_ids:
            return

        # Find tables for those jobs in CH table_registry
        job_ids_str = ", ".join(str(jid) for jid in job_ids)
        try:
            result = await self._ch_client.query(
                f"SELECT DISTINCT table_name FROM table_registry "
                f"WHERE job_id IN ({job_ids_str})"
            )
        except Exception:
            logger.warning("Failed to query table_registry", exc_info=True)
            return

        table_names = [row[0] for row in result.result_rows]
        if not table_names:
            return

        sampled_tables = []
        for table_name in table_names:
            try:
                exists = await self._ch_client.command(f"EXISTS TABLE {table_name}")
                if not exists:
                    sampled_tables.append(table_name)
                    continue

                sample_table = f"{table_name}_sample"
                await self._ch_client.command(f"CREATE TABLE {sample_table} AS {table_name}")
                await self._ch_client.command(
                    f"INSERT INTO {sample_table} SELECT * FROM {table_name} LIMIT 10"
                )
                await self._ch_client.command(f"DROP TABLE {table_name}")
                await self._ch_client.command(f"RENAME TABLE {sample_table} TO {table_name}")
                sampled_tables.append(table_name)
            except Exception:
                logger.warning("Failed to sample table %s", table_name, exc_info=True)

        if not sampled_tables:
            return

        # Delete from CH table_registry for all processed jobs
        try:
            await self._ch_client.command(
                f"ALTER TABLE table_registry DELETE WHERE job_id IN ({job_ids_str})"
            )
        except Exception:
            logger.warning("Failed to delete from table_registry", exc_info=True)

        # Remove sampled tables from SQL table_context_refs so lifecycle cleanup
        # won't drop them — sampled tables persist indefinitely for historical queries
        async with AsyncSession(self._engine) as session:
            if is_sqlite():
                for table_name in sampled_tables:
                    await session.execute(
                        text(
                            "DELETE FROM table_context_refs WHERE table_name = :table_name"
                        ),
                        {"table_name": table_name},
                    )
            else:
                await session.execute(
                    text(
                        "DELETE FROM table_context_refs WHERE table_name = ANY(:table_names)"
                    ),
                    {"table_names": sampled_tables},
                )
            await session.commit()

    async def _cleanup_completed_jobs(self) -> None:
        """Delete job-scoped pin refs for completed/failed jobs."""
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT id FROM jobs "
                    "WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED') "
                    "AND id IN (SELECT DISTINCT context_id FROM table_context_refs)"
                )
            )
            job_ids = [row[0] for row in result.fetchall()]

            if not job_ids:
                return

            if is_sqlite():
                for jid in job_ids:
                    await session.execute(
                        text("DELETE FROM table_context_refs WHERE context_id = :jid"),
                        {"jid": jid},
                    )
            else:
                await session.execute(
                    text("DELETE FROM table_context_refs WHERE context_id = ANY(:job_ids)"),
                    {"job_ids": job_ids},
                )
            await session.commit()

    async def _cleanup_unreferenced_tables(self) -> None:
        """Drop CH tables with total refcount <= 0 across all contexts."""
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT table_name FROM table_context_refs "
                    "WHERE table_name NOT LIKE 'p\\_%' "
                    "GROUP BY table_name "
                    "HAVING SUM(refcount) <= 0"
                )
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
                    text(
                        "DELETE FROM table_context_refs "
                        "WHERE table_name = :table_name"
                    ),
                    {"table_name": table_name},
                )

            await session.commit()

    async def _cleanup_dead_workers(self) -> None:
        """Detect dead workers and mark their running tasks as FAILED."""
        cutoff = datetime.utcnow() - timedelta(seconds=self._worker_timeout)

        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT id FROM workers "
                    "WHERE status = 'ACTIVE' "
                    "AND last_heartbeat < :cutoff"
                ),
                {"cutoff": cutoff},
            )
            dead_worker_ids = [row[0] for row in result.fetchall()]

            if not dead_worker_ids:
                return

            now = datetime.utcnow()

            if is_sqlite():
                for wid in dead_worker_ids:
                    await session.execute(
                        text("UPDATE workers SET status = 'STOPPED' WHERE id = :wid"),
                        {"wid": wid},
                    )
                    await session.execute(
                        text(
                            "UPDATE tasks SET status = 'FAILED', "
                            "completed_at = :now, "
                            "error = 'Worker died (heartbeat timeout)' "
                            "WHERE worker_id = :wid "
                            "AND status IN ('RUNNING', 'CLAIMED')"
                        ),
                        {"wid": wid, "now": now},
                    )
            else:
                await session.execute(
                    text(
                        "UPDATE workers SET status = 'STOPPED' "
                        "WHERE id = ANY(:worker_ids)"
                    ),
                    {"worker_ids": dead_worker_ids},
                )
                await session.execute(
                    text(
                        "UPDATE tasks SET status = 'FAILED', "
                        "completed_at = :now, "
                        "error = 'Worker died (heartbeat timeout)' "
                        "WHERE worker_id = ANY(:worker_ids) "
                        "AND status IN ('RUNNING', 'CLAIMED')"
                    ),
                    {"worker_ids": dead_worker_ids, "now": now},
                )

            await session.commit()

            for wid in dead_worker_ids:
                logger.warning("Worker %s marked as dead (heartbeat timeout)", wid)
