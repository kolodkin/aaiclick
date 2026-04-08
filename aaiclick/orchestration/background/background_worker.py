"""Background worker for cleanup and job scheduling.

BackgroundWorker polls the database for:
1. Completed/failed jobs — deletes their job-scoped pin refs
2. Tables with no run refs — drops them in ClickHouse
3. Expired sample tables — drops _sample tables older than oplog TTL
4. Dead workers — marks their running tasks as FAILED
5. Scheduled jobs — creates Job runs when next_run_at is due

Completely independent of DataContext and OrchContext — has its own DB engine and CH client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import warnings
from datetime import datetime, timedelta

from croniter import croniter
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from aaiclick.backend import is_chdb, parse_ch_url
from aaiclick.oplog.cleanup import lineage_aware_drop
from aaiclick.snowflake_id import get_snowflake_id

from ..env import get_db_url
from .handler import BackgroundHandler, create_background_handler

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 10.0
DEFAULT_WORKER_TIMEOUT = 90.0


class BackgroundWorker:
    """Background worker for cleanup and job scheduling.

    Performs five operations on each poll:
    1. Job cleanup: deletes pin refs for completed/failed jobs
    2. Table cleanup: drops CH tables with no run refs
    3. Sample cleanup: drops expired _sample tables older than oplog TTL
    4. Dead worker detection: marks tasks from expired workers as FAILED
    5. Job scheduling: creates Job runs for registered jobs whose next_run_at is due

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
        self._handler: BackgroundHandler | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_db_url(), echo=False)
        self._handler = create_background_handler()

        if is_chdb():
            from aaiclick.data.data_context.chdb_client import create_chdb_client

            self._ch_client = create_chdb_client()
        else:
            from clickhouse_connect import get_async_client

            # clickhouse-connect >=0.15 FutureWarning about thread-pool async wrapper
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="The current async client", category=FutureWarning)
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
        await self._cleanup_completed_jobs()
        await self._cleanup_unreferenced_tables()
        await self._cleanup_expired_samples()
        await self._cleanup_dead_workers()
        await self._check_schedules()

    async def _cleanup_completed_jobs(self) -> None:
        """Delete pin refs for completed/failed/cancelled jobs.

        Removes table_pin_refs rows so pinned tables become eligible
        for cleanup by _cleanup_unreferenced_tables.
        """
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT DISTINCT job_id FROM table_pin_refs "
                    "WHERE job_id IN ("
                    "  SELECT id FROM jobs "
                    "  WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')"
                    ")"
                )
            )
            job_ids = [row[0] for row in result.fetchall()]

            if not job_ids:
                return

            await self._handler.clear_job_pins(session, job_ids)
            await session.commit()

    async def _cleanup_unreferenced_tables(self) -> None:
        """Drop CH tables with no run refs and no pin.

        A table is eligible for cleanup when it has no pin_refs AND no
        run_refs.  Pin refs are inserted by the producer and deleted by
        _cleanup_completed_jobs when the job finishes.
        """
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT DISTINCT tcr.table_name FROM table_context_refs tcr "
                    "WHERE tcr.table_name NOT LIKE 'p\\_%' "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM table_pin_refs tpr "
                    "  WHERE tpr.table_name = tcr.table_name"
                    ") "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM table_run_refs trr "
                    "  WHERE trr.table_name = tcr.table_name"
                    ")"
                )
            )
            rows = result.fetchall()

            for (table_name,) in rows:
                try:
                    await lineage_aware_drop(self._ch_client, table_name)
                except Exception:
                    logger.warning("Failed to drop CH table %s", table_name, exc_info=True)

                await session.execute(
                    text(
                        "DELETE FROM table_context_refs "
                        "WHERE table_name = :table_name"
                    ),
                    {"table_name": table_name},
                )

            await session.commit()

    async def _cleanup_expired_samples(self) -> None:
        """Drop sample tables older than the oplog TTL.

        Sample tables ({table}_sample) are created by lineage_aware_drop when
        ephemeral tables are cleaned up.  Once past the oplog TTL they no longer
        have matching operation_log rows, so keeping them wastes storage.
        """
        ttl_days = int(os.environ.get("AAICLICK_OPLOG_TTL_DAYS", "90"))
        try:
            result = await self._ch_client.query(
                "SELECT name FROM system.tables "
                "WHERE database = currentDatabase() "
                "AND name LIKE '%\\_sample' "
                "AND name NOT LIKE 'p\\_%' "
                f"AND metadata_modification_time < now() - INTERVAL {ttl_days} DAY"
            )
            for (table_name,) in result.result_rows:
                try:
                    await self._ch_client.command(
                        f"DROP TABLE IF EXISTS {table_name}"
                    )
                    logger.debug("Dropped expired sample table %s", table_name)
                except Exception:
                    logger.warning(
                        "Failed to drop sample table %s", table_name, exc_info=True
                    )
        except Exception:
            logger.debug("Failed to query expired sample tables", exc_info=True)

    async def _cleanup_dead_workers(self) -> None:
        """Detect dead workers, mark their running tasks as FAILED, and clean orphaned run refs."""
        cutoff = datetime.utcnow() - timedelta(seconds=self._worker_timeout)

        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT id FROM workers "
                    "WHERE status IN ('ACTIVE', 'STOPPING') "
                    "AND last_heartbeat < :cutoff"
                ),
                {"cutoff": cutoff},
            )
            dead_worker_ids = [row[0] for row in result.fetchall()]

            if not dead_worker_ids:
                return

            # Collect run_ids of tasks that will be marked as FAILED
            # (currently RUNNING/CLAIMED on dead workers) before marking.
            orphaned_run_ids = await self._handler.get_dead_worker_run_ids(
                session, dead_worker_ids,
            )

            now = datetime.utcnow()
            await self._handler.mark_dead_workers(session, dead_worker_ids, now)

            # Remove orphaned run refs from table_run_refs so affected
            # tables become eligible for cleanup.
            if orphaned_run_ids:
                await self._handler.clean_task_runs(
                    session, [str(rid) for rid in orphaned_run_ids],
                )

            await session.commit()

            for wid in dead_worker_ids:
                logger.warning("Worker %s marked as dead (heartbeat timeout)", wid)

    async def _check_schedules(self) -> None:
        """Create Job runs for registered jobs whose next_run_at is due.

        Uses optimistic locking on next_run_at to prevent duplicate runs
        when multiple background workers are active.

        Uses raw SQL instead of ORM because BackgroundWorker operates
        independently of OrchContext with its own DB engine.
        """
        now = datetime.utcnow()

        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT id, name, entrypoint, schedule, default_kwargs, next_run_at "
                    "FROM registered_jobs "
                    "WHERE enabled = true AND next_run_at <= :now"
                ),
                {"now": now},
            )
            due_jobs = result.fetchall()

            for row in due_jobs:
                reg_id, name, entrypoint, schedule, default_kwargs, old_next_run = row

                # Compute next fire time from cron
                new_next_run = croniter(schedule, now).get_next(datetime)

                # Optimistic lock: only update if next_run_at hasn't changed
                lock_result = await session.execute(
                    text(
                        "UPDATE registered_jobs "
                        "SET next_run_at = :new_next, updated_at = :now "
                        "WHERE id = :reg_id AND next_run_at = :old_next"
                    ),
                    {
                        "new_next": new_next_run,
                        "now": now,
                        "reg_id": reg_id,
                        "old_next": old_next_run,
                    },
                )

                if lock_result.rowcount == 0:
                    continue

                # Won the race — create Job + entry Task
                job_id = get_snowflake_id()
                task_id = get_snowflake_id()

                await session.execute(
                    text(
                        "INSERT INTO jobs (id, name, status, run_type, registered_job_id, created_at) "
                        "VALUES (:id, :name, 'PENDING', 'SCHEDULED', :reg_id, :now)"
                    ),
                    {"id": job_id, "name": name, "reg_id": reg_id, "now": now},
                )

                await session.execute(
                    text(
                        "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, status, created_at, max_retries, attempt) "
                        "VALUES (:id, :job_id, :entrypoint, :name, :kwargs, 'PENDING', :now, 0, 0)"
                    ),
                    {
                        "id": task_id,
                        "job_id": job_id,
                        "entrypoint": entrypoint,
                        "name": name,
                        "kwargs": default_kwargs if isinstance(default_kwargs, str) else (json.dumps(default_kwargs) if default_kwargs else "{}"),
                        "now": now,
                    },
                )

                logger.info("Scheduled job '%s' created (job_id=%s)", name, job_id)

            await session.commit()
