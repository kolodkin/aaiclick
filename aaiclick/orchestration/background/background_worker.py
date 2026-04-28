"""Background worker for cleanup and job scheduling.

BackgroundWorker polls the database for:
1. Completed/failed jobs — deletes their job-scoped pin refs
2. Tables with no run refs — drops them in ClickHouse (samples registered with job)
3. Expired jobs — deletes all job data (CH tables, oplog, SQL metadata) + orphans
4. Dead workers — marks their running tasks as FAILED
5. Scheduled jobs — creates Job runs when next_run_at is due

All resource cleanup is job-driven: every CH table, sample, and oplog entry
traces to a job_id via table_registry (SQL). Resources without a job_id
(orphans) are cleaned up after the same TTL.

Completely independent of DataContext and OrchContext — has its own DB engine and CH client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import warnings
from datetime import datetime, timedelta
from typing import Any, cast

from croniter import croniter
from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from aaiclick.backend import is_chdb, parse_ch_url
from aaiclick.oplog.cleanup import TableOwner, lineage_aware_drop
from aaiclick.snowflake import get_snowflake_id

from ..env import get_db_url
from ..models import JobStatus
from .handler import BackgroundHandler, create_background_handler, in_clause, try_complete_job

# Base delay for retry backoff (seconds).  Actual delay = BASE * 2^attempt.
RETRY_BASE_DELAY = 1

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 10.0
DEFAULT_WORKER_TIMEOUT = 90.0


_REF_TABLES = ("table_context_refs", "table_pin_refs", "table_run_refs")


async def _delete_table_refs(session: AsyncSession, table_names: list[str]) -> None:
    """Delete all ref-table rows for the given table names."""
    if not table_names:
        return
    ph, params = in_clause(table_names, "tn")
    for ref_table in _REF_TABLES:
        await session.execute(
            text(f"DELETE FROM {ref_table} WHERE table_name IN ({ph})"),
            params,
        )


class BackgroundWorker:
    """Background worker for cleanup and job scheduling.

    Performs five operations on each poll:
    1. Pending cleanup: processes failed tasks (ref cleanup, retry/fail transition)
    2. Unreferenced tables: drops CH tables with no run/pin refs (samples tracked with job)
    3. Expired jobs: deletes all data for jobs past AAICLICK_JOB_TTL_DAYS + orphans
    4. Dead worker detection: marks tasks from expired workers as PENDING_CLEANUP
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
        self._engine: AsyncEngine = create_async_engine(get_db_url(), echo=False)
        self._ch_client: Any = None
        self._shutdown: asyncio.Event = asyncio.Event()
        self._handler: BackgroundHandler = create_background_handler()

    async def start(self) -> None:
        if is_chdb():
            from aaiclick.data.data_context.chdb_client import create_chdb_client

            self._ch_client = create_chdb_client()
        else:
            from clickhouse_connect import get_async_client

            # clickhouse-connect >=0.15 FutureWarning about thread-pool async wrapper
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="The current async client", category=FutureWarning)
                self._ch_client = await get_async_client(**parse_ch_url())
        self._task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task:
            await self._task
        await self._engine.dispose()
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
        await self._process_pending_cleanup()
        await self._cleanup_unreferenced_tables()
        await self._cleanup_expired_jobs()
        await self._cleanup_dead_workers()
        await self._check_schedules()

    async def _process_pending_cleanup(self) -> None:
        """Process tasks in PENDING_CLEANUP status.

        1. Batch-clean run_refs and pin_refs for all failed tasks
        2. Transition each to PENDING (retries remaining) or FAILED (exhausted)
        3. Check job completion for jobs with newly-FAILED tasks
        """
        async with AsyncSession(self._engine) as session:
            tasks = await self._handler.get_pending_cleanup_tasks(session)
            if not tasks:
                return

            # Clean run_refs (batched) and pin_refs (per-task) for all failed tasks
            run_ids_to_clean = [str(t.run_ids[-1]) for t in tasks if t.run_ids]
            if run_ids_to_clean:
                await self._handler.clean_task_runs(session, run_ids_to_clean)
            for t in tasks:
                await self._handler.clean_task_pins(session, t.task_id)

            # Transition each task and collect jobs that need completion check
            failed_job_ids: set[int] = set()
            for task in tasks:
                has_retries = task.attempt < task.max_retries
                if has_retries:
                    retry_after = datetime.utcnow() + timedelta(
                        seconds=RETRY_BASE_DELAY * (2**task.attempt),
                    )
                    await self._handler.transition_pending_cleanup(
                        session,
                        task.task_id,
                        has_retries=True,
                        attempt=task.attempt + 1,
                        retry_after=retry_after,
                    )
                    logger.info(
                        "Task %s cleaned and scheduled for retry (attempt %d/%d)",
                        task.task_id,
                        task.attempt + 1,
                        task.max_retries,
                    )
                else:
                    await self._handler.transition_pending_cleanup(
                        session,
                        task.task_id,
                        has_retries=False,
                        attempt=task.attempt + 1,
                        retry_after=datetime.utcnow(),
                    )
                    logger.info("Task %s cleaned and marked FAILED", task.task_id)
                    failed_job_ids.add(task.job_id)

            for job_id in failed_job_ids:
                await try_complete_job(session, job_id)

            await session.commit()

    async def _cleanup_unreferenced_tables(self) -> None:
        """No-op; scheduled for removal in Phase 6 of the lifecycle simplification.

        Phase 1 of the lifecycle simplification dropped table_run_refs and
        table_context_refs, which this method queried. The poll loop still
        invokes it; Phase 6 deletes both the call site and the method.
        """
        return

    async def _lookup_table_owners(self, table_names: list[str]) -> dict[str, TableOwner]:
        """Look up ownership metadata from table_registry for a list of table names."""
        if not table_names:
            return {}
        async with AsyncSession(self._engine) as session:
            ph, params = in_clause(table_names, "tn")
            result = await session.execute(
                text(f"SELECT table_name, job_id, task_id, run_id FROM table_registry WHERE table_name IN ({ph})"),
                params,
            )
            return {row[0]: TableOwner(job_id=row[1], task_id=row[2], run_id=row[3]) for row in result.fetchall()}

    async def _cleanup_expired_jobs(self) -> None:
        """Delete all data for expired jobs and orphaned resources.

        Job-driven cleanup: every resource traces to a job_id.
        1. Find completed/failed/cancelled jobs older than AAICLICK_JOB_TTL_DAYS
        2. For each: delete all CH tables, oplog, SQL metadata via _delete_job_data()
        3. Clean up orphaned resources (job_id IS NULL) older than the TTL
        """
        ttl_days = int(os.environ.get("AAICLICK_JOB_TTL_DAYS", "90"))
        cutoff = datetime.utcnow() - timedelta(days=ttl_days)

        # 1. Delete expired jobs
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text(
                    "SELECT id FROM jobs WHERE status IN (:completed, :failed, :cancelled) AND completed_at < :cutoff"
                ),
                {
                    "completed": JobStatus.COMPLETED.value,
                    "failed": JobStatus.FAILED.value,
                    "cancelled": JobStatus.CANCELLED.value,
                    "cutoff": cutoff,
                },
            )
            expired_job_ids = [row[0] for row in result.fetchall()]

        for job_id in expired_job_ids:
            try:
                await self._delete_job_data(job_id)
                logger.info("Deleted expired job %s and all associated data", job_id)
            except Exception:
                logger.warning("Failed to delete expired job %s", job_id, exc_info=True)

        # 2. Clean up orphaned resources (no job association)
        await self._cleanup_orphaned_resources(ttl_days)

    async def _delete_job_data(self, job_id: int) -> None:
        """Delete all CH and SQL data for a single job.

        User-managed ``p_*`` tables are exempt from the CH drop list: they
        outlive any single job and are only removed via
        ``delete_persistent_object()``. Job-scoped ``j_<id>_*`` tables and
        unnamed ``t_*`` tables belonging to this job are dropped normally.
        """
        async with AsyncSession(self._engine) as session:
            # 1. Find all CH tables belonging to this job from table_registry,
            #    excluding user-managed globals (``p_*``).
            result = await session.execute(
                text(
                    "SELECT DISTINCT table_name FROM table_registry "
                    "WHERE job_id = :job_id AND table_name NOT LIKE 'p\\_%' ESCAPE '\\'"
                ),
                {"job_id": job_id},
            )
            table_names = [row[0] for row in result.fetchall()]

            # 2. Drop all non-global CH tables (includes samples registered in table_registry)
            for table_name in table_names:
                try:
                    await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    logger.debug("Failed to drop table %s", table_name, exc_info=True)

            # operation_log still lives on CH, not SQL.
            try:
                await self._ch_client.command(f"ALTER TABLE operation_log DELETE WHERE job_id = {job_id}")
            except Exception:
                logger.debug("Failed to delete operation_log for job %s", job_id, exc_info=True)

            # 4. Delete SQL metadata
            await session.execute(
                text("DELETE FROM table_registry WHERE job_id = :job_id"),
                {"job_id": job_id},
            )
            # Get task IDs for this job
            result = await session.execute(
                text("SELECT id FROM tasks WHERE job_id = :job_id"),
                {"job_id": job_id},
            )
            task_ids = [row[0] for row in result.fetchall()]

            if task_ids:
                # Delete dependencies referencing these tasks
                ph, params = in_clause(task_ids, "tid")
                await session.execute(
                    text(
                        f"DELETE FROM dependencies "
                        f"WHERE (previous_id IN ({ph}) AND previous_type = 'task') "
                        f"OR (next_id IN ({ph}) AND next_type = 'task')"
                    ),
                    params,
                )

            # Delete ref tables for tables belonging to this job
            await _delete_table_refs(session, table_names)

            # Delete tasks
            await session.execute(
                text("DELETE FROM tasks WHERE job_id = :job_id"),
                {"job_id": job_id},
            )

            # Delete groups (and their dependencies)
            result = await session.execute(
                text("SELECT id FROM groups WHERE job_id = :job_id"),
                {"job_id": job_id},
            )
            group_ids = [row[0] for row in result.fetchall()]
            if group_ids:
                ph, params = in_clause(group_ids, "gid")
                await session.execute(
                    text(
                        f"DELETE FROM dependencies "
                        f"WHERE (previous_id IN ({ph}) AND previous_type = 'group') "
                        f"OR (next_id IN ({ph}) AND next_type = 'group')"
                    ),
                    params,
                )
                await session.execute(
                    text("DELETE FROM groups WHERE job_id = :job_id"),
                    {"job_id": job_id},
                )

            # Delete the job itself
            await session.execute(
                text("DELETE FROM jobs WHERE id = :job_id"),
                {"job_id": job_id},
            )

            await session.commit()

    async def _cleanup_orphaned_resources(self, ttl_days: int) -> None:
        """Clean up CH resources with no job association older than the TTL.

        Handles resources that were never associated with a job (local mode,
        pre-migration data, or failed registration). Drops CH tables named
        in orphaned ``table_registry`` rows, deletes those rows, and deletes
        matching orphaned ``operation_log`` entries.
        """
        cutoff = datetime.utcnow() - timedelta(days=ttl_days)

        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text("SELECT DISTINCT table_name FROM table_registry WHERE job_id IS NULL AND created_at < :cutoff"),
                {"cutoff": cutoff},
            )
            orphan_tables = [row[0] for row in result.fetchall()]

            for table_name in orphan_tables:
                try:
                    await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
                    logger.debug("Dropped orphaned table %s", table_name)
                except Exception:
                    logger.debug("Failed to drop orphaned table %s", table_name, exc_info=True)

            await session.execute(
                text("DELETE FROM table_registry WHERE job_id IS NULL AND created_at < :cutoff"),
                {"cutoff": cutoff},
            )
            await session.commit()

        # operation_log still lives on CH — prune orphaned rows there.
        try:
            await self._ch_client.command(
                "ALTER TABLE operation_log DELETE "
                "WHERE job_id IS NULL "
                f"AND created_at < now() - INTERVAL {ttl_days} DAY"
            )
        except Exception:
            logger.debug("Failed to delete orphaned operation_log entries", exc_info=True)

    async def _cleanup_dead_workers(self) -> None:
        """Detect dead workers, mark their running tasks as PENDING_CLEANUP.

        Ref cleanup is handled by _process_pending_cleanup on the next cycle.
        """
        cutoff = datetime.utcnow() - timedelta(seconds=self._worker_timeout)

        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text("SELECT id FROM workers WHERE status IN ('ACTIVE', 'STOPPING') AND last_heartbeat < :cutoff"),
                {"cutoff": cutoff},
            )
            dead_worker_ids = [row[0] for row in result.fetchall()]

            if not dead_worker_ids:
                return

            now = datetime.utcnow()
            await self._handler.mark_dead_workers(session, dead_worker_ids, now)
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
                    "WHERE enabled = :enabled AND next_run_at <= :now"
                ),
                {"enabled": True, "now": now},
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

                if cast(CursorResult, lock_result).rowcount == 0:
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
                        "kwargs": default_kwargs
                        if isinstance(default_kwargs, str)
                        else (json.dumps(default_kwargs) if default_kwargs else "{}"),
                        "now": now,
                    },
                )

                logger.info("Scheduled job '%s' created (job_id=%s)", name, job_id)

            await session.commit()
