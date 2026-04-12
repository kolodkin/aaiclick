"""Background worker for cleanup and job scheduling.

BackgroundWorker polls the database for:
1. Completed/failed jobs — deletes their job-scoped pin refs
2. Tables with no run refs — drops them in ClickHouse (samples registered with job)
3. Expired jobs — deletes all job data (CH tables, oplog, SQL metadata) + orphans
4. Dead workers — marks their running tasks as FAILED
5. Scheduled jobs — creates Job runs when next_run_at is due

All resource cleanup is job-driven: every CH table, sample, and oplog entry
traces to a job_id via table_registry. Resources without a job_id (orphans)
are cleaned up after the same TTL.

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
from sqlmodel import select

from aaiclick.backend import is_chdb, parse_ch_url
from aaiclick.oplog.cleanup import TableOwner, lineage_aware_drop
from aaiclick.snowflake_id import get_snowflake_id

from ..env import get_db_url
from ..models import Job, JobStatus, PreservationMode, TaskStatus
from .handler import BackgroundHandler, create_background_handler, in_clause

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
                        seconds=RETRY_BASE_DELAY * (2 ** task.attempt),
                    )
                    await self._handler.transition_pending_cleanup(
                        session, task.task_id,
                        has_retries=True,
                        attempt=task.attempt + 1,
                        retry_after=retry_after,
                    )
                    logger.info(
                        "Task %s cleaned and scheduled for retry "
                        "(attempt %d/%d)", task.task_id, task.attempt + 1, task.max_retries,
                    )
                else:
                    await self._handler.transition_pending_cleanup(
                        session, task.task_id,
                        has_retries=False,
                        attempt=task.attempt + 1,
                        retry_after=datetime.utcnow(),
                    )
                    logger.info("Task %s cleaned and marked FAILED", task.task_id)
                    failed_job_ids.add(task.job_id)

            for job_id in failed_job_ids:
                await self._try_complete_job(session, job_id)

            await session.commit()

    async def _try_complete_job(self, session: AsyncSession, job_id: int) -> None:
        """Check if all tasks for a job are in terminal state and update job status."""
        result = await session.execute(
            text("SELECT status FROM tasks WHERE job_id = :job_id"),
            {"job_id": job_id},
        )
        statuses = [row[0] for row in result.fetchall()]
        if not statuses:
            return

        non_terminal = {
            TaskStatus.PENDING, TaskStatus.CLAIMED,
            TaskStatus.RUNNING, TaskStatus.PENDING_CLEANUP,
        }
        if any(s in non_terminal for s in statuses):
            return

        now = datetime.utcnow()
        if any(s == TaskStatus.FAILED for s in statuses):
            await session.execute(
                text(
                    "UPDATE jobs SET status = :failed, completed_at = :now, "
                    "error = 'One or more tasks failed' "
                    "WHERE id = :job_id"
                ),
                {"job_id": job_id, "now": now, "failed": JobStatus.FAILED.value},
            )
        else:
            await session.execute(
                text(
                    "UPDATE jobs SET status = :completed, completed_at = :now "
                    "WHERE id = :job_id"
                ),
                {"job_id": job_id, "now": now, "completed": JobStatus.COMPLETED.value},
            )

    async def _cleanup_unreferenced_tables(self) -> None:
        """Drop CH tables with no pin refs and no run refs.

        Each consumer task has its own pin_ref row (created by producer
        fan-out, removed by consumer's unpin during deserialization).
        A table is eligible when all consumers have unpinned AND no
        run_refs remain.

        Looks up the owning job_id from table_registry so the sample
        table created by lineage_aware_drop is registered with the job
        and cleaned up when the job expires.

        Preservation mode gates the actual drop:

        - ``NONE``: drop via ``lineage_aware_drop`` (no lineage refs → hard drop).
        - ``STRATEGY``: drop via ``lineage_aware_drop`` — strategy-matched
          rows survive as a ``_sample`` table registered with the job.
        - ``FULL``: skip the drop entirely. The table lives until the job
          TTL expires and ``_cleanup_expired_jobs`` collects it.
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
            if not rows:
                return

            table_names = [r[0] for r in rows]

            # Batch-lookup ownership from table_registry in ClickHouse
            owner_map = await self._lookup_table_owners(table_names)

            # Resolve preservation mode per owning job so FULL jobs skip the drop.
            owning_job_ids = {o.job_id for o in owner_map.values() if o.job_id is not None}
            mode_map = await self._lookup_job_preservation_modes(session, owning_job_ids)

            dropped_tables: list[str] = []
            for table_name in table_names:
                owner = owner_map.get(table_name)
                mode = (
                    mode_map.get(owner.job_id, PreservationMode.NONE)
                    if owner is not None and owner.job_id is not None
                    else PreservationMode.NONE
                )
                if mode is PreservationMode.FULL:
                    # Keep the table alive until the job expires.
                    continue
                try:
                    await lineage_aware_drop(self._ch_client, table_name, owner=owner)
                except Exception:
                    logger.warning("Failed to drop CH table %s", table_name, exc_info=True)
                dropped_tables.append(table_name)

            if dropped_tables:
                await _delete_table_refs(session, dropped_tables)

            await session.commit()

    async def _lookup_job_preservation_modes(
        self,
        session: AsyncSession,
        job_ids: set[int],
    ) -> dict[int, PreservationMode]:
        """Return ``{job_id: PreservationMode}`` for the given jobs."""
        if not job_ids:
            return {}
        result = await session.execute(
            select(Job.id, Job.preservation_mode).where(Job.id.in_(job_ids)),
        )
        return {row[0]: row[1] for row in result.all()}

    async def _lookup_table_owners(self, table_names: list[str]) -> dict[str, TableOwner]:
        """Look up ownership metadata from table_registry for a list of table names."""
        if not table_names:
            return {}
        escaped = ", ".join("'" + t.replace("'", "\\'") + "'" for t in table_names)
        try:
            result = await self._ch_client.query(
                f"SELECT table_name, job_id, task_id, run_id FROM table_registry "
                f"WHERE table_name IN ({escaped})"
            )
            return {
                row[0]: TableOwner(job_id=row[1], task_id=row[2], run_id=row[3])
                for row in result.result_rows
            }
        except Exception:
            logger.debug("Failed to lookup owners from table_registry", exc_info=True)
            return {}

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
                    "SELECT id FROM jobs "
                    "WHERE status IN (:completed, :failed, :cancelled) "
                    "AND completed_at < :cutoff"
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
        """Delete all CH and SQL data for a single job."""
        # 1. Find all CH tables belonging to this job from table_registry
        try:
            result = await self._ch_client.query(
                "SELECT DISTINCT table_name FROM table_registry "
                f"WHERE job_id = {job_id}"
            )
            table_names = [row[0] for row in result.result_rows]
        except Exception:
            logger.debug("Failed to query table_registry for job %s", job_id, exc_info=True)
            table_names = []

        # 2. Drop all CH tables (includes samples registered in table_registry)
        for table_name in table_names:
            try:
                await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                logger.debug("Failed to drop table %s", table_name, exc_info=True)

        # 3. Delete operation_log and table_registry entries for this job
        try:
            await self._ch_client.command(
                f"ALTER TABLE operation_log DELETE WHERE job_id = {job_id}"
            )
        except Exception:
            logger.debug("Failed to delete operation_log for job %s", job_id, exc_info=True)
        try:
            await self._ch_client.command(
                f"ALTER TABLE table_registry DELETE WHERE job_id = {job_id}"
            )
        except Exception:
            logger.debug("Failed to delete table_registry for job %s", job_id, exc_info=True)

        # 4. Delete SQL metadata
        async with AsyncSession(self._engine) as session:
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
        pre-migration data, or failed registration).

        1. Drop CH tables in table_registry with job_id IS NULL older than TTL
        2. Delete orphaned operation_log entries (job_id IS NULL) older than TTL
        3. Delete orphaned table_registry entries (job_id IS NULL) older than TTL
        """
        # 1. Drop orphaned CH tables from table_registry
        try:
            result = await self._ch_client.query(
                "SELECT DISTINCT table_name FROM table_registry "
                "WHERE job_id IS NULL "
                f"AND created_at < now() - INTERVAL {ttl_days} DAY"
            )
            for (table_name,) in result.result_rows:
                try:
                    await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
                    logger.debug("Dropped orphaned table %s", table_name)
                except Exception:
                    logger.debug("Failed to drop orphaned table %s", table_name, exc_info=True)
        except Exception:
            logger.debug("Failed to query orphaned tables", exc_info=True)

        # 2. Delete orphaned operation_log entries
        try:
            await self._ch_client.command(
                "ALTER TABLE operation_log DELETE "
                "WHERE job_id IS NULL "
                f"AND created_at < now() - INTERVAL {ttl_days} DAY"
            )
        except Exception:
            logger.debug("Failed to delete orphaned operation_log entries", exc_info=True)

        # 3. Delete orphaned table_registry entries
        try:
            await self._ch_client.command(
                "ALTER TABLE table_registry DELETE "
                "WHERE job_id IS NULL "
                f"AND created_at < now() - INTERVAL {ttl_days} DAY"
            )
        except Exception:
            logger.debug("Failed to delete orphaned table_registry entries", exc_info=True)

    async def _cleanup_dead_workers(self) -> None:
        """Detect dead workers, mark their running tasks as PENDING_CLEANUP.

        Ref cleanup is handled by _process_pending_cleanup on the next cycle.
        """
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
