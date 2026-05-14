"""Background handler protocol and factory dispatch.

Concrete implementations live in sqlite_handler.py and pg_handler.py.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import NamedTuple, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.backend import is_sqlite

from ...datetime_utils import utc_now
from ..models import (
    JOB_COMPLETED,
    JOB_FAILED,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_PENDING_CLEANUP,
    TASK_RUNNING,
    TASK_UPSTREAM_FAILED,
)

JOB_FAILED_ERROR = "One or more tasks failed"
UPSTREAM_FAILED_ERROR = "Upstream task failed"

_CASCADE_UPSTREAM_FAILED_SQL = """
    UPDATE tasks SET status = :upstream_failed, completed_at = :now, error = :error_msg
    WHERE job_id = :job_id
      AND status = :pending
      AND (
        EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = tasks.id
              AND d.next_type = 'task'
              AND d.previous_type = 'task'
              AND prev.status IN (:failed, :cancelled, :upstream_failed)
        )
        OR EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = tasks.id
              AND d.next_type = 'task'
              AND d.previous_type = 'group'
              AND prev.status IN (:failed, :cancelled, :upstream_failed)
        )
        OR (tasks.group_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = tasks.group_id
              AND d.next_type = 'group'
              AND d.previous_type = 'task'
              AND prev.status IN (:failed, :cancelled, :upstream_failed)
        ))
        OR (tasks.group_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = tasks.group_id
              AND d.next_type = 'group'
              AND d.previous_type = 'group'
              AND prev.status IN (:failed, :cancelled, :upstream_failed)
        ))
      )
"""


async def cascade_upstream_failed(session: AsyncSession, job_id: int) -> int:
    """Mark transitively-downstream PENDING tasks as UPSTREAM_FAILED.

    Loops the cascade UPDATE until it converges (no rows changed) so a chain
    A→B→C→D collapses in one call. Returns the total number of tasks marked.
    Caller is responsible for committing.
    """
    now = utc_now()
    params = {
        "job_id": job_id,
        "now": now,
        "error_msg": UPSTREAM_FAILED_ERROR,
        "pending": TASK_PENDING,
        "failed": TASK_FAILED,
        "cancelled": TASK_CANCELLED,
        "upstream_failed": TASK_UPSTREAM_FAILED,
    }
    total = 0
    while True:
        result = await session.execute(text(_CASCADE_UPSTREAM_FAILED_SQL), params)
        changed = cast(CursorResult, result).rowcount or 0
        if changed <= 0:
            break
        total += changed
    return total


def in_clause(ids: list, prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause compatible with both SQLite and PostgreSQL.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params


async def try_complete_job(session: AsyncSession, job_id: int) -> None:
    """Mark a job COMPLETED or FAILED if all its tasks are in terminal states.

    No-op while any task is still PENDING, CLAIMED, RUNNING, or PENDING_CLEANUP.
    The terminal check is aggregated inside SQL (one row returned regardless
    of task count) so this stays O(1) on the worker hot path even for large
    jobs. Uses raw SQL on the passed session so it works both inside and
    outside an active ``orch_context``. The caller is responsible for committing.

    When any task is in a non-success terminal state, sweeps PENDING tasks
    whose transitive upstream failed and marks them UPSTREAM_FAILED — otherwise
    they would block job completion forever. The sweep is gated on the rollup
    aggregate so the happy path stays a single SELECT.
    """
    result = await session.execute(
        text(
            "SELECT "
            "  COUNT(*) AS total, "
            "  SUM(CASE WHEN status IN "
            "    (:pending, :claimed, :running, :pending_cleanup) "
            "    THEN 1 ELSE 0 END) AS non_terminal, "
            "  SUM(CASE WHEN status IN (:failed, :upstream_failed) THEN 1 ELSE 0 END) AS failed, "
            "  SUM(CASE WHEN status IN (:failed, :cancelled, :upstream_failed) "
            "    THEN 1 ELSE 0 END) AS cascade_trigger "
            "FROM tasks WHERE job_id = :job_id"
        ),
        {
            "job_id": job_id,
            "pending": TASK_PENDING,
            "claimed": TASK_CLAIMED,
            "running": TASK_RUNNING,
            "pending_cleanup": TASK_PENDING_CLEANUP,
            "failed": TASK_FAILED,
            "cancelled": TASK_CANCELLED,
            "upstream_failed": TASK_UPSTREAM_FAILED,
        },
    )
    total, non_terminal, failed, cascade_trigger = result.one()
    if cascade_trigger and non_terminal:
        marked = await cascade_upstream_failed(session, job_id)
        non_terminal -= marked
        failed += marked
    if not total or non_terminal:
        return

    now = utc_now()
    if failed:
        await session.execute(
            text("UPDATE jobs SET status = :status, completed_at = :now, error = :error WHERE id = :job_id"),
            {
                "job_id": job_id,
                "now": now,
                "status": JOB_FAILED,
                "error": JOB_FAILED_ERROR,
            },
        )
    else:
        await session.execute(
            text("UPDATE jobs SET status = :status, completed_at = :now WHERE id = :job_id"),
            {"job_id": job_id, "now": now, "status": JOB_COMPLETED},
        )


class PendingCleanupTask(NamedTuple):
    """Row returned by get_pending_cleanup_tasks."""

    task_id: int
    job_id: int
    worker_id: int
    error: str
    run_ids: list
    attempt: int
    max_retries: int


class BackgroundHandler(ABC):
    """Abstract base for backend-specific background cleanup SQL."""

    @staticmethod
    @abstractmethod
    async def mark_dead_workers(
        session: AsyncSession,
        dead_worker_ids: list[int],
        now: datetime,
    ) -> None:
        """Mark dead workers as STOPPED and their tasks as PENDING_CLEANUP."""
        ...

    @staticmethod
    async def clean_task_run(session: AsyncSession, run_id: str) -> None:
        """Delete all table_run_refs rows for a given run_id (crash recovery)."""
        await session.execute(
            text("DELETE FROM table_run_refs WHERE run_id = :run_id"),
            {"run_id": run_id},
        )

    @staticmethod
    async def clean_task_pins(session: AsyncSession, task_id: int) -> None:
        """Delete all table_pin_refs rows for a given task_id.

        Cleans pin refs that upstream producers created for this task as
        a downstream consumer.  Called during PENDING_CLEANUP processing
        so stale pins don't block table cleanup.
        """
        await session.execute(
            text("DELETE FROM table_pin_refs WHERE task_id = :task_id"),
            {"task_id": task_id},
        )

    @staticmethod
    @abstractmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        """Batch-delete table_run_refs rows for multiple run_ids."""
        ...

    @staticmethod
    @abstractmethod
    async def get_pending_cleanup_tasks(
        session: AsyncSession,
    ) -> list[PendingCleanupTask]:
        """Return tasks in PENDING_CLEANUP status."""
        ...

    @staticmethod
    async def transition_pending_cleanup(
        session: AsyncSession,
        task_id: int,
        *,
        has_retries: bool,
        attempt: int,
        retry_after: datetime,
    ) -> None:
        """Transition a PENDING_CLEANUP task to PENDING or FAILED."""
        if has_retries:
            await session.execute(
                text(
                    "UPDATE tasks SET status = :status, "
                    "attempt = :attempt, retry_after = :retry_after, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "started_at = NULL, completed_at = NULL "
                    "WHERE id = :task_id"
                ),
                {
                    "task_id": task_id,
                    "attempt": attempt,
                    "retry_after": retry_after,
                    "status": TASK_PENDING,
                },
            )
        else:
            await session.execute(
                text("UPDATE tasks SET status = :status, completed_at = :now WHERE id = :task_id"),
                {
                    "task_id": task_id,
                    "now": utc_now(),
                    "status": TASK_FAILED,
                },
            )


def create_background_handler() -> BackgroundHandler:
    """Create the appropriate handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteBackgroundHandler

        return SqliteBackgroundHandler()

    from .pg_handler import PgBackgroundHandler

    return PgBackgroundHandler()
