"""Background handler protocol and factory dispatch.

Concrete implementations live in sqlite_handler.py and pg_handler.py.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import NamedTuple, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.backend import is_sqlite

from ...datetime_utils import utc_now
from ..execution.sql_loader import load_sql
from ..models import (
    CANCELLABLE_TASK_STATUSES,
    JOB_COMPLETED,
    JOB_FAILED,
    TASK_CANCELLED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_PENDING_CANCELLED_CLEANUP,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_UPSTREAM_FAILED,
    TaskStatus,
)

JOB_FAILED_ERROR = "One or more tasks failed"
UPSTREAM_FAILED_ERROR = "Upstream task failed"
GROUP_SIBLING_ABORTED_ERROR = "Aborted: a sibling task in the group failed"
DEAD_WORKER_ERROR = "ExecutionWorker died (heartbeat timeout)"

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
              AND prev.status IN (:failed, :cancelling, :cancelled, :upstream_failed)
        )
        OR EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = tasks.id
              AND d.next_type = 'task'
              AND d.previous_type = 'group'
              AND prev.status IN (:failed, :cancelling, :cancelled, :upstream_failed)
        )
        OR (tasks.group_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = tasks.group_id
              AND d.next_type = 'group'
              AND d.previous_type = 'task'
              AND prev.status IN (:failed, :cancelling, :cancelled, :upstream_failed)
        ))
        OR (tasks.group_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = tasks.group_id
              AND d.next_type = 'group'
              AND d.previous_type = 'group'
              AND prev.status IN (:failed, :cancelling, :cancelled, :upstream_failed)
        ))
      )
"""


async def cascade_upstream_failed(session: AsyncSession, job_id: int) -> int:
    """Mark transitively-downstream PENDING tasks as UPSTREAM_FAILED.

    A ``PENDING_CANCELLED_CLEANUP`` upstream already counts as failed: it can
    only settle to ``CANCELLED``.

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
        "cancelling": TASK_PENDING_CANCELLED_CLEANUP,
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


class CancellingTransition(NamedTuple):
    """SQL fragments for an UPDATE that moves tasks to PENDING_CANCELLED_CLEANUP.

    Shared by ``cancel_job`` and the group-sibling abort so the ownership rule
    lives once: a ``PENDING_FAILURE_CLEANUP`` task was already reported back,
    so its ``execution_worker_id`` is released with the transition; any other
    cancellable task stays owned until its worker reports the killed run.
    """

    set_sql: str
    cancellable_sql: str
    params: dict


def cancelling_transition() -> CancellingTransition:
    """Build the SET clause, the cancellable-status IN list, and their params."""
    ph, params = in_clause(list(CANCELLABLE_TASK_STATUSES), "st")
    return CancellingTransition(
        set_sql=(
            "status = :cancelling, "
            "execution_worker_id = CASE WHEN status = :failure_cleanup THEN NULL ELSE execution_worker_id END"
        ),
        cancellable_sql=ph,
        params={
            **params,
            "cancelling": TASK_PENDING_CANCELLED_CLEANUP,
            "failure_cleanup": TASK_PENDING_FAILURE_CLEANUP,
        },
    )


_CASCADE_ABORT_GROUP_SIBLINGS_SQL = """
    UPDATE tasks SET {set_sql}, error = :error_msg
    WHERE job_id = :job_id
      AND status IN ({cancellable})
      AND group_id IS NOT NULL
      AND group_id IN (
        SELECT group_id FROM tasks sib
        WHERE sib.job_id = :job_id
          AND sib.group_id IS NOT NULL
          AND sib.status IN (:failed, :cancelling, :cancelled, :upstream_failed)
      )
"""


async def cascade_abort_group_siblings(session: AsyncSession, job_id: int) -> int:
    """Cancel still-active siblings of any failed/cancelled group member (fail-fast).

    A group with a member in a non-success state (``FAILED``,
    ``PENDING_CANCELLED_CLEANUP``, ``CANCELLED``, ``UPSTREAM_FAILED``) has
    broken its all-success contract, so its remaining siblings are wasted
    compute. They get the same transition as ``cancel_job``: a ``RUNNING``
    sibling's cancellation monitor (``execution/claiming.py``) aborts the run,
    and the cancelled-cleanup pass settles each sibling to ``CANCELLED`` once
    no worker owns it. A ``COMPLETED`` sibling is terminal and untouched.

    Runs unconditionally on every ``try_complete_job`` pass that sees a failure.
    Returns the number of siblings cancelled. The caller is responsible for
    committing.
    """
    cancelling = cancelling_transition()
    result = await session.execute(
        text(
            _CASCADE_ABORT_GROUP_SIBLINGS_SQL.format(set_sql=cancelling.set_sql, cancellable=cancelling.cancellable_sql)
        ),
        {
            **cancelling.params,
            "job_id": job_id,
            "error_msg": GROUP_SIBLING_ABORTED_ERROR,
            "cancelled": TASK_CANCELLED,
            "failed": TASK_FAILED,
            "upstream_failed": TASK_UPSTREAM_FAILED,
        },
    )
    return cast(CursorResult, result).rowcount or 0


def in_clause(ids: list, prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause compatible with both SQLite and PostgreSQL.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params


# The status-set knowledge lives in the SQL files, not in code. Eager loads
# keep a mis-packaged wheel failing at import.
JOB_ROLLUP_SQL = load_sql("job_rollup.sql")
COMPLETE_JOB_SQL = load_sql("complete_job.sql")


async def _job_rollup(session: AsyncSession, job_id: int) -> tuple[int, int, int, int]:
    """Aggregate a job's task statuses: (total, non_terminal, failed, cascade_trigger)."""
    result = await session.execute(text(JOB_ROLLUP_SQL), {"job_id": job_id})
    total, non_terminal, failed, cascade_trigger = result.one()
    return total, non_terminal or 0, failed or 0, cascade_trigger or 0


async def _complete_job(session: AsyncSession, job_id: int, failed: int) -> None:
    """Terminal job update, run only after the rollup saw zero non-terminal tasks.

    The SQL skips an already-terminal job, so the rollup after the last
    cancelled task settles never turns a CANCELLED job into COMPLETED."""
    await session.execute(
        text(COMPLETE_JOB_SQL),
        {
            "job_id": job_id,
            "now": utc_now(),
            "status": JOB_FAILED if failed else JOB_COMPLETED,
            "error": JOB_FAILED_ERROR if failed else None,
        },
    )


async def roll_up_job(session: AsyncSession, job_id: int) -> None:
    """The worker recipe: mark a job COMPLETED/FAILED once all tasks are terminal.

    This is the contract the execution worker follows on task success.
    No cascade: stranded downstream tasks are handled by whoever performs failure
    transitions — ``try_complete_job`` on the BackgroundWorker's cleanup passes
    — so by the time a success-path rollup runs, any UPSTREAM_FAILED sweep has
    already happened.
    """
    total, non_terminal, failed, _ = await _job_rollup(session, job_id)
    if not total or non_terminal:
        return
    await _complete_job(session, job_id, failed)


async def try_complete_job(session: AsyncSession, job_id: int) -> None:
    """Mark a job COMPLETED or FAILED if all its tasks are in terminal states.

    No-op while any task is still PENDING, CLAIMED, RUNNING, or in a cleanup
    state. The terminal check is aggregated inside SQL (one row returned
    regardless of task count) so this stays O(1) even for large jobs. Uses raw
    SQL on the passed session so it works both inside and outside an active
    ``orch_context``. The caller is responsible for committing.

    The full recipe for the BackgroundWorker's failure and cancellation
    transitions: on top of ``roll_up_job``'s shared rollup, when any task is
    in a non-success state it sweeps PENDING tasks whose transitive upstream
    failed and marks them UPSTREAM_FAILED — otherwise they would block job
    completion forever. The sweep is gated on the rollup aggregate so the
    happy path stays a single SELECT.
    """
    total, non_terminal, failed, cascade_trigger = await _job_rollup(session, job_id)
    if cascade_trigger and non_terminal:
        # Fail-fast first: a doomed group's still-active siblings become
        # cancelling upstreams (still non-terminal until the cancelled-cleanup
        # pass settles them), which the downstream UPSTREAM_FAILED sweep then
        # propagates in the same pass.
        await cascade_abort_group_siblings(session, job_id)
        marked = await cascade_upstream_failed(session, job_id)
        non_terminal -= marked
        failed += marked
    if not total or non_terminal:
        return
    await _complete_job(session, job_id, failed)


def _run_ids(value: list | str | None) -> list:
    """Decode a raw ``run_ids`` column: a list on PostgreSQL, JSON text on SQLite."""
    return value if isinstance(value, list) else json.loads(value or "[]")


class CleanupTask(NamedTuple):
    """Row returned by get_cleanup_tasks."""

    task_id: int
    job_id: int
    run_ids: list
    attempt: int
    max_retries: int


class BackgroundHandler(ABC):
    """Abstract base for backend-specific background cleanup SQL."""

    @staticmethod
    @abstractmethod
    async def mark_dead_execution_workers(
        session: AsyncSession,
        dead_execution_worker_ids: list[int],
        now: datetime,
    ) -> None:
        """Mark dead workers as STOPPED and release their tasks.

        RUNNING / CLAIMED tasks become PENDING_FAILURE_CLEANUP. A
        PENDING_CANCELLED_CLEANUP task will never be reported back, so its
        ownership is released here (as ``release_cancelled_run`` would).
        """
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
        a downstream consumer.  Called by the cleanup passes so stale pins
        don't block table cleanup.
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
    async def get_cleanup_tasks(session: AsyncSession, status: TaskStatus) -> list[CleanupTask]:
        """Return tasks in the given cleanup status whose run has ended.

        A ``PENDING_CANCELLED_CLEANUP`` task still owned by a worker is being
        stopped, so only unowned ones are returned; a failure-cleanup task
        was always reported back first.
        """
        unowned = " AND execution_worker_id IS NULL" if status == TASK_PENDING_CANCELLED_CLEANUP else ""
        result = await session.execute(
            text(f"SELECT id, job_id, run_ids, attempt, max_retries FROM tasks WHERE status = :status{unowned}"),
            {"status": status},
        )
        return [CleanupTask._make((*row[:2], _run_ids(row[2]), *row[3:])) for row in result.fetchall()]

    @staticmethod
    async def transition_failure_cleanup(
        session: AsyncSession,
        task_id: int,
        *,
        has_retries: bool,
        attempt: int,
        retry_after: datetime,
    ) -> None:
        """Transition a PENDING_FAILURE_CLEANUP task to PENDING or FAILED.

        The ``AND status = 'PENDING_FAILURE_CLEANUP'`` in each WHERE clause
        guards a race with ``clear_task`` and ``cancel_job``: if either runs
        between the sweep's read and this write, the UPDATE matches no rows
        and the other transition stands.
        """
        if has_retries:
            await session.execute(
                text(
                    "UPDATE tasks SET status = :status, "
                    "attempt = :attempt, retry_after = :retry_after, "
                    "execution_worker_id = NULL, claimed_at = NULL, "
                    "started_at = NULL, completed_at = NULL "
                    "WHERE id = :task_id AND status = :failure_cleanup"
                ),
                {
                    "task_id": task_id,
                    "attempt": attempt,
                    "retry_after": retry_after,
                    "status": TASK_PENDING,
                    "failure_cleanup": TASK_PENDING_FAILURE_CLEANUP,
                },
            )
        else:
            await session.execute(
                text(
                    "UPDATE tasks SET status = :status, completed_at = :now "
                    "WHERE id = :task_id AND status = :failure_cleanup"
                ),
                {
                    "task_id": task_id,
                    "now": utc_now(),
                    "status": TASK_FAILED,
                    "failure_cleanup": TASK_PENDING_FAILURE_CLEANUP,
                },
            )

    @staticmethod
    async def transition_cancelled_cleanup(session: AsyncSession, task_id: int) -> None:
        """Settle a PENDING_CANCELLED_CLEANUP task to CANCELLED.

        Status-guarded like ``transition_failure_cleanup`` so a ``clear_task``
        in the read/write gap stands.
        """
        await session.execute(
            text(
                "UPDATE tasks SET status = :status, completed_at = :now "
                "WHERE id = :task_id AND status = :cancelled_cleanup"
            ),
            {
                "task_id": task_id,
                "now": utc_now(),
                "status": TASK_CANCELLED,
                "cancelled_cleanup": TASK_PENDING_CANCELLED_CLEANUP,
            },
        )


def create_background_handler() -> BackgroundHandler:
    """Create the appropriate handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteBackgroundHandler

        return SqliteBackgroundHandler()

    from .pg_handler import PgBackgroundHandler

    return PgBackgroundHandler()
