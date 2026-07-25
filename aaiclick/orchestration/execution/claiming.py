"""Atomic task claiming and cancellation for distributed workers."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from ...datetime_utils import utc_now
from ..background.handler import in_clause
from ..models import (
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_RUNNING,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_PENDING_CLEANUP,
    TASK_RUNNING,
    Job,
    JobStatus,
    Task,
    TaskStatus,
)
from ..orch_context import get_db_handler, get_sql_session

_TERMINAL_JOB_STATUSES = (JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED)


class JobNotFound(ValueError):
    """Raised when no job with the given id exists."""


class JobAlreadyTerminal(ValueError):
    """Raised when attempting to cancel a job that is already in a terminal state."""


class TaskNotFound(ValueError):
    """Raised when no task with the given id exists."""


async def claim_next_task(execution_worker_id: int) -> Task | None:
    """
    Atomically claim the next available task for a worker.

    Delegates to the backend-specific handler from the orchestration context.

    When the first task of a job is claimed:
    - Job status transitions from PENDING to RUNNING
    - Job's started_at is set to current time

    Dependency checking:
    - Task -> Task: Task waits for previous task to complete
    - Group -> Task: Task waits for all tasks in previous group to complete
    - Task -> Group: Tasks in group wait for previous task to complete
    - Group -> Group: Tasks in group wait for all tasks in previous group to complete

    Args:
        execution_worker_id: ID of the worker claiming the task

    Returns:
        Task if one was claimed, None if no tasks available
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        task = await handler.claim_next_task(session, execution_worker_id, utc_now())
        await session.commit()
        return task


async def update_task_status(
    task_id: int,
    status: TaskStatus,
    error: str | None = None,
    result: dict | None = None,
    expected_epoch: int | None = None,
) -> bool:
    """
    Update a task's status and optional error/result.

    Also updates the last entry in run_statuses to match the new status.

    Args:
        task_id: Task ID to update
        status: New status
        error: Error message (for FAILED status)
        result: Result reference (for COMPLETED status)
        expected_epoch: If given, the write is rejected when the task's
            ``run_epoch`` no longer matches — the fencing guard that lets a
            ``clear_task`` invalidate an in-flight run's late writes.

    Returns:
        bool: True if task was found and updated
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        query = handler.lock_query(select(Task).where(Task.id == task_id))
        query_result = await session.execute(query)
        task = query_result.scalar_one_or_none()
        if task is None:
            return False

        if task.status == TASK_CANCELLED:
            return False

        if expected_epoch is not None and task.run_epoch != expected_epoch:
            return False

        task.status = status
        if status == TASK_RUNNING:
            task.started_at = utc_now()
        elif status in (TASK_COMPLETED, TASK_FAILED):
            task.completed_at = utc_now()
            if error:
                task.error = error
            if result:
                task.result = result

        if task.run_statuses:
            task.run_statuses = [*task.run_statuses[:-1], status]

        session.add(task)
        await session.commit()
        return True


async def update_job_status(job_id: int, status: JobStatus, error: str | None = None) -> bool:
    """
    Update a job's status.

    Args:
        job_id: Job ID to update
        status: New status
        error: Error message (for FAILED status)

    Returns:
        bool: True if job was found and updated
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        query = handler.lock_query(select(Job).where(Job.id == job_id))
        query_result = await session.execute(query)
        job = query_result.scalar_one_or_none()
        if job is None:
            return False

        job.status = status
        if status in (JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED):
            job.completed_at = utc_now()
            if error:
                job.error = error

        session.add(job)
        await session.commit()
        return True


async def cancel_job(job_id: int) -> Job:
    """
    Cancel a job and all its non-terminal tasks.

    Atomically transitions the job to CANCELLED and bulk-updates all
    PENDING, CLAIMED, and RUNNING tasks to CANCELLED. Tasks already
    COMPLETED or FAILED are left unchanged.

    Only PENDING and RUNNING jobs can be cancelled.

    Args:
        job_id: Job ID to cancel

    Returns:
        Job: The cancelled Job row, refreshed from the session.

    Raises:
        JobNotFound: If no job with ``job_id`` exists.
        JobAlreadyTerminal: If the job is already in a terminal state
            (COMPLETED, FAILED, CANCELLED).
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        query = handler.lock_query(select(Job).where(Job.id == job_id))
        query_result = await session.execute(query)
        job = query_result.scalar_one_or_none()
        if job is None:
            raise JobNotFound(f"Job {job_id} not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            raise JobAlreadyTerminal(f"Job {job_id} already in terminal state: {job.status}")

        now = utc_now()
        job.status = JOB_CANCELLED
        job.completed_at = now
        session.add(job)

        await session.execute(
            text(
                "UPDATE tasks SET status = :cancelled_status, "
                "completed_at = :now "
                "WHERE job_id = :job_id "
                "AND status IN (:pending, :claimed, :running, :pending_cleanup)"
            ),
            {
                "cancelled_status": TASK_CANCELLED,
                "now": now,
                "job_id": job_id,
                "pending": TASK_PENDING,
                "claimed": TASK_CLAIMED,
                "running": TASK_RUNNING,
                "pending_cleanup": TASK_PENDING_CLEANUP,
            },
        )

        await session.commit()
        return job


async def check_task_cancelled(task_id: int) -> bool:
    """
    Check if a task has been cancelled.

    Used by the worker's cancellation monitor to detect when a running
    task's job has been cancelled via cancel_job().

    Args:
        task_id: Task ID to check

    Returns:
        bool: True if task status is CANCELLED
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Task.status).where(Task.id == task_id))
        status = result.scalar_one_or_none()
        return status == TASK_CANCELLED


async def check_run_aborted(task_id: int, expected_epoch: int) -> bool:
    """Return True when a worker's current run should abort.

    A run is aborted when its task was cancelled (``cancel_job``) or cleared
    (``clear_task`` bumps ``run_epoch``). Used by the worker's cancellation
    monitor to interrupt an in-flight task whose ownership has moved on.

    A missing task returns False — the monitor simply stops on the next poll.
    """
    async with get_sql_session() as session:
        row = (await session.execute(select(Task.status, Task.run_epoch).where(Task.id == task_id))).one_or_none()
    if row is None:
        return False
    status, run_epoch = row
    return status == TASK_CANCELLED or run_epoch != expected_epoch


async def _downstream_task_ids(session: AsyncSession, task_id: int) -> set[int]:
    """Return every task id transitively downstream of ``task_id`` (excluding it).

    Walks the four dependency edge shapes the scheduler already understands
    (task→task, task→group, group→task, group→group). A grouped task pulls in
    the group's *consumers* — the group is no longer fully complete — but never
    its parallel siblings, which are not downstream.
    """

    def classify(edges, task_dest: set[int], group_dest: set[int]) -> None:
        """Sort dependency ``(next_id, next_type)`` rows into task vs group sets."""
        for next_id, next_type in edges:
            (task_dest if next_type == "task" else group_dest).add(next_id)

    downstream: set[int] = set()
    frontier = {task_id}
    while frontier:
        task_ids = list(frontier)
        tph, tparams = in_clause(task_ids, "t")
        rows = await session.execute(
            text(f"SELECT DISTINCT group_id FROM tasks WHERE id IN ({tph}) AND group_id IS NOT NULL"),
            tparams,
        )
        group_ids = [r[0] for r in rows]

        succ_task_ids: set[int] = set()
        succ_group_ids: set[int] = set()

        # Edges originating from the frontier tasks themselves.
        classify(
            await session.execute(
                text(
                    f"SELECT next_id, next_type FROM dependencies WHERE previous_type = 'task' AND previous_id IN ({tph})"
                ),
                tparams,
            ),
            succ_task_ids,
            succ_group_ids,
        )

        # Edges originating from the groups those frontier tasks belong to.
        if group_ids:
            gph, gparams = in_clause(group_ids, "g")
            classify(
                await session.execute(
                    text(
                        f"SELECT next_id, next_type FROM dependencies "
                        f"WHERE previous_type = 'group' AND previous_id IN ({gph})"
                    ),
                    gparams,
                ),
                succ_task_ids,
                succ_group_ids,
            )

        # Resolve any successor groups to their member tasks.
        if succ_group_ids:
            sgph, sgparams = in_clause(sorted(succ_group_ids), "sg")
            member_rows = await session.execute(
                text(f"SELECT id FROM tasks WHERE group_id IN ({sgph})"),
                sgparams,
            )
            succ_task_ids.update(r[0] for r in member_rows)

        new = succ_task_ids - downstream - {task_id}
        downstream |= new
        frontier = new
    return downstream


async def clear_task(task_id: int) -> tuple[list[int], Job]:
    """Reset a task and all its transitive downstream tasks to PENDING.

    Mirrors Airflow's "clear task": the target task and every task that
    depends on it (directly or through groups) are reset to ``PENDING`` for
    re-run. Upstream tasks and their output tables are left untouched.

    Each affected task's ``run_epoch`` is bumped so any worker currently
    executing one of them has its late writes fenced off (see
    ``check_run_aborted`` / ``update_task_status``). A terminal job
    (COMPLETED / FAILED / CANCELLED) is reactivated to ``RUNNING`` so the
    cleared tasks are claimable again.

    Args:
        task_id: Task to clear, along with its downstream.

    Returns:
        ``(cleared_task_ids, job)`` — the sorted affected ids and the job
        row (reactivated to ``RUNNING`` if it had been terminal).

    Raises:
        TaskNotFound: If no task with ``task_id`` exists.
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        task = (await session.execute(handler.lock_query(select(Task).where(Task.id == task_id)))).scalar_one_or_none()
        if task is None:
            raise TaskNotFound(f"Task {task_id} not found")

        affected = sorted({task_id} | await _downstream_task_ids(session, task_id))

        ph, params = in_clause(affected, "id")
        await session.execute(
            text(
                f"UPDATE tasks SET status = :pending, run_epoch = run_epoch + 1, "
                f"execution_worker_id = NULL, claimed_at = NULL, started_at = NULL, "
                f"completed_at = NULL, error = NULL, result = NULL, "
                f"retry_after = NULL WHERE id IN ({ph})"
            ),
            {**params, "pending": TASK_PENDING},
        )

        job = (await session.execute(handler.lock_query(select(Job).where(Job.id == task.job_id)))).scalar_one()
        if job.status in _TERMINAL_JOB_STATUSES:
            job.status = JOB_RUNNING
            job.completed_at = None
            job.error = None
            session.add(job)

        await session.commit()
        return affected, job
