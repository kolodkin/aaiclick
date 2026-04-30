"""Tests for task retry logic with PENDING_CLEANUP lifecycle."""

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlmodel import select

from ..background.test_pending_cleanup import run_pending_cleanup
from ..factories import create_job, create_task
from ..jobs import get_task
from ..models import TASK_CANCELLED, TASK_COMPLETED, TASK_FAILED, TASK_PENDING_CLEANUP, TASK_RUNNING, Task, TaskStatus
from ..orch_context import get_sql_session
from .claiming import claim_next_task, update_task_status
from .mp_worker import mp_worker_main_loop
from .worker import (
    _set_pending_cleanup,
    deregister_worker,
    register_worker,
)


async def _cancel_all_pending_tasks():
    """Cancel all pending/running/pending_cleanup tasks to prevent interference."""
    async with get_sql_session() as session:
        await session.execute(
            text(
                "UPDATE tasks SET status = 'CANCELLED', completed_at = :now "
                "WHERE status IN ('PENDING', 'CLAIMED', 'RUNNING', 'PENDING_CLEANUP')"
            ),
            {"now": datetime.utcnow()},
        )
        await session.commit()


async def test_set_pending_cleanup(orch_ctx):
    """_set_pending_cleanup transitions a task to PENDING_CLEANUP with error."""
    job = await create_job(
        "test_pending_cleanup",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=3,
        ),
    )

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        t = result.scalar_one()
        task_id = t.id

    await update_task_status(task_id, TASK_RUNNING)
    await _set_pending_cleanup(task_id, "test error")

    t = await get_task(task_id)
    assert t is not None
    assert t.status == TASK_PENDING_CLEANUP
    assert t.error == "test error"


async def test_claim_respects_retry_after(orch_ctx):
    """Tasks with future retry_after are not claimed."""
    await _cancel_all_pending_tasks()

    job = await create_job(
        "test_claim_retry",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
            max_retries=1,
        ),
    )

    # Set retry_after to future
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id).with_for_update())
        t = result.scalar_one()
        task_id = t.id
        t.retry_after = datetime.utcnow() + timedelta(hours=1)
        session.add(t)
        await session.commit()

    worker = await register_worker()

    # Should not be claimed (retry_after in the future)
    claimed = await claim_next_task(worker.id)
    assert claimed is None

    # Set retry_after to past
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id).with_for_update())
        t = result.scalar_one()
        t.retry_after = datetime.utcnow() - timedelta(seconds=1)
        session.add(t)
        await session.commit()

    # Now it should be claimable
    claimed = await claim_next_task(worker.id)
    assert claimed is not None
    assert claimed.id == task_id

    await deregister_worker(worker.id)


async def _run_until_terminal(job_id: int, max_cycles: int = 20) -> None:
    """Run worker + cleanup cycles until the task reaches a terminal state.

    Each cycle: check status → run worker (executes one task) → run background cleanup.
    Raises AssertionError if max_cycles is exhausted without reaching a terminal state.

    Shared with ``test_retry_mp`` — the mp-worker tests must live in a
    dedicated module so ``orch_ctx_no_ch`` can be module-scoped.
    """
    for _ in range(max_cycles):
        async with get_sql_session() as session:
            result = await session.execute(select(Task.status).where(Task.job_id == job_id))
            status = result.scalar_one()
            if status in (TASK_COMPLETED, TASK_FAILED, TASK_CANCELLED):
                return

        await mp_worker_main_loop(
            max_tasks=1,
            install_signal_handlers=False,
            max_empty_polls=1,
        )
        await run_pending_cleanup()

    raise AssertionError(f"Task did not reach terminal state after {max_cycles} cycles")
