"""Tests for task retry logic with PENDING_FAILURE_CLEANUP lifecycle."""

from datetime import timedelta

from sqlalchemy import text
from sqlmodel import select

from ...datetime_utils import utc_now
from ..background.test_failure_cleanup import run_failure_cleanup
from ..factories import create_job, create_task
from ..models import TASK_CANCELLED, TASK_COMPLETED, TASK_FAILED, Task
from ..orch_context import get_sql_session
from .claiming import claim_next_task
from .execution_worker import (
    deregister_execution_worker,
    register_execution_worker,
)
from .mp_worker import mp_worker_main_loop


async def _cancel_all_pending_tasks():
    """Cancel all pending/running/failure-cleanup tasks to prevent interference."""
    async with get_sql_session() as session:
        await session.execute(
            text(
                "UPDATE tasks SET status = 'CANCELLED', completed_at = :now "
                "WHERE status IN ('PENDING', 'CLAIMED', 'RUNNING', 'PENDING_FAILURE_CLEANUP')"
            ),
            {"now": utc_now()},
        )
        await session.commit()


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
        t.retry_after = utc_now() + timedelta(hours=1)
        session.add(t)
        await session.commit()

    worker = await register_execution_worker()

    # Should not be claimed (retry_after in the future)
    claimed = await claim_next_task(worker.id)
    assert claimed is None

    # Set retry_after to past
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id).with_for_update())
        t = result.scalar_one()
        t.retry_after = utc_now() - timedelta(seconds=1)
        session.add(t)
        await session.commit()

    # Now it should be claimable
    claimed = await claim_next_task(worker.id)
    assert claimed is not None
    assert claimed.id == task_id

    await deregister_execution_worker(worker.id)


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
        await run_failure_cleanup()

    raise AssertionError(f"Task did not reach terminal state after {max_cycles} cycles")
