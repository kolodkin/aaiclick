"""Direct unit tests for try_complete_job.

Pins the contract independently of the integration paths in BackgroundWorker
and worker: when all tasks are terminal, a job transitions to COMPLETED
(or FAILED if any task failed); otherwise it stays untouched.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.orchestration.background.handler import JOB_FAILED_ERROR, try_complete_job


async def _insert_job(engine, job_id, status="RUNNING"):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO jobs (id, name, status, run_type, created_at) "
                "VALUES (:id, 'test_job', :status, 'MANUAL', :now)"
            ),
            {"id": job_id, "status": status, "now": datetime.utcnow()},
        )
        await session.commit()


async def _insert_tasks(engine, job_id, statuses):
    """Insert one task per status for a given job."""
    async with AsyncSession(engine) as session:
        for idx, status in enumerate(statuses):
            await session.execute(
                text(
                    "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, "
                    "status, created_at, max_retries, attempt, run_statuses) "
                    "VALUES (:id, :job_id, 'test.func', 'test', '{}', "
                    ":status, :now, 0, 0, '[]')"
                ),
                {
                    "id": job_id * 100 + idx,
                    "job_id": job_id,
                    "status": status,
                    "now": datetime.utcnow(),
                },
            )
        await session.commit()


async def _get_job(engine, job_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT status, completed_at, error FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        return result.fetchone()


async def _run_try_complete(engine, job_id):
    async with AsyncSession(engine) as session:
        await try_complete_job(session, job_id)
        await session.commit()


async def test_try_complete_job_empty_job_is_noop(bg_db):
    """Job with no tasks is a no-op — stays as-is."""
    await _insert_job(bg_db, 1, status="RUNNING")

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "RUNNING"
    assert completed_at is None
    assert error is None


async def test_try_complete_job_all_completed_marks_completed(bg_db):
    """All tasks COMPLETED → job COMPLETED with completed_at set."""
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "COMPLETED", "COMPLETED"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "COMPLETED"
    assert completed_at is not None
    assert error is None


async def test_try_complete_job_any_failed_marks_failed(bg_db):
    """Any FAILED task among terminal statuses → job FAILED with error message."""
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "FAILED", "COMPLETED"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "FAILED"
    assert completed_at is not None
    assert error == JOB_FAILED_ERROR


async def test_try_complete_job_pending_is_noop(bg_db):
    """Any PENDING task → no-op, job stays RUNNING."""
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "PENDING"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "RUNNING"
    assert completed_at is None


async def test_try_complete_job_claimed_is_noop(bg_db):
    """Any CLAIMED task → no-op."""
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "CLAIMED"])

    await _run_try_complete(bg_db, 1)

    status, _, _ = await _get_job(bg_db, 1)
    assert status == "RUNNING"


async def test_try_complete_job_running_is_noop(bg_db):
    """Any RUNNING task → no-op."""
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "RUNNING"])

    await _run_try_complete(bg_db, 1)

    status, _, _ = await _get_job(bg_db, 1)
    assert status == "RUNNING"


async def test_try_complete_job_pending_cleanup_is_noop(bg_db):
    """Any PENDING_CLEANUP task → no-op (cleanup still in flight)."""
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "PENDING_CLEANUP"])

    await _run_try_complete(bg_db, 1)

    status, _, _ = await _get_job(bg_db, 1)
    assert status == "RUNNING"


async def test_try_complete_job_cancelled_counts_as_terminal_non_failed(bg_db):
    """CANCELLED tasks are terminal and not failed → job COMPLETED.

    In practice cancellation is initiated via ``cancel_job()`` which transitions
    the job itself to CANCELLED, so ``try_complete_job`` rarely sees CANCELLED
    tasks. This test documents the contract for the edge case where it does.
    """
    await _insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "CANCELLED"])

    await _run_try_complete(bg_db, 1)

    status, _, _ = await _get_job(bg_db, 1)
    assert status == "COMPLETED"
