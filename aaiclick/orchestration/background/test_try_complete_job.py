"""Direct unit tests for try_complete_job.

Pins the contract independently of the integration paths in BackgroundWorker
and worker: when all tasks are terminal, a job transitions to COMPLETED
(or FAILED if any task failed); otherwise it stays untouched.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.orchestration.background.handler import JOB_FAILED_ERROR, try_complete_job

from .conftest import insert_job


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
    await insert_job(bg_db, 1)

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "RUNNING"
    assert completed_at is None
    assert error is None


async def test_try_complete_job_all_completed_marks_completed(bg_db):
    """All tasks COMPLETED → job COMPLETED with completed_at set."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "COMPLETED", "COMPLETED"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "COMPLETED"
    assert completed_at is not None
    assert error is None


async def test_try_complete_job_any_failed_marks_failed(bg_db):
    """Any FAILED task among terminal statuses → job FAILED with error message."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "FAILED", "COMPLETED"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "FAILED"
    assert completed_at is not None
    assert error == JOB_FAILED_ERROR


@pytest.mark.parametrize("non_terminal", ["PENDING", "CLAIMED", "RUNNING", "PENDING_CLEANUP"])
async def test_try_complete_job_non_terminal_is_noop(bg_db, non_terminal):
    """Any non-terminal task → job stays RUNNING, no completed_at set."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", non_terminal])

    await _run_try_complete(bg_db, 1)

    status, completed_at, _ = await _get_job(bg_db, 1)
    assert status == "RUNNING"
    assert completed_at is None


async def test_try_complete_job_cancelled_counts_as_terminal_non_failed(bg_db):
    """CANCELLED tasks are terminal and not failed → job COMPLETED."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "CANCELLED"])

    await _run_try_complete(bg_db, 1)

    status, _, _ = await _get_job(bg_db, 1)
    assert status == "COMPLETED"
