"""Tests for PENDING_CANCELLED_CLEANUP background processing.

Verifies that the background worker:
1. Waits for a cancelled task's worker to report the killed run back
2. Drops the last run's run_refs and the task's pin_refs, then settles it to CANCELLED
3. Rolls the job up afterwards without touching a job cancel_job already closed
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.env import get_db_url

from ...datetime_utils import utc_now
from .conftest import get_run_refs, insert_job, insert_pin_ref, insert_run_ref
from .test_failure_cleanup import _get_job_status, _get_pin_refs, _get_task_status, _insert_task, _make_worker

DEAD_WORKER_ID = 999


async def run_cancelled_cleanup() -> None:
    """Run one cancelled-cleanup pass against the current orch_context DB."""
    worker = BackgroundWorker(poll_interval=0)
    worker._engine = create_async_engine(get_db_url(), echo=False)
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    try:
        await worker._process_cancelled_cleanup()
    finally:
        await worker._engine.dispose()


async def _insert_dead_worker(engine, worker_id: int, *, stale_by: timedelta) -> None:
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO execution_workers (id, hostname, pid, status, created_at, started_at, last_heartbeat, "
                "tasks_completed, tasks_failed) "
                "VALUES (:id, 'host', 1, 'ACTIVE', :now, :now, :heartbeat, 0, 0)"
            ),
            {"id": worker_id, "now": utc_now(), "heartbeat": utc_now() - stale_by},
        )
        await session.commit()


async def _get_worker_ownership(engine, task_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT execution_worker_id FROM tasks WHERE id = :id"), {"id": task_id})
        return result.scalar_one()


async def test_cancelled_cleanup_settles_unowned_task(bg_db):
    """An unowned cancelled task loses its last run's run_refs and its pin_refs
    and settles to CANCELLED; the cancelled job stays CANCELLED."""
    await insert_job(bg_db, 1000, status="CANCELLED")
    await _insert_task(bg_db, 100, 1000, status="PENDING_CANCELLED_CLEANUP", run_ids="[111]")
    await insert_run_ref(bg_db, "t_partial", "111")
    await insert_pin_ref(bg_db, "t_upstream", 100)
    await insert_pin_ref(bg_db, "t_upstream", 200)

    await _make_worker(bg_db)._process_cancelled_cleanup()

    status, _, _, _, _, completed_at = await _get_task_status(bg_db, 100)
    assert status == "CANCELLED"
    assert completed_at is not None
    assert await get_run_refs(bg_db, "t_partial") == set()
    assert await _get_pin_refs(bg_db, 100) == set()
    assert await _get_pin_refs(bg_db, 200) == {"t_upstream"}
    assert await _get_job_status(bg_db, 1000) == "CANCELLED"


async def test_cancelled_cleanup_waits_for_owning_worker(bg_db):
    """A cancelled task still owned by a worker is being stopped: nothing is
    dropped until the worker reports the run back."""
    await insert_job(bg_db, 1000, status="CANCELLED")
    await _insert_task(
        bg_db, 100, 1000, status="PENDING_CANCELLED_CLEANUP", run_ids="[111]", execution_worker_id=DEAD_WORKER_ID
    )
    await insert_run_ref(bg_db, "t_partial", "111")
    await insert_pin_ref(bg_db, "t_upstream", 100)

    await _make_worker(bg_db)._process_cancelled_cleanup()

    status, _, _, _, _, _ = await _get_task_status(bg_db, 100)
    assert status == "PENDING_CANCELLED_CLEANUP"
    assert await get_run_refs(bg_db, "t_partial") == {"111"}
    assert await _get_pin_refs(bg_db, 100) == {"t_upstream"}


async def test_cancelled_cleanup_fails_job_after_sibling_abort(bg_db):
    """A fail-fast sibling settling to CANCELLED is what lets its still-RUNNING
    job roll up to FAILED."""
    await insert_job(bg_db, 1000, status="RUNNING")
    await _insert_task(bg_db, 100, 1000, status="FAILED")
    await _insert_task(bg_db, 101, 1000, status="PENDING_CANCELLED_CLEANUP")

    await _make_worker(bg_db)._process_cancelled_cleanup()

    status, _, _, _, _, _ = await _get_task_status(bg_db, 101)
    assert status == "CANCELLED"
    assert await _get_job_status(bg_db, 1000) == "FAILED"


async def test_dead_worker_releases_cancelled_task(bg_db):
    """A worker that dies mid-kill never reports back; the dead-worker sweep
    releases its cancelled task so the next cleanup pass can settle it."""
    await insert_job(bg_db, 1000, status="CANCELLED")
    await _insert_dead_worker(bg_db, DEAD_WORKER_ID, stale_by=timedelta(seconds=120))
    await _insert_task(
        bg_db, 100, 1000, status="PENDING_CANCELLED_CLEANUP", run_ids="[111]", execution_worker_id=DEAD_WORKER_ID
    )
    worker = _make_worker(bg_db)

    await worker._cleanup_dead_workers()
    assert await _get_worker_ownership(bg_db, 100) is None
    status, _, _, _, _, _ = await _get_task_status(bg_db, 100)
    assert status == "PENDING_CANCELLED_CLEANUP"

    await worker._process_cancelled_cleanup()
    status, _, _, _, _, _ = await _get_task_status(bg_db, 100)
    assert status == "CANCELLED"
