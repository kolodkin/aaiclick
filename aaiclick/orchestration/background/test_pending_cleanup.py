"""Tests for PENDING_CLEANUP background processing.

Verifies that the background worker correctly:
1. Cleans run_refs and pin_refs for failed tasks
2. Transitions PENDING_CLEANUP → PENDING (retries remaining) or FAILED (exhausted)
3. Checks job completion after marking tasks FAILED
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.background.background_worker import BackgroundWorker, RETRY_BASE_DELAY
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.env import get_db_url

from .conftest import get_run_refs, insert_job, insert_pin_ref, insert_run_ref


async def _insert_task(engine, task_id, job_id, *, status, attempt=0, max_retries=0,
                       run_ids="[]", error=None, worker_id=None):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, status, "
                "created_at, max_retries, attempt, run_ids, run_statuses, error, worker_id) "
                "VALUES (:id, :job_id, 'test.func', 'test', '{}', :status, "
                ":now, :max_retries, :attempt, :run_ids, '[]', :error, :worker_id)"
            ),
            {
                "id": task_id, "job_id": job_id, "status": status,
                "now": datetime.utcnow(), "max_retries": max_retries,
                "attempt": attempt, "run_ids": run_ids, "error": error,
                "worker_id": worker_id,
            },
        )
        await session.commit()


async def _get_task_status(engine, task_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                "SELECT status, attempt, retry_after, error, worker_id, completed_at "
                "FROM tasks WHERE id = :id"
            ),
            {"id": task_id},
        )
        return result.fetchone()


async def _get_pin_refs(engine, task_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT table_name FROM table_pin_refs WHERE task_id = :tid"),
            {"tid": task_id},
        )
        return {row[0] for row in result.fetchall()}


async def _get_job_status(engine, job_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT status FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        row = result.fetchone()
        return row[0] if row else None


def _make_worker(engine):
    """Create a BackgroundWorker wired to the given engine."""
    worker = BackgroundWorker()
    worker._engine = engine
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    return worker


async def run_pending_cleanup() -> None:
    """Process PENDING_CLEANUP tasks using the current orch_context DB.

    Test helper that creates a temporary BackgroundWorker pointed at the
    same SQL database and runs a single cleanup cycle.
    """
    worker = BackgroundWorker(poll_interval=0)
    worker._engine = create_async_engine(get_db_url(), echo=False)
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    try:
        await worker._process_pending_cleanup()
    finally:
        await worker._engine.dispose()


async def test_pending_cleanup_transitions_to_pending_with_retries(bg_db):
    """PENDING_CLEANUP task with retries → PENDING with incremented attempt."""
    await insert_job(bg_db, 1000)
    await _insert_task(
        bg_db, 100, 1000,
        status="PENDING_CLEANUP",
        attempt=0,
        max_retries=3,
        run_ids='[111]',
        error="task failed",
        worker_id=999,
    )
    await insert_run_ref(bg_db, "t_intermediate", "111")

    await _make_worker(bg_db)._process_pending_cleanup()

    row = await _get_task_status(bg_db, 100)
    status, attempt, retry_after, error, worker_id, completed_at = row
    assert status == "PENDING"
    assert attempt == 1
    assert retry_after is not None
    assert worker_id is None

    assert await get_run_refs(bg_db, "t_intermediate") == set()


async def test_pending_cleanup_transitions_to_failed_no_retries(bg_db):
    """PENDING_CLEANUP task with no retries → FAILED."""
    await insert_job(bg_db, 1000)
    await _insert_task(
        bg_db, 100, 1000,
        status="PENDING_CLEANUP",
        attempt=0,
        max_retries=0,
        run_ids='[111]',
        error="task failed",
    )
    await insert_run_ref(bg_db, "t_table", "111")

    await _make_worker(bg_db)._process_pending_cleanup()

    row = await _get_task_status(bg_db, 100)
    status, attempt, retry_after, error, worker_id, completed_at = row
    assert status == "FAILED"
    assert completed_at is not None

    assert await get_run_refs(bg_db, "t_table") == set()


async def test_pending_cleanup_cleans_pin_refs(bg_db):
    """PENDING_CLEANUP processing removes pin_refs for the task."""
    await insert_job(bg_db, 1000)
    await _insert_task(
        bg_db, 100, 1000,
        status="PENDING_CLEANUP",
        attempt=0,
        max_retries=1,
        run_ids='[111]',
        error="task failed",
    )
    await insert_pin_ref(bg_db, "t_upstream_data", 100)
    await insert_pin_ref(bg_db, "t_other_data", 100)
    await insert_pin_ref(bg_db, "t_upstream_data", 200)

    await _make_worker(bg_db)._process_pending_cleanup()

    assert await _get_pin_refs(bg_db, 100) == set()
    assert await _get_pin_refs(bg_db, 200) == {"t_upstream_data"}


async def test_pending_cleanup_completes_job_when_all_failed(bg_db):
    """Job transitions to FAILED when last task transitions from PENDING_CLEANUP to FAILED."""
    await insert_job(bg_db, 1000, status="RUNNING")
    await _insert_task(bg_db, 100, 1000, status="COMPLETED")
    await _insert_task(
        bg_db, 101, 1000,
        status="PENDING_CLEANUP",
        attempt=0,
        max_retries=0,
        run_ids='[222]',
        error="oops",
    )

    await _make_worker(bg_db)._process_pending_cleanup()

    assert await _get_job_status(bg_db, 1000) == "FAILED"


async def test_pending_cleanup_does_not_complete_job_with_retries(bg_db):
    """Job stays RUNNING when PENDING_CLEANUP task transitions to PENDING (has retries)."""
    await insert_job(bg_db, 1000, status="RUNNING")
    await _insert_task(
        bg_db, 100, 1000,
        status="PENDING_CLEANUP",
        attempt=0,
        max_retries=2,
        run_ids='[111]',
        error="will retry",
    )

    await _make_worker(bg_db)._process_pending_cleanup()

    assert await _get_job_status(bg_db, 1000) == "RUNNING"


async def test_pending_cleanup_retry_backoff(bg_db):
    """Retry backoff doubles each attempt: 1s, 2s, 4s."""
    await insert_job(bg_db, 1000)
    await _insert_task(
        bg_db, 100, 1000,
        status="PENDING_CLEANUP",
        attempt=1,
        max_retries=5,
        run_ids='[111, 222]',
        error="failed again",
    )

    before = datetime.utcnow()
    await _make_worker(bg_db)._process_pending_cleanup()

    row = await _get_task_status(bg_db, 100)
    status, attempt, retry_after_str, error, worker_id, completed_at = row
    assert status == "PENDING"
    assert attempt == 2

    retry_after = datetime.fromisoformat(retry_after_str) if isinstance(retry_after_str, str) else retry_after_str
    delay = (retry_after - before).total_seconds()
    expected = RETRY_BASE_DELAY * (2 ** 1)
    assert delay >= expected * 0.9
    assert delay <= expected + 1.0
