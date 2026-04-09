"""Tests for PENDING_CLEANUP background processing.

Verifies that the background worker correctly:
1. Cleans run_refs and pin_refs for failed tasks
2. Transitions PENDING_CLEANUP → PENDING (retries remaining) or FAILED (exhausted)
3. Checks job completion after marking tasks FAILED
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime
from unittest.mock import AsyncMock

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.background.background_worker import BackgroundWorker, RETRY_BASE_DELAY
from aaiclick.orchestration.background.handler import BackgroundHandler
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.env import get_db_url
from aaiclick.orchestration.models import SQLModel


async def _setup_db():
    """Create a temp SQLite DB with schema, return (async_engine, tmpdir)."""
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_pctest_")
    db_path = os.path.join(tmpdir, "test.db")
    sync_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(sync_engine)
    sync_engine.dispose()
    async_engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    return async_engine, tmpdir


async def _insert_task(engine, task_id, job_id, *, status, attempt=0, max_retries=0,
                       run_ids="[]", error=None, worker_id=None):
    """Insert a task row for testing."""
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


async def _insert_job(engine, job_id, *, status="RUNNING"):
    """Insert a job row for testing."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO jobs (id, name, status, run_type, created_at) "
                "VALUES (:id, 'test_job', :status, 'MANUAL', :now)"
            ),
            {"id": job_id, "status": status, "now": datetime.utcnow()},
        )
        await session.commit()


async def _insert_run_ref(engine, table_name, run_id):
    """Insert a row into table_run_refs."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_run_refs (table_name, run_id) "
                "VALUES (:t, :r)"
            ),
            {"t": table_name, "r": run_id},
        )
        await session.commit()


async def _insert_pin_ref(engine, table_name, task_id):
    """Insert a row into table_pin_refs."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_pin_refs (table_name, task_id) "
                "VALUES (:t, :tid)"
            ),
            {"t": table_name, "tid": task_id},
        )
        await session.commit()


async def _get_task_status(engine, task_id):
    """Return (status, attempt, retry_after, error, worker_id) for a task."""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                "SELECT status, attempt, retry_after, error, worker_id, completed_at "
                "FROM tasks WHERE id = :id"
            ),
            {"id": task_id},
        )
        return result.fetchone()


async def _get_run_refs(engine, table_name):
    """Return set of run_ids for a table in table_run_refs."""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT run_id FROM table_run_refs WHERE table_name = :t"),
            {"t": table_name},
        )
        return {row[0] for row in result.fetchall()}


async def _get_pin_refs(engine, task_id):
    """Return set of table_names pinned for a task."""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT table_name FROM table_pin_refs WHERE task_id = :tid"),
            {"tid": task_id},
        )
        return {row[0] for row in result.fetchall()}


async def _get_job_status(engine, job_id):
    """Return job status."""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT status FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        row = result.fetchone()
        return row[0] if row else None


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


async def test_pending_cleanup_transitions_to_pending_with_retries():
    """PENDING_CLEANUP task with retries → PENDING with incremented attempt."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 1000)
        await _insert_task(
            engine, 100, 1000,
            status="PENDING_CLEANUP",
            attempt=0,
            max_retries=3,
            run_ids='[111]',
            error="task failed",
            worker_id=999,
        )
        await _insert_run_ref(engine, "t_intermediate", "111")

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._process_pending_cleanup()

        # Task should be PENDING with incremented attempt
        row = await _get_task_status(engine, 100)
        status, attempt, retry_after, error, worker_id, completed_at = row
        assert status == "PENDING"
        assert attempt == 1
        assert retry_after is not None
        assert worker_id is None  # Cleared for re-claim

        # Run refs should be cleaned
        assert await _get_run_refs(engine, "t_intermediate") == set()
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_pending_cleanup_transitions_to_failed_no_retries():
    """PENDING_CLEANUP task with no retries → FAILED."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 1000)
        await _insert_task(
            engine, 100, 1000,
            status="PENDING_CLEANUP",
            attempt=0,
            max_retries=0,
            run_ids='[111]',
            error="task failed",
        )
        await _insert_run_ref(engine, "t_table", "111")

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._process_pending_cleanup()

        row = await _get_task_status(engine, 100)
        status, attempt, retry_after, error, worker_id, completed_at = row
        assert status == "FAILED"
        assert completed_at is not None

        # Run refs should be cleaned
        assert await _get_run_refs(engine, "t_table") == set()
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_pending_cleanup_cleans_pin_refs():
    """PENDING_CLEANUP processing removes pin_refs for the task."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 1000)
        await _insert_task(
            engine, 100, 1000,
            status="PENDING_CLEANUP",
            attempt=0,
            max_retries=1,
            run_ids='[111]',
            error="task failed",
        )
        # Pin refs: upstream producer pinned tables for task 100
        await _insert_pin_ref(engine, "t_upstream_data", 100)
        await _insert_pin_ref(engine, "t_other_data", 100)
        # Pin refs for a different task (should NOT be cleaned)
        await _insert_pin_ref(engine, "t_upstream_data", 200)

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._process_pending_cleanup()

        # Pin refs for task 100 should be cleaned
        assert await _get_pin_refs(engine, 100) == set()
        # Pin refs for task 200 should be untouched
        assert await _get_pin_refs(engine, 200) == {"t_upstream_data"}
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_pending_cleanup_completes_job_when_all_failed():
    """Job transitions to FAILED when last task transitions from PENDING_CLEANUP to FAILED."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 1000, status="RUNNING")
        # One task already completed, one in PENDING_CLEANUP with no retries
        await _insert_task(engine, 100, 1000, status="COMPLETED")
        await _insert_task(
            engine, 101, 1000,
            status="PENDING_CLEANUP",
            attempt=0,
            max_retries=0,
            run_ids='[222]',
            error="oops",
        )

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._process_pending_cleanup()

        assert await _get_job_status(engine, 1000) == "FAILED"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_pending_cleanup_does_not_complete_job_with_retries():
    """Job stays RUNNING when PENDING_CLEANUP task transitions to PENDING (has retries)."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 1000, status="RUNNING")
        await _insert_task(
            engine, 100, 1000,
            status="PENDING_CLEANUP",
            attempt=0,
            max_retries=2,
            run_ids='[111]',
            error="will retry",
        )

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._process_pending_cleanup()

        # Job should still be RUNNING (task will be retried)
        assert await _get_job_status(engine, 1000) == "RUNNING"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_pending_cleanup_retry_backoff():
    """Retry backoff doubles each attempt: 1s, 2s, 4s."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 1000)
        await _insert_task(
            engine, 100, 1000,
            status="PENDING_CLEANUP",
            attempt=1,  # Second attempt failing
            max_retries=5,
            run_ids='[111, 222]',
            error="failed again",
        )

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        before = datetime.utcnow()
        await worker._process_pending_cleanup()

        row = await _get_task_status(engine, 100)
        status, attempt, retry_after_str, error, worker_id, completed_at = row
        assert status == "PENDING"
        assert attempt == 2

        # Parse retry_after (SQLite stores as string)
        retry_after = datetime.fromisoformat(retry_after_str) if isinstance(retry_after_str, str) else retry_after_str
        delay = (retry_after - before).total_seconds()
        # Backoff: RETRY_BASE_DELAY * 2^1 = 2s
        expected = RETRY_BASE_DELAY * (2 ** 1)
        assert delay >= expected * 0.9
        assert delay <= expected + 1.0
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)
