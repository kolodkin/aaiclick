"""Tests for PENDING_FAILURE_CLEANUP background processing.

Verifies that the background worker correctly:
1. Cleans run_refs for failed tasks, pin_refs once retries are exhausted
2. Transitions PENDING_FAILURE_CLEANUP → PENDING (retries remaining) or FAILED (exhausted)
3. Checks job completion after marking tasks FAILED
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.orchestration.background.background_worker import RETRY_BASE_DELAY, BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.env import get_db_url
from aaiclick.orchestration.sql_context import create_sql_engine

from ...datetime_utils import utc_now
from .conftest import (
    WORKER_ID,
    get_run_refs,
    insert_job,
    insert_pin_ref,
    insert_run_ref,
    insert_task,
    insert_worker,
    make_worker,
)


async def _get_task_status(engine, task_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                "SELECT status, attempt, retry_after, error, execution_worker_id, completed_at FROM tasks WHERE id = :id"
            ),
            {"id": task_id},
        )
        row = result.fetchone()
        assert row is not None
        return row


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


async def run_cleanup_pass(pass_fn: Callable[[BackgroundWorker], Awaitable[None]]) -> None:
    """Run one BackgroundWorker pass against the current orch_context DB.

    Test helper that creates a temporary BackgroundWorker pointed at the
    same SQL database, e.g. ``run_cleanup_pass(BackgroundWorker._process_failure_cleanup)``.
    """
    worker = BackgroundWorker(poll_interval=0)
    worker._engine = create_sql_engine(get_db_url())
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    try:
        await pass_fn(worker)
    finally:
        await worker._engine.dispose()


async def run_failure_cleanup() -> None:
    await run_cleanup_pass(BackgroundWorker._process_failure_cleanup)


async def test_failure_cleanup_transitions_to_pending_with_retries(bg_db):
    """PENDING_FAILURE_CLEANUP task with retries → PENDING with incremented attempt."""
    await insert_job(bg_db, 1000)
    await insert_worker(bg_db, WORKER_ID)
    await insert_task(
        bg_db,
        100,
        1000,
        status="PENDING_FAILURE_CLEANUP",
        attempt=0,
        max_retries=3,
        run_ids="[111]",
        error="task failed",
        execution_worker_id=WORKER_ID,
    )
    await insert_run_ref(bg_db, "t_intermediate", "111")

    await make_worker(bg_db)._process_failure_cleanup()

    row = await _get_task_status(bg_db, 100)
    status, attempt, retry_after, error, execution_worker_id, completed_at = row
    assert status == "PENDING"
    assert attempt == 1
    assert retry_after is not None
    assert execution_worker_id is None

    assert await get_run_refs(bg_db, "t_intermediate") == set()


async def test_failure_cleanup_transitions_to_failed_no_retries(bg_db):
    """PENDING_FAILURE_CLEANUP task with no retries → FAILED."""
    await insert_job(bg_db, 1000)
    await insert_task(
        bg_db,
        100,
        1000,
        status="PENDING_FAILURE_CLEANUP",
        attempt=0,
        max_retries=0,
        run_ids="[111]",
        error="task failed",
    )
    await insert_run_ref(bg_db, "t_table", "111")

    await make_worker(bg_db)._process_failure_cleanup()

    row = await _get_task_status(bg_db, 100)
    status, attempt, retry_after, error, execution_worker_id, completed_at = row
    assert status == "FAILED"
    assert completed_at is not None

    assert await get_run_refs(bg_db, "t_table") == set()


@pytest.mark.parametrize(
    "max_retries, expected_pins",
    [
        pytest.param(1, {"t_upstream_data", "t_other_data"}, id="retry-keeps-inputs"),
        pytest.param(0, set(), id="exhausted-releases"),
    ],
)
async def test_failure_cleanup_pin_refs(bg_db, max_retries, expected_pins):
    """A retry keeps the task's pin_refs (its inputs); settling to FAILED drops them."""
    await insert_job(bg_db, 1000)
    await insert_task(
        bg_db,
        100,
        1000,
        status="PENDING_FAILURE_CLEANUP",
        attempt=0,
        max_retries=max_retries,
        run_ids="[111]",
        error="task failed",
    )
    await insert_pin_ref(bg_db, "t_upstream_data", 100)
    await insert_pin_ref(bg_db, "t_other_data", 100)
    await insert_pin_ref(bg_db, "t_upstream_data", 200)

    await make_worker(bg_db)._process_failure_cleanup()

    assert await _get_pin_refs(bg_db, 100) == expected_pins
    assert await _get_pin_refs(bg_db, 200) == {"t_upstream_data"}


async def test_failure_cleanup_completes_job_when_all_failed(bg_db):
    """Job transitions to FAILED when last task transitions from PENDING_FAILURE_CLEANUP to FAILED."""
    await insert_job(bg_db, 1000, status="RUNNING")
    await insert_task(bg_db, 100, 1000, status="COMPLETED")
    await insert_task(
        bg_db,
        101,
        1000,
        status="PENDING_FAILURE_CLEANUP",
        attempt=0,
        max_retries=0,
        run_ids="[222]",
        error="oops",
    )

    await make_worker(bg_db)._process_failure_cleanup()

    assert await _get_job_status(bg_db, 1000) == "FAILED"


async def test_failure_cleanup_does_not_complete_job_with_retries(bg_db):
    """Job stays RUNNING when PENDING_FAILURE_CLEANUP task transitions to PENDING (has retries)."""
    await insert_job(bg_db, 1000, status="RUNNING")
    await insert_task(
        bg_db,
        100,
        1000,
        status="PENDING_FAILURE_CLEANUP",
        attempt=0,
        max_retries=2,
        run_ids="[111]",
        error="will retry",
    )

    await make_worker(bg_db)._process_failure_cleanup()

    assert await _get_job_status(bg_db, 1000) == "RUNNING"


async def test_failure_cleanup_retry_backoff(bg_db):
    """Retry backoff doubles each attempt: 1s, 2s, 4s."""
    await insert_job(bg_db, 1000)
    await insert_task(
        bg_db,
        100,
        1000,
        status="PENDING_FAILURE_CLEANUP",
        attempt=1,
        max_retries=5,
        run_ids="[111, 222]",
        error="failed again",
    )

    before = utc_now()
    await make_worker(bg_db)._process_failure_cleanup()

    row = await _get_task_status(bg_db, 100)
    status, attempt, retry_after_str, error, execution_worker_id, completed_at = row
    assert status == "PENDING"
    assert attempt == 2

    retry_after = datetime.fromisoformat(retry_after_str) if isinstance(retry_after_str, str) else retry_after_str
    delay = (retry_after - before).total_seconds()
    expected = RETRY_BASE_DELAY * (2**1)
    assert delay >= expected * 0.9
    assert delay <= expected + 1.0


async def _get_run_epoch(engine, task_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT run_epoch FROM tasks WHERE id = :id"), {"id": task_id})
        return result.scalar_one()


@pytest.mark.parametrize(
    "max_retries, expected_status",
    [
        pytest.param(3, "PENDING", id="retry"),
        pytest.param(0, "FAILED", id="exhausted"),
    ],
)
async def test_failure_cleanup_bumps_run_epoch(bg_db, max_retries, expected_status):
    """Leaving PENDING_FAILURE_CLEANUP fences the run that reported it.

    A worker declared dead by heartbeat timeout may still be alive; once its
    task is retried and re-claimed under the same epoch, its late COMPLETED
    write would pass the epoch guard and land on another worker's run.
    """
    await insert_job(bg_db, 1000)
    await insert_task(bg_db, 100, 1000, status="PENDING_FAILURE_CLEANUP", max_retries=max_retries, run_ids="[111]")
    assert await _get_run_epoch(bg_db, 100) == 0

    await make_worker(bg_db)._process_failure_cleanup()

    assert (await _get_task_status(bg_db, 100))[0] == expected_status
    assert await _get_run_epoch(bg_db, 100) == 1


async def test_dead_worker_sweep_leaves_its_running_task(bg_db):
    """Worker and task lifetimes are independent: a swept worker is STOPPED,
    but its task keeps running until its own claim expires."""
    await insert_job(bg_db, 1000)
    await insert_worker(bg_db, WORKER_ID, stale_by=timedelta(hours=1))
    await insert_task(bg_db, 100, 1000, status="RUNNING", execution_worker_id=WORKER_ID)
    worker = make_worker(bg_db)

    await worker._cleanup_dead_workers()
    await worker._release_expired_claims()

    async with AsyncSession(bg_db) as session:
        status = await session.execute(text("SELECT status FROM execution_workers WHERE id = :id"), {"id": WORKER_ID})
        assert status.scalar_one() == "STOPPED"
    status, _, _, _, owner, _ = await _get_task_status(bg_db, 100)
    assert (status, owner) == ("RUNNING", WORKER_ID)


async def test_expired_claim_is_reclaimed_and_fenced(bg_db):
    """An expired RUNNING claim goes to PENDING_FAILURE_CLEANUP with its run
    closed as FAILED and a bumped ``run_epoch``, so the lost owner's late
    writes are rejected."""
    await insert_job(bg_db, 1000)
    await insert_worker(bg_db, WORKER_ID)
    await insert_task(
        bg_db,
        100,
        1000,
        status="RUNNING",
        run_statuses='["FAILED", "RUNNING"]',
        execution_worker_id=WORKER_ID,
        claim_expired_by=timedelta(seconds=1),
    )
    await insert_task(bg_db, 101, 1000, status="RUNNING", execution_worker_id=WORKER_ID)

    await make_worker(bg_db)._release_expired_claims()

    status, _, _, error, _, _ = await _get_task_status(bg_db, 100)
    assert (status, error) == ("PENDING_FAILURE_CLEANUP", "Task claim expired (worker lost)")
    assert await _get_run_epoch(bg_db, 100) == 1
    async with AsyncSession(bg_db) as session:
        run_statuses = (await session.execute(text("SELECT run_statuses FROM tasks WHERE id = 100"))).scalar_one()
    # A list on PostgreSQL, JSON text on SQLite.
    assert (run_statuses if isinstance(run_statuses, list) else json.loads(run_statuses)) == ["FAILED", "FAILED"]
    assert (await _get_task_status(bg_db, 101))[0] == "RUNNING"
