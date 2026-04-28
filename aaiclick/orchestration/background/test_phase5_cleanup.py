"""Tests for BackgroundWorker cleanup methods (Phase 5 + Phase 8 simplification).

- ``_cleanup_at_job_completion``: drops every CH table tied to a completed job.
- ``_cleanup_failed_task_tables``: per-task drop of unpinned ``t_*`` scratch.
- ``_cleanup_orphan_scratch_tables``: drops unpinned ``t_*`` whose owner is dead.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler

from .conftest import insert_job, insert_pin_ref, insert_table_registry, insert_task


def _worker(engine) -> BackgroundWorker:
    worker = BackgroundWorker()
    worker._engine = engine
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    return worker


async def _ch_drop_calls(worker: BackgroundWorker) -> set[str]:
    return {
        call.args[0].split("IF EXISTS ", 1)[1]
        for call in worker._ch_client.command.call_args_list
        if "DROP TABLE" in call.args[0]
    }


async def _registry_tables(engine) -> set[str]:
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT table_name FROM table_registry"))
        return {row[0] for row in result.fetchall()}


# _cleanup_at_job_completion


async def test_completion_drops_all_job_tables(bg_db):
    job_id = 100
    await insert_job(bg_db, job_id)
    named = f"j_{job_id}_training_set"
    pinned = "t_pinned"
    other_job_table = "j_999_other"

    await insert_table_registry(bg_db, named, job_id=job_id, task_id=1)
    await insert_table_registry(bg_db, pinned, job_id=job_id, task_id=1)
    await insert_table_registry(bg_db, other_job_table, job_id=999, task_id=2)
    await insert_pin_ref(bg_db, pinned, 50)

    worker = _worker(bg_db)
    await worker._cleanup_at_job_completion(job_id=job_id)

    dropped = await _ch_drop_calls(worker)
    assert named in dropped
    assert pinned in dropped
    assert other_job_table not in dropped

    remaining = await _registry_tables(bg_db)
    assert named not in remaining and pinned not in remaining
    assert other_job_table in remaining


async def test_completion_skips_global_p_tables(bg_db):
    job_id = 100
    await insert_job(bg_db, job_id)
    await insert_table_registry(bg_db, "p_user_global", job_id=job_id, task_id=1)
    await insert_table_registry(bg_db, f"j_{job_id}_x", job_id=job_id, task_id=1)

    worker = _worker(bg_db)
    await worker._cleanup_at_job_completion(job_id=job_id)

    dropped = await _ch_drop_calls(worker)
    assert "p_user_global" not in dropped
    assert f"j_{job_id}_x" in dropped


# _cleanup_failed_task_tables


async def test_failed_task_drops_t_scratch(bg_db):
    job_id = 200
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=10, job_id=job_id, status="PENDING_CLEANUP")

    scratch = "t_failed_scratch"
    await insert_table_registry(bg_db, scratch, job_id=job_id, task_id=10)

    worker = _worker(bg_db)
    await worker._cleanup_failed_task_tables()

    dropped = await _ch_drop_calls(worker)
    assert scratch in dropped


async def test_failed_task_keeps_named_j_tables(bg_db):
    """Named j_<id>_* tables are job-scoped — survive task failure, drop at job end."""
    job_id = 200
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=10, job_id=job_id, status="PENDING_CLEANUP")

    named = f"j_{job_id}_keep"
    await insert_table_registry(bg_db, named, job_id=job_id, task_id=10)

    worker = _worker(bg_db)
    await worker._cleanup_failed_task_tables()

    assert named not in await _ch_drop_calls(worker)
    assert named in await _registry_tables(bg_db)


async def test_failed_task_skips_pinned(bg_db):
    job_id = 201
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=11, job_id=job_id, status="PENDING_CLEANUP")

    pinned_t = "t_pinned_consumer"
    await insert_table_registry(bg_db, pinned_t, job_id=job_id, task_id=11)
    await insert_pin_ref(bg_db, pinned_t, 99)

    worker = _worker(bg_db)
    await worker._cleanup_failed_task_tables()

    assert pinned_t not in await _ch_drop_calls(worker)
    assert pinned_t in await _registry_tables(bg_db)


async def test_failed_task_no_pending_cleanup_is_noop(bg_db):
    worker = _worker(bg_db)
    await worker._cleanup_failed_task_tables()
    worker._ch_client.command.assert_not_called()


# _cleanup_orphan_scratch_tables


async def test_orphan_scratch_dropped(bg_db):
    job_id = 300
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=20, job_id=job_id, status="FAILED")

    name = "t_orphan_scratch"
    await insert_table_registry(bg_db, name, job_id=job_id, task_id=20)

    worker = _worker(bg_db)
    await worker._cleanup_orphan_scratch_tables()

    assert name in await _ch_drop_calls(worker)


async def test_orphan_scratch_skips_pinned(bg_db):
    job_id = 301
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=21, job_id=job_id, status="FAILED")

    name = "t_orphan_pinned"
    await insert_table_registry(bg_db, name, job_id=job_id, task_id=21)
    await insert_pin_ref(bg_db, name, 999)

    worker = _worker(bg_db)
    await worker._cleanup_orphan_scratch_tables()

    assert name not in await _ch_drop_calls(worker)


async def test_orphan_scratch_skips_live_owner(bg_db):
    job_id = 302
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=22, job_id=job_id, status="RUNNING")

    name = "t_alive_scratch"
    await insert_table_registry(bg_db, name, job_id=job_id, task_id=22)

    worker = _worker(bg_db)
    await worker._cleanup_orphan_scratch_tables()

    assert name not in await _ch_drop_calls(worker)


async def test_orphan_scratch_skips_named_j_tables(bg_db):
    job_id = 303
    await insert_job(bg_db, job_id)
    await insert_task(bg_db, task_id=23, job_id=job_id, status="FAILED")

    name = f"j_{job_id}_named"
    await insert_table_registry(bg_db, name, job_id=job_id, task_id=23)

    worker = _worker(bg_db)
    await worker._cleanup_orphan_scratch_tables()

    assert name not in await _ch_drop_calls(worker)
