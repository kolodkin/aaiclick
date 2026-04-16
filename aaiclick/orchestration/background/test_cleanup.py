"""Tests for background worker cleanup logic.

Verifies that _cleanup_unreferenced_tables respects table_pin_refs
and table_run_refs for table protection.
Also tests clean_task_run for crash recovery.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.oplog.cleanup import TableOwner
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.handler import BackgroundHandler
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler

from .conftest import get_run_refs, insert_context_ref, insert_pin_ref, insert_run_ref


async def _get_context_tables(engine):
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))
        return {row[0] for row in result.fetchall()}


async def test_cleanup_skips_pinned_tables(bg_db):
    """Tables with a pin_ref are NOT dropped even when no run refs exist."""
    await insert_context_ref(bg_db, "t_unpinned", 100)
    await insert_context_ref(bg_db, "t_pinned", 200)
    await insert_pin_ref(bg_db, "t_pinned", 300)

    worker = BackgroundWorker()
    worker._engine = bg_db
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()

    await worker._cleanup_unreferenced_tables()

    remaining = await _get_context_tables(bg_db)
    assert "t_pinned" in remaining, "Pinned table was dropped"
    assert "t_unpinned" not in remaining, "Unpinned table was not cleaned up"


async def test_cleanup_skips_tables_with_active_runs(bg_db):
    """Tables with run refs in table_run_refs are NOT dropped."""
    await insert_context_ref(bg_db, "t_active", 100)
    await insert_run_ref(bg_db, "t_active", "run_1")
    await insert_context_ref(bg_db, "t_empty", 200)

    worker = BackgroundWorker()
    worker._engine = bg_db
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()

    await worker._cleanup_unreferenced_tables()

    remaining = await _get_context_tables(bg_db)
    assert "t_active" in remaining, "Active table was dropped"
    assert "t_empty" not in remaining, "Empty table was not cleaned up"


async def test_clean_task_run_removes_run_refs(bg_db):
    """clean_task_run deletes all table_run_refs rows for that run_id."""
    await insert_run_ref(bg_db, "t1", "run_1")
    await insert_run_ref(bg_db, "t1", "run_2")
    await insert_run_ref(bg_db, "t2", "run_1")
    await insert_run_ref(bg_db, "t3", "run_3")

    async with AsyncSession(bg_db) as session:
        await BackgroundHandler.clean_task_run(session, "run_1")
        await session.commit()

    assert await get_run_refs(bg_db, "t1") == {"run_2"}
    assert await get_run_refs(bg_db, "t2") == set()
    assert await get_run_refs(bg_db, "t3") == {"run_3"}


async def test_clean_task_runs_batch_removes_multiple_run_ids(bg_db):
    """clean_task_runs batch-deletes table_run_refs rows for multiple run_ids."""
    await insert_run_ref(bg_db, "t1", "run_1")
    await insert_run_ref(bg_db, "t1", "run_2")
    await insert_run_ref(bg_db, "t2", "run_2")
    await insert_run_ref(bg_db, "t3", "run_3")

    handler = SqliteBackgroundHandler()
    async with AsyncSession(bg_db) as session:
        await handler.clean_task_runs(session, ["run_1", "run_2"])
        await session.commit()

    assert await get_run_refs(bg_db, "t1") == set()
    assert await get_run_refs(bg_db, "t2") == set()
    assert await get_run_refs(bg_db, "t3") == {"run_3"}


async def test_clean_task_run_then_cleanup_drops_table(bg_db):
    """After clean_task_run removes run refs, cleanup drops the table."""
    await insert_context_ref(bg_db, "t_orphan", 100)
    await insert_run_ref(bg_db, "t_orphan", "crashed_run")

    async with AsyncSession(bg_db) as session:
        await BackgroundHandler.clean_task_run(session, "crashed_run")
        await session.commit()

    worker = BackgroundWorker()
    worker._engine = bg_db
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()

    await worker._cleanup_unreferenced_tables()

    remaining = await _get_context_tables(bg_db)
    assert "t_orphan" not in remaining, "Orphaned table was not cleaned up"


async def _insert_job(engine, job_id: int, mode: str) -> None:
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO jobs (id, name, status, run_type, preservation_mode, created_at) "
                "VALUES (:id, :name, 'PENDING', 'MANUAL', :mode, :created_at)"
            ),
            {
                "id": job_id,
                "name": f"j{job_id}",
                "mode": mode,
                "created_at": datetime.utcnow(),
            },
        )
        await session.commit()


async def test_cleanup_full_mode_skips_drop(bg_db):
    """Tables belonging to a FULL-mode job are preserved by cleanup."""
    await _insert_job(bg_db, 777, "FULL")
    await insert_context_ref(bg_db, "t_full", 100)

    worker = BackgroundWorker()
    worker._engine = bg_db
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    # Stub the owner lookup: table belongs to job 777.
    worker._lookup_table_owners = AsyncMock(
        return_value={"t_full": TableOwner(job_id=777)},
    )

    await worker._cleanup_unreferenced_tables()

    # Table still present, no drop was attempted on CH.
    remaining = await _get_context_tables(bg_db)
    assert "t_full" in remaining
    worker._ch_client.command.assert_not_called()


async def test_cleanup_none_mode_drops(bg_db):
    """Tables belonging to a NONE-mode job are dropped as normal."""
    await _insert_job(bg_db, 888, "NONE")
    await insert_context_ref(bg_db, "t_none", 100)

    worker = BackgroundWorker()
    worker._engine = bg_db
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    worker._lookup_table_owners = AsyncMock(
        return_value={"t_none": TableOwner(job_id=888)},
    )

    await worker._cleanup_unreferenced_tables()

    remaining = await _get_context_tables(bg_db)
    assert "t_none" not in remaining
