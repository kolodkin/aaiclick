"""Tests for background worker cleanup logic.

Verifies that _cleanup_unreferenced_tables respects table_pin_refs
and table_run_refs for table protection.
Also tests clean_task_run for crash recovery.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from unittest.mock import AsyncMock

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.handler import BackgroundHandler
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.models import SQLModel


async def _setup_db():
    """Create a temp SQLite DB with schema, return (async_engine, tmpdir)."""
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_bgtest_")
    db_path = os.path.join(tmpdir, "test.db")
    sync_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(sync_engine)
    sync_engine.dispose()
    async_engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    return async_engine, tmpdir


async def _insert_context_ref(engine, table_name, context_id):
    """Insert a row into table_context_refs."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_context_refs (table_name, context_id) "
                "VALUES (:t, :c)"
            ),
            {"t": table_name, "c": context_id},
        )
        await session.commit()


async def _insert_pin_ref(engine, table_name, job_id):
    """Insert a row into table_pin_refs (pin protection)."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_pin_refs (table_name, job_id) "
                "VALUES (:t, :j)"
            ),
            {"t": table_name, "j": job_id},
        )
        await session.commit()


async def _insert_job(engine, job_id, status="RUNNING"):
    """Insert a job row for pin ref tests."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO jobs (id, name, status, run_type, created_at) "
                "VALUES (:id, :name, :status, 'MANUAL', CURRENT_TIMESTAMP)"
            ),
            {"id": job_id, "name": f"test_job_{job_id}", "status": status},
        )
        await session.commit()


async def _insert_run_ref(engine, table_name, run_id):
    """Insert a row into table_run_refs (run-level reference)."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_run_refs (table_name, run_id) "
                "VALUES (:t, :r)"
            ),
            {"t": table_name, "r": run_id},
        )
        await session.commit()


async def _get_context_tables(engine):
    """Return set of table_names still in table_context_refs."""
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))
        return {row[0] for row in result.fetchall()}


async def _get_run_refs(engine, table_name):
    """Return set of run_ids for a table in table_run_refs."""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT run_id FROM table_run_refs WHERE table_name = :t"),
            {"t": table_name},
        )
        return {row[0] for row in result.fetchall()}


async def test_cleanup_skips_pinned_tables():
    """Tables with a pin_ref from an active job are NOT dropped."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_job(engine, 999, status="RUNNING")
        await _insert_context_ref(engine, "t_unpinned", 100)
        await _insert_context_ref(engine, "t_pinned", 200)
        await _insert_pin_ref(engine, "t_pinned", 999)

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._cleanup_unreferenced_tables()

        remaining = await _get_context_tables(engine)
        assert "t_pinned" in remaining, "Pinned table was dropped"
        assert "t_unpinned" not in remaining, "Unpinned table was not cleaned up"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_cleanup_skips_tables_with_active_runs():
    """Tables with run refs in table_run_refs are NOT dropped."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_context_ref(engine, "t_active", 100)
        await _insert_run_ref(engine, "t_active", "run_1")
        await _insert_context_ref(engine, "t_empty", 200)

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._cleanup_unreferenced_tables()

        remaining = await _get_context_tables(engine)
        assert "t_active" in remaining, "Active table was dropped"
        assert "t_empty" not in remaining, "Empty table was not cleaned up"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_clean_task_run_removes_run_refs():
    """clean_task_run deletes all table_run_refs rows for that run_id."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_run_ref(engine, "t1", "run_1")
        await _insert_run_ref(engine, "t1", "run_2")
        await _insert_run_ref(engine, "t2", "run_1")
        await _insert_run_ref(engine, "t3", "run_3")

        async with AsyncSession(engine) as session:
            await BackgroundHandler.clean_task_run(session, "run_1")
            await session.commit()

        assert await _get_run_refs(engine, "t1") == {"run_2"}
        assert await _get_run_refs(engine, "t2") == set()
        assert await _get_run_refs(engine, "t3") == {"run_3"}
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_clean_task_runs_batch_removes_multiple_run_ids():
    """clean_task_runs batch-deletes table_run_refs rows for multiple run_ids."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_run_ref(engine, "t1", "run_1")
        await _insert_run_ref(engine, "t1", "run_2")
        await _insert_run_ref(engine, "t2", "run_2")
        await _insert_run_ref(engine, "t3", "run_3")

        handler = SqliteBackgroundHandler()
        async with AsyncSession(engine) as session:
            await handler.clean_task_runs(session, ["run_1", "run_2"])
            await session.commit()

        assert await _get_run_refs(engine, "t1") == set()
        assert await _get_run_refs(engine, "t2") == set()
        assert await _get_run_refs(engine, "t3") == {"run_3"}
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_clean_task_run_then_cleanup_drops_table():
    """After clean_task_run removes run refs, cleanup drops the table."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_context_ref(engine, "t_orphan", 100)
        await _insert_run_ref(engine, "t_orphan", "crashed_run")

        async with AsyncSession(engine) as session:
            await BackgroundHandler.clean_task_run(session, "crashed_run")
            await session.commit()

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._cleanup_unreferenced_tables()

        remaining = await _get_context_tables(engine)
        assert "t_orphan" not in remaining, "Orphaned table was not cleaned up"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)
