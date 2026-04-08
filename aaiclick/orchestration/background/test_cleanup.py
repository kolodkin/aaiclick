"""Tests for background worker cleanup logic.

Verifies that _cleanup_unreferenced_tables respects the job_id pin guard:
tables with non-NULL job_id are skipped even when run_ids is empty.
Also tests clean_task_run for crash recovery.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from unittest.mock import AsyncMock

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.background.background_worker import BackgroundWorker
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


async def _insert_ref(engine, table_name, context_id, run_ids, job_id=None):
    """Insert a row into table_context_refs."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_context_refs (table_name, context_id, run_ids, job_id) "
                "VALUES (:t, :c, :r, :j)"
            ),
            {"t": table_name, "c": context_id, "r": json.dumps(run_ids), "j": job_id},
        )
        await session.commit()


async def _get_tables(engine):
    """Return set of table_names still in table_context_refs."""
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))
        return {row[0] for row in result.fetchall()}


async def _get_run_ids(engine, table_name, context_id):
    """Return the run_ids array for a specific row."""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT run_ids FROM table_context_refs WHERE table_name = :t AND context_id = :c"),
            {"t": table_name, "c": context_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        val = row[0]
        return val if isinstance(val, list) else json.loads(val)


async def test_cleanup_skips_pinned_tables():
    """Tables with job_id set (pinned) are NOT dropped even when run_ids is empty."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_ref(engine, "t_unpinned", 100, [])
        await _insert_ref(engine, "t_pinned", 200, [], job_id=999)

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._cleanup_unreferenced_tables()

        remaining = await _get_tables(engine)
        assert "t_pinned" in remaining, "Pinned table was dropped"
        assert "t_unpinned" not in remaining, "Unpinned table was not cleaned up"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_cleanup_skips_tables_with_active_runs():
    """Tables with non-empty run_ids are NOT dropped."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_ref(engine, "t_active", 100, ["run_1"])
        await _insert_ref(engine, "t_empty", 200, [])

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = SqliteBackgroundHandler()
        worker._ch_client = AsyncMock()

        await worker._cleanup_unreferenced_tables()

        remaining = await _get_tables(engine)
        assert "t_active" in remaining, "Active table was dropped"
        assert "t_empty" not in remaining, "Empty table was not cleaned up"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_clean_task_run_removes_run_id():
    """clean_task_run removes the specified run_id from all rows."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_ref(engine, "t1", 100, ["run_1", "run_2"])
        await _insert_ref(engine, "t2", 200, ["run_1"])
        await _insert_ref(engine, "t3", 300, ["run_3"])

        handler = SqliteBackgroundHandler()
        async with AsyncSession(engine) as session:
            await handler.clean_task_run(session, "run_1")
            await session.commit()

        assert await _get_run_ids(engine, "t1", 100) == ["run_2"]
        assert await _get_run_ids(engine, "t2", 200) == []
        assert await _get_run_ids(engine, "t3", 300) == ["run_3"]
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)


async def test_clean_task_run_then_cleanup_drops_table():
    """After clean_task_run empties run_ids, cleanup drops the table."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_ref(engine, "t_orphan", 100, ["crashed_run"])

        handler = SqliteBackgroundHandler()
        async with AsyncSession(engine) as session:
            await handler.clean_task_run(session, "crashed_run")
            await session.commit()

        worker = BackgroundWorker()
        worker._engine = engine
        worker._handler = handler
        worker._ch_client = AsyncMock()

        await worker._cleanup_unreferenced_tables()

        remaining = await _get_tables(engine)
        assert "t_orphan" not in remaining, "Orphaned table was not cleaned up"
    finally:
        await engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)
