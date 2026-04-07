"""Tests for background worker cleanup logic.

Verifies that _cleanup_unreferenced_tables respects the job_id pin guard:
tables with non-NULL job_id are skipped even when refcount <= 0.
"""

from __future__ import annotations

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


async def _insert_ref(engine, table_name, context_id, refcount, job_id=None):
    """Insert a row into table_context_refs."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_context_refs (table_name, context_id, refcount, job_id) "
                "VALUES (:t, :c, :r, :j)"
            ),
            {"t": table_name, "c": context_id, "r": refcount, "j": job_id},
        )
        await session.commit()


async def _get_tables(engine):
    """Return set of table_names still in table_context_refs."""
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))
        return {row[0] for row in result.fetchall()}


async def test_cleanup_skips_pinned_tables():
    """Tables with job_id set (pinned) are NOT dropped even if refcount <= 0."""
    engine, tmpdir = await _setup_db()
    try:
        await _insert_ref(engine, "t_unpinned", 100, 0)
        await _insert_ref(engine, "t_pinned", 200, 0, job_id=999)

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
