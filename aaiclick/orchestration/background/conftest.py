"""Shared test fixtures for background worker tests."""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.models import SQLModel


@pytest.fixture
async def bg_db():
    """Create a temp SQLite DB with schema, yield (async_engine, tmpdir), then cleanup."""
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_bgtest_")
    db_path = os.path.join(tmpdir, "test.db")
    sync_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(sync_engine)
    sync_engine.dispose()
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    yield engine
    await engine.dispose()
    shutil.rmtree(tmpdir, ignore_errors=True)


async def insert_context_ref(engine, table_name, context_id):
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


async def insert_pin_ref(engine, table_name, task_id):
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


async def insert_run_ref(engine, table_name, run_id):
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
