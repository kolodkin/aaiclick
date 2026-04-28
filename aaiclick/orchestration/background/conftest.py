"""Shared test fixtures for background worker tests."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.orchestration.models import SQLModel
from aaiclick.snowflake import get_snowflake_id


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


async def insert_job(engine, job_id, *, status="RUNNING", preserve=None):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO jobs (id, name, status, run_type, preserve, created_at) "
                "VALUES (:id, 'test_job', :status, 'MANUAL', :preserve, :now)"
            ),
            {
                "id": job_id,
                "status": status,
                "preserve": json.dumps(preserve) if preserve is not None else None,
                "now": datetime.utcnow(),
            },
        )
        await session.commit()


async def insert_task(engine, task_id, *, job_id, status="RUNNING"):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO tasks "
                "(id, job_id, entrypoint, name, kwargs, status, created_at, max_retries, attempt, run_ids, run_statuses) "
                "VALUES (:id, :job_id, 'm.f', 't', '{}', :status, :now, 0, 0, '[]', '[]')"
            ),
            {
                "id": task_id,
                "job_id": job_id,
                "status": status,
                "now": datetime.utcnow(),
            },
        )
        await session.commit()


async def insert_task_name_lock(engine, *, job_id, name, task_id):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO task_name_locks (job_id, name, task_id, acquired_at) "
                "VALUES (:job_id, :name, :task_id, :now)"
            ),
            {"job_id": job_id, "name": name, "task_id": task_id, "now": datetime.utcnow()},
        )
        await session.commit()


async def insert_context_ref(engine, table_name, context_id, advisory_id=None):
    # Auto-mint so separate tables never silently share a lock key in tests.
    if advisory_id is None:
        advisory_id = get_snowflake_id()
    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO table_context_refs (table_name, context_id, advisory_id) VALUES (:t, :c, :a)"),
            {"t": table_name, "c": context_id, "a": advisory_id},
        )
        await session.commit()


async def insert_pin_ref(engine, table_name, task_id):
    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO table_pin_refs (table_name, task_id) VALUES (:t, :tid)"),
            {"t": table_name, "tid": task_id},
        )
        await session.commit()


async def insert_run_ref(engine, table_name, run_id):
    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO table_run_refs (table_name, run_id) VALUES (:t, :r)"),
            {"t": table_name, "r": run_id},
        )
        await session.commit()


async def insert_table_registry(engine, table_name, job_id=None, task_id=None, run_id=None, schema_doc=None):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO table_registry (table_name, job_id, task_id, run_id, created_at, schema_doc) "
                "VALUES (:tn, :jid, :tid, :rid, :now, :sd)"
            ),
            {
                "tn": table_name,
                "jid": job_id,
                "tid": task_id,
                "rid": run_id,
                "now": datetime.utcnow(),
                "sd": schema_doc,
            },
        )
        await session.commit()


async def get_run_refs(engine, table_name):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT run_id FROM table_run_refs WHERE table_name = :t"),
            {"t": table_name},
        )
        return {row[0] for row in result.fetchall()}
