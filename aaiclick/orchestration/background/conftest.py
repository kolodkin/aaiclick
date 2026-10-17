"""Shared fixtures and helpers for background worker tests.

``bg_db`` gives each test its own SQLite engine, separate from ``orch_ctx``,
for the ``BackgroundWorker`` built by ``make_worker``.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.data.object_context import ChClient
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.models import SQLModel
from aaiclick.orchestration.sql_context import create_sql_engine
from aaiclick.snowflake import get_snowflake_id

from ...datetime_utils import utc_now


@pytest.fixture
async def bg_db():
    """Yield an async engine on a temp SQLite DB with the full schema; dispose and delete it after."""
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_bgtest_")
    db_path = os.path.join(tmpdir, "test.db")
    sync_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(sync_engine)
    sync_engine.dispose()
    engine = create_sql_engine(f"sqlite+aiosqlite:///{db_path}")
    yield engine
    await engine.dispose()
    shutil.rmtree(tmpdir, ignore_errors=True)


def make_worker(engine, ch_client: ChClient | None = None) -> BackgroundWorker:
    """A BackgroundWorker on ``engine`` with ``ch_client``, or a mocked ClickHouse client."""
    worker = BackgroundWorker()
    worker._engine = engine
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = ch_client if ch_client is not None else AsyncMock()
    return worker


def mock_ch(worker: BackgroundWorker) -> AsyncMock:
    """The mocked ClickHouse client ``make_worker`` gave ``worker``."""
    assert isinstance(worker._ch_client, AsyncMock)
    return worker._ch_client


async def insert_job(engine, job_id, *, status="RUNNING", preservation_mode="NONE"):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO jobs (id, name, status, run_type, preservation_mode, created_at) "
                "VALUES (:id, 'test_job', :status, 'MANUAL', :mode, :now)"
            ),
            {"id": job_id, "status": status, "mode": preservation_mode, "now": utc_now()},
        )
        await session.commit()


WORKER_ID = 999


async def insert_task(
    engine, task_id, job_id, *, status, attempt=0, max_retries=0, run_ids="[]", error=None, execution_worker_id=None
):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, status, "
                "created_at, max_retries, attempt, run_ids, run_statuses, error, execution_worker_id) "
                "VALUES (:id, :job_id, 'test.func', 'test', '{}', :status, "
                ":now, :max_retries, :attempt, :run_ids, '[]', :error, :execution_worker_id)"
            ),
            {
                "id": task_id,
                "job_id": job_id,
                "status": status,
                "now": utc_now(),
                "max_retries": max_retries,
                "attempt": attempt,
                "run_ids": run_ids,
                "error": error,
                "execution_worker_id": execution_worker_id,
            },
        )
        await session.commit()


async def insert_worker(engine, worker_id, *, stale_by=timedelta(0)):
    """Insert an ACTIVE execution worker whose heartbeat is ``stale_by`` old."""
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO execution_workers (id, hostname, pid, status, created_at, started_at, last_heartbeat, "
                "tasks_completed, tasks_failed) "
                "VALUES (:id, 'host', 1, 'ACTIVE', :now, :now, :heartbeat, 0, 0)"
            ),
            {"id": worker_id, "now": utc_now(), "heartbeat": utc_now() - stale_by},
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
                "now": utc_now(),
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
