"""Shared helpers for background worker tests.

The ``bg_db`` engine fixture lives in ``aaiclick/orchestration/conftest.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.data.data_context import ChClient
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.snowflake import get_snowflake_id

from ...datetime_utils import utc_now


def make_worker(engine, ch_client: ChClient | None = None) -> BackgroundWorker:
    """A BackgroundWorker on ``engine`` with ``ch_client``, or a mocked ClickHouse client."""
    worker = BackgroundWorker()
    worker._engine = engine
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = ch_client if ch_client is not None else AsyncMock()
    return worker


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
