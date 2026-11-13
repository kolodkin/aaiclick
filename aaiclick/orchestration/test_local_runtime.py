"""Tests for local_runtime() — the lifespan-shared worker helper.

End-to-end coverage (worker startup, job completion, shutdown
cancellation) lives in aaiclick/server/test_app.py — that file's
app_client fixture already enters the FastAPI lifespan, which is the
helper's only production caller.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import aiosqlite.core
import pytest
from sqlalchemy import func, select, update
from sqlmodel import col

from aaiclick.backend import is_local
from aaiclick.datetime_utils import utc_now
from aaiclick.testing import reset_test_state, until

from . import local_runtime as lr
from . import sql_context
from .factories import create_job, create_task
from .jobs.queries import get_tasks_for_job
from .models import RUN_SCHEDULED, TASK_PENDING, Job, RegisteredJob
from .orch_context import get_sql_session, orch_context
from .registered_jobs import register_job

SIMPLE = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


class _StopEarly(Exception):
    """Ends the helper once the setup decision under test has been made."""


class _RaisingWorker:
    async def start(self) -> None:
        raise _StopEarly


async def _idle_forever(*, stop: asyncio.Event, **_kwargs) -> None:
    await stop.wait()


def _set_up_local_db(monkeypatch) -> None:
    """Local mode with a set-up, current DB; the execution worker idles."""
    monkeypatch.setattr(lr, "is_local", lambda: True)
    monkeypatch.setattr(lr, "is_setup_done", lambda: True)
    monkeypatch.setattr(lr, "missing_local_tables", lambda: [])
    monkeypatch.setattr(lr, "stale_local_db_reason", lambda: None)
    monkeypatch.setattr(lr, "execution_worker_main_loop", _idle_forever)


async def test_local_runtime_rejects_distributed_mode(monkeypatch):
    """Outside local mode the helper raises before touching any resource."""
    monkeypatch.setattr(lr, "is_local", lambda: False)
    with pytest.raises(RuntimeError, match="requires local mode"):
        async with lr.local_runtime():
            pass


async def test_local_runtime_rejects_a_stale_local_db(monkeypatch):
    """The ``setup_done`` marker carries no schema version, so an upgrade over
    an existing install would skip ``setup()`` and start the workers against a
    database that is behind the models. The helper refuses instead."""
    monkeypatch.setattr(lr, "is_local", lambda: True)
    monkeypatch.setattr(lr, "is_setup_done", lambda: True)
    monkeypatch.setattr(lr, "stale_local_db_reason", lambda: "stale: jobs.error")

    with pytest.raises(RuntimeError, match="--force"):
        async with lr.local_runtime():
            pass


async def test_local_runtime_rebuilds_when_the_marker_outlives_the_schema(monkeypatch):
    """A ``setup_done`` marker beside a database missing its tables is not
    set up. ``stale_local_db_reason`` cannot see this — it compares columns of
    tables that exist — so the helper checks for missing tables and re-runs
    setup instead of starting workers that fail on the first query."""
    monkeypatch.setattr(lr, "is_local", lambda: True)
    monkeypatch.setattr(lr, "is_setup_done", lambda: True)
    monkeypatch.setattr(lr, "missing_local_tables", lambda: ["jobs", "tasks"])
    monkeypatch.setattr(lr, "stale_local_db_reason", lambda: None)
    called: list[bool] = []
    monkeypatch.setattr(lr, "setup", lambda: called.append(True))
    monkeypatch.setattr(lr, "render_setup_result", lambda result: None)
    monkeypatch.setattr(lr, "BackgroundWorker", _RaisingWorker)

    with pytest.raises(_StopEarly):
        async with lr.local_runtime():
            pass
    assert called == [True]


async def test_local_runtime_opens_one_sql_engine(monkeypatch):
    """The BackgroundWorker runs inside the helper's ``orch_context`` and uses
    its engine, so a local server holds one connection pool on local.db —
    a second pool raced the first for the SQLite lock at startup."""
    _set_up_local_db(monkeypatch)
    # Its first poll would run on teardown against the shared test DB.
    monkeypatch.setattr(lr.BackgroundWorker, "_do_cleanup", AsyncMock())
    spy = Mock(wraps=sql_context.create_async_engine)
    monkeypatch.setattr(sql_context, "create_async_engine", spy)

    async with lr.local_runtime():
        pass

    assert spy.call_count == 1


async def test_local_runtime_stops_the_worker_between_db_writes(monkeypatch):
    """Race window: the block ends while the worker's claim has written but
    not committed. A cancel landing inside one of its driver calls makes
    SQLAlchemy abandon the connection without a rollback, which strands
    SQLite's write lock: the worker's own STOPPED write then fails with
    "database is locked". The worker must finish the claim, hand the task
    back and stop cleanly."""
    if not is_local():
        pytest.skip("the local runtime runs only on chdb + SQLite")
    async with reset_test_state(orch_context(), reset_sql=True):
        job = await create_job("claimed_at_shutdown", create_task(SIMPLE))
    mid_claim = asyncio.Event()
    claim_connections = []
    execute = aiosqlite.core.Connection._execute

    async def pause_mid_claim(self, fn, *args, **kwargs):
        if args and str(args[0]).lstrip().startswith("UPDATE tasks SET status = ?, execution_worker_id"):
            claim_connections.append(self)
        elif self in claim_connections and fn.__name__ == "fetchall" and not mid_claim.is_set():
            # The claim has written and is reading the task back; the commit is still to come.
            mid_claim.set()
            await asyncio.sleep(0.2)
        return await execute(self, fn, *args, **kwargs)

    monkeypatch.setattr(aiosqlite.core.Connection, "_execute", pause_mid_claim)

    async with lr.local_runtime():
        await asyncio.wait_for(mid_claim.wait(), 10)

    async with orch_context():
        (task,) = await get_tasks_for_job(job.id)
    assert task.status == TASK_PENDING and task.execution_worker_id is None


async def _scheduled_runs(registered_job_id: int) -> int:
    async with get_sql_session() as session:
        return (
            await session.execute(
                select(func.count())
                .select_from(Job)
                .where(col(Job.registered_job_id) == registered_job_id, col(Job.run_type) == RUN_SCHEDULED)
            )
        ).scalar_one()


async def test_local_runtime_creates_a_due_scheduled_run(orch_ctx, monkeypatch):
    """Local mode is one process: the BackgroundWorker polls inside the
    server's ``orch_context``, and the ``orch_context`` its schedule pass
    nests to create the run reuses that engine instead of opening one."""
    _set_up_local_db(monkeypatch)
    reg = await register_job(name="local_sched", entrypoint="myapp.local_sched", schedule="* * * * *")
    async with get_sql_session() as session:
        await session.execute(
            update(RegisteredJob)
            .where(col(RegisteredJob.id) == reg.id)
            .values(next_run_at=utc_now() - timedelta(minutes=5))
        )
        await session.commit()

    spy = Mock(wraps=sql_context.create_async_engine)
    monkeypatch.setattr(sql_context, "create_async_engine", spy)

    async with lr.local_runtime():
        assert await until(lambda: _scheduled_runs(reg.id), what="scheduled run") == 1

    assert spy.call_count == 0
