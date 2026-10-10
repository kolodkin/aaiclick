"""The background worker turns pending sandbox files into ``run_job`` calls."""

import asyncio
import os
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.background.background_worker import SANDBOX_STALE_AFTER, BackgroundWorker
from aaiclick.orchestration.models import SANDBOX_FAILED, SANDBOX_RUNNING, SANDBOX_SUBMITTED, SandboxFile
from aaiclick.orchestration.orch_context import get_sql_session

RUN_JOB = "aaiclick.orchestration.background.background_worker.run_job"


async def _run_step() -> None:
    worker = BackgroundWorker()
    worker._engine = create_async_engine(os.environ["AAICLICK_SQL_URL"], echo=False)
    worker._ch_client = None
    try:
        await worker._run_sandbox_files()
    finally:
        await worker._engine.dispose()


async def _insert_row(job_names: list[str]) -> SandboxFile:
    row = SandboxFile(
        name="demo", path="20261009/sb_1_demo.py", git_remote="ssh://sb", git_sha="abc", job_names=job_names
    )
    async with get_sql_session() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return row


async def _reload(row_id: int) -> SandboxFile:
    async with get_sql_session() as session:
        row = await session.get(SandboxFile, row_id)
    assert row is not None
    return row


async def test_pending_row_runs_every_job(orch_ctx):
    row = await _insert_row(["first", "second"])
    calls = []

    async def fake_run_job(name, entrypoint, **kw):
        calls.append((name, entrypoint, kw))
        return SimpleNamespace(id=100 + len(calls))

    with patch(RUN_JOB, fake_run_job):
        await _run_step()

    reloaded = await _reload(row.id)
    assert reloaded.status == SANDBOX_SUBMITTED and reloaded.job_ids == [101, 102]
    assert calls[0][0] == "sb_1_demo.first" and calls[0][1] == "20261009.sb_1_demo.first"
    assert calls[0][2] == {"git_remote": "ssh://sb", "git_sha": "abc", "run_type": "SANDBOX"}
    assert calls[1][0] == "sb_1_demo.second"


async def test_failure_on_second_job_keeps_first_id(orch_ctx):
    row = await _insert_row(["first", "second", "third"])

    async def fake_run_job(name, entrypoint, **kw):
        if name.endswith(".second"):
            raise ValueError("boom")
        return SimpleNamespace(id=7)

    with patch(RUN_JOB, fake_run_job):
        await _run_step()

    reloaded = await _reload(row.id)
    assert reloaded.status == SANDBOX_FAILED and reloaded.job_ids == [7] and reloaded.error == "second: boom"


async def test_concurrent_steps_submit_each_job_once(orch_ctx):
    """Two background workers polling at once (or one poll overlapping a
    manual call) must not both turn the same pending row into jobs."""
    row = await _insert_row(["first", "second"])
    calls = []

    async def slow_run_job(name, entrypoint, **kw):
        calls.append(name)
        await asyncio.sleep(0.05)
        return SimpleNamespace(id=len(calls))

    with patch(RUN_JOB, slow_run_job):
        await asyncio.gather(_run_step(), _run_step())

    reloaded = await _reload(row.id)
    assert sorted(calls) == ["sb_1_demo.first", "sb_1_demo.second"]
    assert reloaded.status == SANDBOX_SUBMITTED and reloaded.job_ids == [1, 2]


async def test_row_is_running_while_its_jobs_are_created(orch_ctx):
    """Readers must never see the final status before the jobs exist."""
    row = await _insert_row(["first"])
    seen = []

    async def peeking_run_job(name, entrypoint, **kw):
        seen.append((await _reload(row.id)).status)
        return SimpleNamespace(id=1)

    with patch(RUN_JOB, peeking_run_job):
        await _run_step()

    assert seen == [SANDBOX_RUNNING]
    assert (await _reload(row.id)).status == SANDBOX_SUBMITTED


async def test_stale_running_row_is_failed_not_rerun(orch_ctx):
    """A row a crashed worker claimed but never finished turns failed."""
    row = await _insert_row(["first"])
    async with get_sql_session() as session:
        stuck = await session.get(SandboxFile, row.id)
        assert stuck is not None
        stuck.status = SANDBOX_RUNNING
        stuck.updated_at = utc_now() - SANDBOX_STALE_AFTER - timedelta(seconds=1)
        session.add(stuck)
        await session.commit()
    calls = []

    async def recording_run_job(name, entrypoint, **kw):
        calls.append(name)
        return SimpleNamespace(id=1)

    with patch(RUN_JOB, recording_run_job):
        await _run_step()

    reloaded = await _reload(row.id)
    assert calls == []
    assert reloaded.status == SANDBOX_FAILED and reloaded.error == "interrupted before its jobs were created"
