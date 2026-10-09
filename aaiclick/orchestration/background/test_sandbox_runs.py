"""The background worker turns pending sandbox files into ``run_job`` calls."""

import os
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import select

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.models import SANDBOX_FAILED, SANDBOX_SUBMITTED, SandboxFile
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
        return (await session.execute(select(SandboxFile).where(SandboxFile.id == row_id))).scalar_one()


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
