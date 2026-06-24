from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.internal_api.errors import NotFound
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import flush_task_logs
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session


async def _set_run_ids(task_id: int, run_ids: list[int]) -> None:
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.id == task_id))).scalar_one()
        task.run_ids = run_ids
        s.add(task)
        await s.commit()


async def test_logs_unavailable_when_task_never_ran(orch_ctx):
    job = await create_job("logs_none", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_read_from_clickhouse(orch_ctx):
    job = await create_job("logs_ch", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 42
    await flush_task_logs(task.id, job.id, run_id, ["line one", "line two"])
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    assert result.available is True
    assert result.lines == ["line one", "line two"]


async def test_logs_read_latest_run_only(orch_ctx):
    job = await create_job("logs_latest", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await flush_task_logs(task.id, job.id, 1, ["first attempt"])
    await flush_task_logs(task.id, job.id, 2, ["second attempt"])
    await _set_run_ids(task.id, [1, 2])

    result = await get_task_logs(task.id)

    assert result.available is True
    assert result.lines == ["second attempt"]


async def test_logs_unavailable_when_run_has_no_lines(orch_ctx):
    job = await create_job("logs_empty", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await _set_run_ids(task.id, [7])

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_not_found_raises(orch_ctx):
    with pytest.raises(NotFound):
        await get_task_logs(999999999)
