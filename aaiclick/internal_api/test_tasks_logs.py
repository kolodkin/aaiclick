from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.internal_api.errors import NotFound
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session


async def _set_log_path(task_id: int, path: str | None) -> None:
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.id == task_id))).scalar_one()
        task.log_path = path
        s.add(task)
        await s.commit()


async def test_logs_unavailable_when_log_path_none(orch_ctx):
    job = await create_job("logs_none", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_read_from_file(orch_ctx, tmp_path):
    job = await create_job("logs_file", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    log_file = tmp_path / "task.log"
    log_file.write_text("line one\nline two\n")
    await _set_log_path(task.id, str(log_file))

    result = await get_task_logs(task.id)

    assert result.available is True
    assert result.log_path == str(log_file)
    assert result.lines == ["line one", "line two"]


async def test_logs_unavailable_when_file_missing(orch_ctx, tmp_path):
    job = await create_job("logs_missing", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await _set_log_path(task.id, str(tmp_path / "does_not_exist.log"))

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_not_found_raises(orch_ctx):
    with pytest.raises(NotFound):
        await get_task_logs(999999999)
