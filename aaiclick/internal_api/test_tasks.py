"""Tests for ``aaiclick.internal_api.tasks``."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import TaskDetail

from . import errors, tasks

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def test_get_task_returns_detail(orch_ctx):
    job = await create_job("task_detail_job", _SAMPLE_TASK)
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()

    detail = await tasks.get_task(task.id)

    assert isinstance(detail, TaskDetail)
    assert detail.id == task.id
    assert detail.job_id == job.id
    assert detail.entrypoint == _SAMPLE_TASK


async def test_get_task_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await tasks.get_task(999_999_999)
