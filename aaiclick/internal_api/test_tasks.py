"""Tests for ``aaiclick.internal_api.tasks``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.view_models import TaskDetail

from . import errors, tasks

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def test_get_task_returns_detail(orch_ctx):
    job = await create_job("task_detail_job", _SAMPLE_TASK)
    task = (await get_tasks_for_job(job.id))[0]

    detail = await tasks.get_task(task.id)

    assert isinstance(detail, TaskDetail)
    assert detail.id == task.id
    assert detail.job_id == job.id
    assert detail.entrypoint == _SAMPLE_TASK


async def test_get_task_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await tasks.get_task(999_999_999)
