"""Tests for ``aaiclick.internal_api.tasks``."""

from __future__ import annotations

import pytest
from sqlmodel import col, update

from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import TASK_COMPLETED, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import ClearTaskView, TaskDetail
from aaiclick.view_models import TaskListFilter

from . import errors, tasks

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
_FAILING_TASK = "aaiclick.orchestration.fixtures.sample_tasks.failing_task"


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


async def test_clear_task_returns_view(orch_ctx):
    job = await create_job("clear_task_job", _SAMPLE_TASK)
    task = (await get_tasks_for_job(job.id))[0]

    view = await tasks.clear_task(task.id)

    assert isinstance(view, ClearTaskView)
    assert view.cleared_task_ids == [task.id]
    assert view.job.id == job.id


async def test_clear_task_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await tasks.clear_task(999_999_999)


async def _two_jobs() -> tuple[list[int], int]:
    first = await create_job("list_tasks_a", _SAMPLE_TASK)
    second = await create_job("list_tasks_b", _FAILING_TASK)
    first_task = (await get_tasks_for_job(first.id))[0]
    second_task = (await get_tasks_for_job(second.id))[0]
    return [second_task.id, first_task.id], first_task.id


async def test_list_tasks_newest_first(orch_ctx):
    newest_first, _ = await _two_jobs()

    page = await tasks.list_tasks()

    assert [t.id for t in page.items] == newest_first
    assert page.total == 2


@pytest.mark.parametrize(
    "filter_kwargs",
    [
        pytest.param({"name": "%simple%"}, id="name-pattern"),
        pytest.param({"job": "list_tasks_a"}, id="job-by-name"),
        pytest.param({"status": "COMPLETED"}, id="status"),
    ],
)
async def test_list_tasks_filters(orch_ctx, filter_kwargs):
    _, simple_id = await _two_jobs()
    async with get_sql_session() as session:
        await session.execute(update(Task).where(col(Task.id) == simple_id).values(status=TASK_COMPLETED))
        await session.commit()

    page = await tasks.list_tasks(TaskListFilter(**filter_kwargs))

    assert [t.id for t in page.items] == [simple_id]


@pytest.mark.parametrize(
    ("name", "matches_simple"),
    [
        pytest.param("%simple%task%", True, id="percent-is-wildcard"),
        pytest.param("%simple_task%", True, id="underscore-matches-itself"),
        pytest.param("%simple_tas_", False, id="underscore-is-not-a-wildcard"),
    ],
)
async def test_list_tasks_name_wildcards(orch_ctx, name, matches_simple):
    _, simple_id = await _two_jobs()

    page = await tasks.list_tasks(TaskListFilter(name=name))

    assert [t.id for t in page.items] == ([simple_id] if matches_simple else [])


async def test_list_tasks_paging(orch_ctx):
    newest_first, _ = await _two_jobs()

    page = await tasks.list_tasks(TaskListFilter(limit=1, offset=1))

    assert [t.id for t in page.items] == newest_first[1:]
    assert page.total == 2


async def test_list_tasks_unknown_job_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await tasks.list_tasks(TaskListFilter(job="no_such_job"))
