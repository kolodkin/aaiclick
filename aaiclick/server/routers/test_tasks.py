from __future__ import annotations

from aaiclick.orchestration.factories import _callable_to_string, create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.view_models import TaskDetail
from aaiclick.view_models import Problem, ProblemCode

from ..app import API_PREFIX


async def test_get_task(orch_ctx, app_client):
    job = await create_job("http_task_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    response = await app_client.get(f"{API_PREFIX}/tasks/{task.id}")

    assert response.status_code == 200
    detail = TaskDetail.model_validate(response.json())
    assert detail.id == task.id
    assert detail.entrypoint == _callable_to_string(simple_task)


async def test_get_task_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/tasks/999999999")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND
