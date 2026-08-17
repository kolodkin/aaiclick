from __future__ import annotations

from sqlmodel import select

from aaiclick.auth import security
from aaiclick.auth.models import ROLE_VIEWER
from aaiclick.orchestration.factories import _callable_to_string, create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import flush_task_logs
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import ClearTaskView, TaskDetail, TaskLogsView
from aaiclick.view_models import STDOUT_STREAM, LogLine, Problem, ProblemCode

from ..app import API_PREFIX

RBAC_SECRET = "rbac-tasks-test-secret-key-32-plus-bytes"


async def test_viewer_cannot_clear_task(orch_ctx, app_client, monkeypatch):
    """``clear_task`` bumps ``run_epoch`` and resets downstream tasks — a
    mutation, so it is admin-only like cancel/purge."""
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", RBAC_SECRET)
    job = await create_job("rbac_clear_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    token = security.encode_access_token(user_id=2, role=ROLE_VIEWER, secret=RBAC_SECRET, ttl=60)

    response = await app_client.post(
        f"{API_PREFIX}/tasks/{task.id}/clear", headers={"Authorization": f"Bearer {token}"}
    )

    assert response.status_code == 403


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


async def test_get_task_logs(orch_ctx, app_client):
    job = await create_job("logs_route_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    response = await app_client.get(f"{API_PREFIX}/tasks/{task.id}/logs")

    assert response.status_code == 200
    logs = TaskLogsView.model_validate(response.json())
    assert logs.available is False


async def test_get_task_logs_reads_clickhouse_through_scope(orch_ctx, app_client):
    """A task with captured logs returns them — exercising the CH-backed read
    path through the endpoint's ``orch_scope_with_ch`` dependency (a SQL-only
    scope would raise 'no active data context' here)."""
    job = await create_job("logs_ch_route", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await flush_task_logs(task.id, job.id, 123, [LogLine(stream=STDOUT_STREAM, text="hello from clickhouse")])
    async with get_sql_session() as s:
        row = (await s.execute(select(Task).where(Task.id == task.id))).scalar_one()
        row.run_ids = [123]
        s.add(row)
        await s.commit()

    response = await app_client.get(f"{API_PREFIX}/tasks/{task.id}/logs")

    assert response.status_code == 200
    logs = TaskLogsView.model_validate(response.json())
    assert logs.available is True
    assert [(line.stream, line.text) for line in logs.lines] == [(STDOUT_STREAM, "hello from clickhouse")]


async def test_get_task_logs_accepts_tail_param(orch_ctx, app_client):
    job = await create_job("logs_tail_route_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    response = await app_client.get(f"{API_PREFIX}/tasks/{task.id}/logs", params={"tail": 5})

    assert response.status_code == 200
    TaskLogsView.model_validate(response.json())


async def test_get_task_logs_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/tasks/999999999/logs")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND


async def test_clear_task(orch_ctx, app_client):
    job = await create_job("http_clear_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    response = await app_client.post(f"{API_PREFIX}/tasks/{task.id}/clear")

    assert response.status_code == 200
    view = ClearTaskView.model_validate(response.json())
    assert view.cleared_task_ids == [task.id]
    assert view.job.id == job.id


async def test_clear_task_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.post(f"{API_PREFIX}/tasks/999999999/clear")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND
