from __future__ import annotations

from aaiclick.orchestration.execution.worker import register_worker
from aaiclick.orchestration.models import WORKER_STOPPING
from aaiclick.orchestration.view_models import WorkerView
from aaiclick.view_models import Page, Problem, ProblemCode

from ..app import API_PREFIX


async def test_list_workers(orch_ctx, app_client):
    await register_worker(hostname="http_worker")

    response = await app_client.get(f"{API_PREFIX}/workers")

    assert response.status_code == 200
    page = Page[WorkerView].model_validate(response.json())
    assert page.total is not None and page.total >= 1
    assert any(w.hostname == "http_worker" for w in page.items)


async def test_stop_worker(orch_ctx, app_client):
    worker = await register_worker(hostname="http_stop")

    response = await app_client.post(f"{API_PREFIX}/workers/{worker.id}/stop")

    assert response.status_code == 200
    view = WorkerView.model_validate(response.json())
    assert view.status == WORKER_STOPPING


async def test_stop_worker_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.post(f"{API_PREFIX}/workers/999999999/stop")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND


async def test_stop_already_stopping_returns_409(orch_ctx, app_client):
    worker = await register_worker(hostname="http_double_stop")
    await app_client.post(f"{API_PREFIX}/workers/{worker.id}/stop")

    response = await app_client.post(f"{API_PREFIX}/workers/{worker.id}/stop")

    assert response.status_code == 409
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.CONFLICT
