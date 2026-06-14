from __future__ import annotations

from aaiclick.auth import security
from aaiclick.auth.models import ROLE_VIEWER
from aaiclick.internal_api import workers as workers_api
from aaiclick.internal_api.errors import Invalid, WorkerSpawnFailed
from aaiclick.orchestration.execution.worker import register_worker
from aaiclick.orchestration.models import WORKER_STOPPING
from aaiclick.orchestration.view_models import WorkerView
from aaiclick.view_models import Page, Problem, ProblemCode

from ..app import API_PREFIX

RBAC_SECRET = "rbac-workers-test-secret-key-32-plus-bytes"


async def test_viewer_cannot_start_worker(orch_ctx, app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.setenv("AAICLICK_JWT_SECRET", RBAC_SECRET)
    token = security.encode_access_token(user_id=2, role=ROLE_VIEWER, secret=RBAC_SECRET, ttl=60)
    res = await app_client.post(f"{API_PREFIX}/workers", json={}, headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 403


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


async def test_start_worker_returns_202_with_location(orch_ctx, app_client, monkeypatch):
    async def ok(request):
        return None

    monkeypatch.setattr(workers_api, "start_worker", ok)

    response = await app_client.post(f"{API_PREFIX}/workers", json={"max_tasks": 5})

    assert response.status_code == 202
    assert response.headers["location"] == f"{API_PREFIX}/workers"
    assert response.content == b""


async def test_start_worker_local_mode_returns_422(orch_ctx, app_client, monkeypatch):
    async def raise_invalid(request):
        raise Invalid("requires distributed backends")

    monkeypatch.setattr(workers_api, "start_worker", raise_invalid)

    response = await app_client.post(f"{API_PREFIX}/workers", json={})

    assert response.status_code == 422
    assert Problem.model_validate(response.json()).code is ProblemCode.INVALID


async def test_start_worker_spawn_failure_returns_503(orch_ctx, app_client, monkeypatch):
    async def raise_spawn(request):
        raise WorkerSpawnFailed("missing binary")

    monkeypatch.setattr(workers_api, "start_worker", raise_spawn)

    response = await app_client.post(f"{API_PREFIX}/workers", json={})

    assert response.status_code == 503
    assert Problem.model_validate(response.json()).code is ProblemCode.WORKER_SPAWN_FAILED
