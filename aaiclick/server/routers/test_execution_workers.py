from __future__ import annotations

from aaiclick.auth import security
from aaiclick.internal_api import execution_workers as execution_workers_api
from aaiclick.internal_api.errors import ExecutionWorkerSpawnFailed, Invalid
from aaiclick.orchestration.execution.execution_worker import register_execution_worker
from aaiclick.orchestration.models import EXECUTION_WORKER_STOPPING
from aaiclick.orchestration.view_models import ExecutionWorkerView
from aaiclick.view_models import Page, Problem, ProblemCode

from ..app import API_PREFIX

RBAC_SECRET = "rbac-execution_workers-test-secret-key-32-plus-bytes"


async def test_viewer_cannot_start_worker(orch_ctx, app_client, monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", RBAC_SECRET)
    token = security.encode_access_token(user_id=2, superadmin=False, tenants={}, secret=RBAC_SECRET, ttl=60)
    res = await app_client.post(
        f"{API_PREFIX}/execution-workers", json={}, headers={"Authorization": f"Bearer {token}"}
    )
    assert res.status_code == 403


async def test_list_workers(orch_ctx, app_client):
    await register_execution_worker(hostname="http_worker")

    response = await app_client.get(f"{API_PREFIX}/execution-workers")

    assert response.status_code == 200
    page = Page[ExecutionWorkerView].model_validate(response.json())
    assert page.total is not None and page.total >= 1
    assert any(w.hostname == "http_worker" for w in page.items)


async def test_stop_worker(orch_ctx, app_client):
    execution_worker = await register_execution_worker(hostname="http_stop")

    response = await app_client.post(f"{API_PREFIX}/execution-workers/{execution_worker.id}/stop")

    assert response.status_code == 200
    view = ExecutionWorkerView.model_validate(response.json())
    assert view.status == EXECUTION_WORKER_STOPPING


async def test_stop_worker_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.post(f"{API_PREFIX}/execution-workers/999999999/stop")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND


async def test_stop_already_stopping_returns_409(orch_ctx, app_client):
    execution_worker = await register_execution_worker(hostname="http_double_stop")
    await app_client.post(f"{API_PREFIX}/execution-workers/{execution_worker.id}/stop")

    response = await app_client.post(f"{API_PREFIX}/execution-workers/{execution_worker.id}/stop")

    assert response.status_code == 409
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.CONFLICT


async def test_start_worker_returns_202_with_location(orch_ctx, app_client, monkeypatch):
    async def ok(request):
        return None

    monkeypatch.setattr(execution_workers_api, "start_execution_worker", ok)

    response = await app_client.post(f"{API_PREFIX}/execution-workers", json={"max_tasks": 5})

    assert response.status_code == 202
    assert response.headers["location"] == f"{API_PREFIX}/execution-workers"
    assert response.content == b""


async def test_start_worker_local_mode_returns_422(orch_ctx, app_client, monkeypatch):
    async def raise_invalid(request):
        raise Invalid("requires distributed backends")

    monkeypatch.setattr(execution_workers_api, "start_execution_worker", raise_invalid)

    response = await app_client.post(f"{API_PREFIX}/execution-workers", json={})

    assert response.status_code == 422
    assert Problem.model_validate(response.json()).code is ProblemCode.INVALID


async def test_start_worker_spawn_failure_returns_503(orch_ctx, app_client, monkeypatch):
    async def raise_spawn(request):
        raise ExecutionWorkerSpawnFailed("missing binary")

    monkeypatch.setattr(execution_workers_api, "start_execution_worker", raise_spawn)

    response = await app_client.post(f"{API_PREFIX}/execution-workers", json={})

    assert response.status_code == 503
    assert Problem.model_validate(response.json()).code is ProblemCode.EXECUTION_WORKER_SPAWN_FAILED
