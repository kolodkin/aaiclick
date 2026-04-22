"""Integration tests for ``aaiclick.server.routers.registered_jobs``."""

from __future__ import annotations

from aaiclick.orchestration.registered_jobs import register_job as _register_job_impl
from aaiclick.orchestration.view_models import RegisteredJobView
from aaiclick.view_models import Page, Problem

from ..app import API_PREFIX


async def test_list_registered_jobs(orch_ctx, app_client):
    await _register_job_impl(name="http_rj_a", entrypoint="myapp.http_rj_a")

    response = await app_client.get(f"{API_PREFIX}/registered-jobs")

    assert response.status_code == 200
    page = Page[RegisteredJobView].model_validate(response.json())
    assert page.total is not None and page.total >= 1
    assert any(rj.name == "http_rj_a" for rj in page.items)


async def test_register_job(orch_ctx, app_client):
    response = await app_client.post(
        f"{API_PREFIX}/registered-jobs",
        json={"name": "http_new_rj", "entrypoint": "myapp.http_new_rj"},
    )

    assert response.status_code == 201
    view = RegisteredJobView.model_validate(response.json())
    assert view.name == "http_new_rj"


async def test_register_job_duplicate_returns_409(orch_ctx, app_client):
    await _register_job_impl(name="http_dup_rj", entrypoint="myapp.http_dup_rj")

    response = await app_client.post(
        f"{API_PREFIX}/registered-jobs",
        json={"name": "http_dup_rj", "entrypoint": "myapp.http_dup_rj"},
    )

    assert response.status_code == 409
    problem = Problem.model_validate(response.json())
    assert problem.code == "conflict"


async def test_enable_disable_job(orch_ctx, app_client):
    await _register_job_impl(name="http_toggle", entrypoint="myapp.http_toggle", enabled=False)

    enable = await app_client.post(f"{API_PREFIX}/registered-jobs/http_toggle/enable")
    assert enable.status_code == 200
    assert RegisteredJobView.model_validate(enable.json()).enabled is True

    disable = await app_client.post(f"{API_PREFIX}/registered-jobs/http_toggle/disable")
    assert disable.status_code == 200
    assert RegisteredJobView.model_validate(disable.json()).enabled is False


async def test_enable_unknown_returns_404(orch_ctx, app_client):
    response = await app_client.post(f"{API_PREFIX}/registered-jobs/nope/enable")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code == "not_found"
