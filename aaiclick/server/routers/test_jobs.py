from __future__ import annotations

from aaiclick.orchestration.factories import _callable_to_string, create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.models import JOB_CANCELLED, JOB_PENDING
from aaiclick.orchestration.view_models import JobDetail, JobStatsView, JobView
from aaiclick.view_models import Page, Problem, ProblemCode

from ..app import API_PREFIX


async def test_list_jobs_returns_page(orch_ctx, app_client):
    await create_job("http_list_a", simple_task)

    response = await app_client.get(f"{API_PREFIX}/jobs")

    assert response.status_code == 200
    page = Page[JobView].model_validate(response.json())
    assert page.total is not None and page.total >= 1
    assert any(j.name == "http_list_a" for j in page.items)


async def test_list_jobs_filter_by_status(orch_ctx, app_client):
    await create_job("http_status_job", simple_task)

    response = await app_client.get(
        f"{API_PREFIX}/jobs",
        params={"status": JOB_PENDING},
    )

    assert response.status_code == 200
    page = Page[JobView].model_validate(response.json())
    assert any(j.name == "http_status_job" for j in page.items)


async def test_get_job_by_int_id(orch_ctx, app_client):
    created = await create_job("http_get", simple_task)

    response = await app_client.get(f"{API_PREFIX}/jobs/{created.id}")

    assert response.status_code == 200
    detail = JobDetail.model_validate(response.json())
    assert detail.id == created.id
    assert detail.name == "http_get"


async def test_get_job_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/jobs/999999999")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND
    assert problem.status == 404


async def test_job_stats(orch_ctx, app_client):
    created = await create_job("http_stats", simple_task)

    response = await app_client.get(f"{API_PREFIX}/jobs/{created.id}/stats")

    assert response.status_code == 200
    stats = JobStatsView.model_validate(response.json())
    assert stats.job_id == created.id


async def test_cancel_job(orch_ctx, app_client):
    created = await create_job("http_cancel", simple_task)

    response = await app_client.post(f"{API_PREFIX}/jobs/{created.id}/cancel")

    assert response.status_code == 200
    view = JobView.model_validate(response.json())
    assert view.status == JOB_CANCELLED


async def test_cancel_already_cancelled_returns_409(orch_ctx, app_client):
    created = await create_job("http_double_cancel", simple_task)
    await app_client.post(f"{API_PREFIX}/jobs/{created.id}/cancel")

    response = await app_client.post(f"{API_PREFIX}/jobs/{created.id}/cancel")

    assert response.status_code == 409
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.CONFLICT


async def test_run_job(orch_ctx, app_client):
    response = await app_client.post(
        f"{API_PREFIX}/jobs:run",
        json={"name": _callable_to_string(simple_task)},
    )

    assert response.status_code == 201
    view = JobView.model_validate(response.json())
    assert view.name == simple_task.__name__
