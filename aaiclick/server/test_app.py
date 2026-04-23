from __future__ import annotations

from starlette.routing import Route

from .app import API_PREFIX, app


def test_all_resource_routes_are_prefixed():
    excluded = {"/health", "/docs/oauth2-redirect"}
    for r in app.routes:
        if not isinstance(r, Route) or r.path in excluded:
            continue
        assert r.path.startswith(API_PREFIX), f"route {r.path!r} is not under {API_PREFIX}"


def test_expected_routes_are_registered():
    paths = {r.path for r in app.routes if isinstance(r, Route)}
    for expected in [
        f"{API_PREFIX}/jobs",
        f"{API_PREFIX}/jobs/{{ref}}",
        f"{API_PREFIX}/jobs/{{ref}}/stats",
        f"{API_PREFIX}/jobs/{{ref}}/cancel",
        f"{API_PREFIX}/jobs:run",
        f"{API_PREFIX}/registered-jobs",
        f"{API_PREFIX}/registered-jobs/{{name}}/enable",
        f"{API_PREFIX}/registered-jobs/{{name}}/disable",
        f"{API_PREFIX}/tasks/{{task_id}}",
        f"{API_PREFIX}/workers",
        f"{API_PREFIX}/workers/{{worker_id}}/stop",
        f"{API_PREFIX}/objects",
        f"{API_PREFIX}/objects/{{name}}",
        f"{API_PREFIX}/objects:purge",
        "/health",
    ]:
        assert expected in paths, f"missing route {expected}"


async def test_health(app_client):
    response = await app_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_openapi_schema_served_under_prefix(app_client):
    response = await app_client.get(f"{API_PREFIX}/openapi.json")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]
    for model_name in [
        "JobView",
        "JobDetail",
        "JobStatsView",
        "TaskDetail",
        "WorkerView",
        "RegisteredJobView",
        "ObjectView",
        "ObjectDetail",
    ]:
        assert model_name in schemas, f"{model_name} not in OpenAPI components.schemas"
