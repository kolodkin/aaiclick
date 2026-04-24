from __future__ import annotations

from fastapi.routing import APIRoute
from starlette.routing import Route

from aaiclick.view_models import Problem

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
        "Problem",
    ]:
        assert model_name in schemas, f"{model_name} not in OpenAPI components.schemas"


async def test_openapi_advertises_problem_responses_for_declared_routes(app_client):
    """Any route that declares a ``Problem`` response via ``problem_responses()`` must
    appear in the OpenAPI spec with the matching ``Problem`` ``$ref`` — guards against
    FastAPI silently dropping the annotation (e.g. handler signature collision).
    """
    paths = (await app_client.get(f"{API_PREFIX}/openapi.json")).json()["paths"]

    saw_any_declared = False
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        problem_codes = {
            code for code, spec in route.responses.items() if isinstance(spec, dict) and spec.get("model") is Problem
        }
        if not problem_codes:
            continue
        saw_any_declared = True
        method = next(iter(route.methods - {"HEAD"})).lower()
        route_responses = paths[route.path][method]["responses"]
        for code in problem_codes:
            key = str(code)
            assert key in route_responses, f"{method.upper()} {route.path} missing {code} in OpenAPI"
            ref = route_responses[key].get("content", {}).get("application/json", {}).get("schema", {}).get("$ref", "")
            assert ref.endswith("/Problem"), f"{method.upper()} {route.path} {code} is not Problem (got {ref!r})"
    assert saw_any_declared, "no routes declared Problem responses — check problem_responses() usage"
