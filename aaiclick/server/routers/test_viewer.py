from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.view_models import Page, Problem, ProblemCode
from aaiclick.viewer.view_models import Dashboard, DashboardResults, SavedQuery

from ..app import API_PREFIX

V = f"{API_PREFIX}/viewer"


async def test_query_object_route(orch_ctx, app_client):
    await create_object_from_value({"id": [1, 2], "name": ["a", "b"]}, name="http_orders", scope="global")
    response = await app_client.post(f"{V}/query", json={"object": "http_orders", "order_by": [["id", "ASC"]]})
    assert response.status_code == 200 and response.headers["content-type"].startswith("application/json")
    body = response.json()  # ClickHouse's JSONCompact, verbatim
    assert [c["name"] for c in body["meta"]] == ["id", "name"] and body["data"] == [["1", "a"], ["2", "b"]]
    assert body["rows"] == 2 and "statistics" in body

    csv = await app_client.post(f"{V}/query", json={"object": "http_orders", "fields": ["id"], "fmt": "csv"})
    assert csv.headers["content-type"].startswith("text/csv") and csv.text.splitlines() == ['"id"', "1", "2"]


async def test_query_object_route_errors(orch_ctx, app_client):
    missing = await app_client.post(f"{V}/query", json={"object": "nope"})
    assert missing.status_code == 404 and Problem.model_validate(missing.json()).code is ProblemCode.NOT_FOUND
    bad = await app_client.post(f"{V}/query", json={"object": "nope", "where": "1; DROP TABLE x"})
    assert bad.status_code == 422


async def test_saved_query_and_dashboard_routes(orch_ctx, app_client):
    await create_object_from_value({"id": [1]}, name="http_o", scope="global")
    put = await app_client.put(f"{V}/queries/q1", json={"object": "http_o", "where": "id > 0"})
    assert put.status_code == 200 and SavedQuery.model_validate(put.json()).name == "q1"
    listed = Page[SavedQuery].model_validate((await app_client.get(f"{V}/queries")).json())
    assert [q.name for q in listed.items] == ["q1"]

    put_d = await app_client.put(f"{V}/dashboards/d1", json={"html": "<p/>", "queries": {"p": {"object": "http_o"}}})
    assert put_d.status_code == 200 and Dashboard.model_validate(put_d.json()).name == "d1"
    run = await app_client.post(f"{V}/dashboards/d1:run")
    assert DashboardResults.model_validate(run.json()).results == {"p": {"id": ["1"]}}

    assert (await app_client.delete(f"{V}/queries/q1")).status_code == 200
    assert (await app_client.delete(f"{V}/dashboards/d1")).status_code == 200
    assert (await app_client.get(f"{V}/dashboards/d1")).status_code == 404


async def test_viewer_requires_auth(orch_ctx, anon_client, monkeypatch):
    # Distributed mode (auth on), independent of the local/dist test matrix.
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", "viewer-router-test-secret-key-32-plus-bytes")
    response = await anon_client.post(f"{V}/query", json={"object": "x"})
    assert response.status_code == 401
