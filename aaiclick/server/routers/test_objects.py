"""Integration tests for ``aaiclick.server.routers.objects``.

Object routes enter ``orch_context(with_ch=True)`` per request. The tests
use the existing ``orch_ctx`` fixture which provides the outer context and
ch client — the per-request nested orch_context reuses the outer client.
"""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.view_models import Page, Problem

from ..app import API_PREFIX


async def test_list_objects(orch_ctx, app_client):
    await create_object_from_value([1, 2, 3], name="http_obj_a")

    response = await app_client.get(f"{API_PREFIX}/objects")

    assert response.status_code == 200
    page = Page[ObjectView].model_validate(response.json())
    assert any(o.name == "http_obj_a" for o in page.items)


async def test_get_object(orch_ctx, app_client):
    await create_object_from_value([1, 2], name="http_obj_get")

    response = await app_client.get(f"{API_PREFIX}/objects/http_obj_get")

    assert response.status_code == 200
    detail = ObjectDetail.model_validate(response.json())
    assert detail.name == "http_obj_get"


async def test_get_object_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/objects/does_not_exist")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code == "not_found"


async def test_delete_object(orch_ctx, app_client):
    await create_object_from_value([9], name="http_obj_del")

    response = await app_client.delete(f"{API_PREFIX}/objects/http_obj_del")

    assert response.status_code == 200
    assert response.json() == {"name": "http_obj_del"}


async def test_list_objects_rejects_non_global_scope_returns_422(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/objects", params={"scope": "temp"})

    assert response.status_code == 422
    problem = Problem.model_validate(response.json())
    assert problem.code == "invalid"


async def test_purge_without_filters_returns_422(orch_ctx, app_client):
    response = await app_client.post(f"{API_PREFIX}/objects:purge", json={})

    assert response.status_code == 422
    problem = Problem.model_validate(response.json())
    assert problem.code == "invalid"
