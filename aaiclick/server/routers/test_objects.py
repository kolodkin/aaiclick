from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.view_models import Page, Problem, ProblemCode

from ..app import API_PREFIX


async def test_list_objects(orch_ctx, app_client):
    await create_object_from_value([1, 2, 3], name="http_obj_a", scope="global")

    response = await app_client.get(f"{API_PREFIX}/objects")

    assert response.status_code == 200
    page = Page[ObjectView].model_validate(response.json())
    assert any(o.name == "http_obj_a" for o in page.items)


async def test_get_object(orch_ctx, app_client):
    await create_object_from_value([1, 2], name="http_obj_get", scope="global")

    response = await app_client.get(f"{API_PREFIX}/objects/http_obj_get")

    assert response.status_code == 200
    detail = ObjectDetail.model_validate(response.json())
    assert detail.name == "http_obj_get"


async def test_get_object_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/objects/does_not_exist")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND


async def test_delete_object(orch_ctx, app_client):
    await create_object_from_value([9], name="http_obj_del", scope="global")

    response = await app_client.delete(f"{API_PREFIX}/objects/http_obj_del")

    assert response.status_code == 200
    assert response.json() == {"name": "http_obj_del"}


async def test_list_objects_rejects_non_global_scope_returns_422(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/objects", params={"scope": "temp"})

    assert response.status_code == 422
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.INVALID


async def test_purge_without_filters_returns_422(orch_ctx, app_client):
    response = await app_client.post(f"{API_PREFIX}/objects:purge", json={})

    assert response.status_code == 422
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.INVALID
