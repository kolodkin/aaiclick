import pytest

from aaiclick.auth import security
from aaiclick.auth.view_models import CreateTenantRequest, CreateUserRequest
from aaiclick.internal_api import tenants as tenants_api
from aaiclick.internal_api import users as users_api

from ..app import API_PREFIX

SECRET = "router-tenants-test-secret-key-32-plus-bytes"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


def _header(*, superadmin=False, tenants=None):
    token = security.encode_access_token(user_id=1, superadmin=superadmin, tenants=tenants or {}, secret=SECRET, ttl=60)
    return {"Authorization": f"Bearer {token}"}


async def test_create_and_get_tenant_local_mode(orch_ctx, app_client):
    res = await app_client.post(f"{API_PREFIX}/tenants", json={"slug": "acme", "name": "Acme"})
    assert res.status_code == 201 and res.json()["slug"] == "acme"

    tenant_id = res.json()["id"]
    got = await app_client.get(f"{API_PREFIX}/tenants/{tenant_id}")
    assert got.status_code == 200 and got.json()["id"] == tenant_id


async def test_duplicate_slug_conflict(orch_ctx, app_client):
    await app_client.post(f"{API_PREFIX}/tenants", json={"slug": "acme", "name": "Acme"})
    res = await app_client.post(f"{API_PREFIX}/tenants", json={"slug": "acme", "name": "Again"})
    assert res.status_code == 409 and res.json()["code"] == "conflict"


async def test_bad_slug_422(orch_ctx, app_client):
    res = await app_client.post(f"{API_PREFIX}/tenants", json={"slug": "Not A Slug!", "name": "Bad"})
    assert res.status_code == 422


async def test_list_tenants_requires_superadmin(orch_ctx, app_client, enabled):
    res = await app_client.get(f"{API_PREFIX}/tenants", headers=_header(tenants={5: "admin"}))
    assert res.status_code == 403 and res.json()["code"] == "forbidden"
    res = await app_client.get(f"{API_PREFIX}/tenants", headers=_header(superadmin=True))
    assert res.status_code == 200


async def test_member_routes_allow_tenant_admin_only(orch_ctx, app_client, enabled):
    tenant = await tenants_api.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    user = await users_api.create_user(CreateUserRequest(username="lee", password="pw"))

    admin = _header(tenants={tenant.id: "admin"})
    viewer = _header(tenants={tenant.id: "viewer"})

    put = await app_client.put(
        f"{API_PREFIX}/tenants/{tenant.id}/members/{user.id}", json={"role": "viewer"}, headers=admin
    )
    assert put.status_code == 200 and put.json()["role"] == "viewer"

    listed = await app_client.get(f"{API_PREFIX}/tenants/{tenant.id}/members", headers=admin)
    assert listed.status_code == 200 and listed.json()["total"] == 1

    denied = await app_client.put(
        f"{API_PREFIX}/tenants/{tenant.id}/members/{user.id}", json={"role": "admin"}, headers=viewer
    )
    assert denied.status_code == 403

    removed = await app_client.delete(f"{API_PREFIX}/tenants/{tenant.id}/members/{user.id}", headers=admin)
    assert removed.status_code == 204


async def test_get_tenant_non_member_not_found(orch_ctx, app_client, enabled):
    tenant = await tenants_api.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    res = await app_client.get(f"{API_PREFIX}/tenants/{tenant.id}", headers=_header(tenants={999: "admin"}))
    assert res.status_code == 404
