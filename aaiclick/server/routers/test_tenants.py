from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER, SCOPE_ADMIN, SCOPE_WRITE
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateTenantRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens
from aaiclick.internal_api import tenants as tenants_api
from aaiclick.internal_api import users as users_api

from ..app import API_PREFIX
from ..conftest import bearer


def _header(*, superadmin=False, tenants=None):
    return bearer(1, superadmin=superadmin, tenants=tenants)


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


async def test_write_token_cannot_manage_memberships(orch_ctx, app_client, enabled):
    """Path-scoped routes gate on scope too, not role alone: managing members is
    an `admin` operation whichever way the tenant is named."""
    tenant = await store.create_tenant(slug="acme", name="Acme")
    boss = await users_api.create_user(CreateUserRequest(username="boss", password="pw"))
    await store.set_membership(tenant_id=tenant.id, user_id=boss.id, role=ROLE_ADMIN)
    created = await api_tokens.create_token(
        boss.id, CreateApiTokenRequest(name="ci", scope=SCOPE_WRITE, tenant_id=tenant.id)
    )
    res = await app_client.put(
        f"{API_PREFIX}/tenants/{tenant.id}/members/{boss.id}",
        json={"role": ROLE_VIEWER},
        headers={"Authorization": f"Bearer {created.token}"},
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_a_bound_token_cannot_reach_another_tenant_by_path(orch_ctx, app_client, enabled):
    """The binding holds on the path routes too, or it is not a binding."""
    mine = await store.create_tenant(slug="mine", name="Mine")
    theirs = await store.create_tenant(slug="theirs", name="Theirs")
    root = await users_api.create_user(CreateUserRequest(username="root", password="pw", superadmin=True))
    created = await api_tokens.create_token(
        root.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN, tenant_id=mine.id)
    )
    res = await app_client.get(
        f"{API_PREFIX}/tenants/{theirs.id}/members",
        headers={"Authorization": f"Bearer {created.token}"},
    )
    assert res.status_code == 404


async def test_superadmin_is_not_a_membership_role(orch_ctx, app_client, enabled):
    """It is the instance flag on `users`; the boundary rejects it rather than
    leaving a row whose role resolves to instance scope."""
    tenant = await store.create_tenant(slug="acme", name="Acme")
    boss = await users_api.create_user(CreateUserRequest(username="boss", password="pw"))
    await store.set_membership(tenant_id=tenant.id, user_id=boss.id, role=ROLE_ADMIN)
    res = await app_client.put(
        f"{API_PREFIX}/tenants/{tenant.id}/members/{boss.id}",
        json={"role": "superadmin"},
        headers=_header(superadmin=True),
    )
    assert res.status_code == 422
