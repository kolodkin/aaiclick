from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER, SCOPE_ADMIN
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.server.app import API_PREFIX

from ..conftest import login


async def _member(username: str, tenant_id: int, role: str):
    view = await users.create_user(CreateUserRequest(username=username, password="pw"))
    await store.set_membership(tenant_id=tenant_id, user_id=view.id, role=role)
    return view


async def test_invite_returns_user_and_link(orch_ctx, app_client, enabled):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    root = await users.create_user(CreateUserRequest(username="root", password="pw", superadmin=True))
    headers = await login(app_client, "root")
    res = await app_client.post(
        f"{API_PREFIX}/invites",
        json={"username": "invitee", "tenant_id": str(tenant.id), "role": ROLE_VIEWER},
        headers=headers,
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["user"]["username"] == "invitee" and body["user"]["has_password"] is False
    assert body["link"]["token"] and root.id is not None


async def test_viewer_is_forbidden(orch_ctx, app_client, enabled):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await _member("viewer", tenant.id, ROLE_VIEWER)
    headers = await login(app_client, "viewer")
    res = await app_client.post(
        f"{API_PREFIX}/invites",
        json={"username": "nope", "tenant_id": str(tenant.id), "role": ROLE_VIEWER},
        headers=headers,
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_tenant_admin_may_invite_into_their_own_tenant(orch_ctx, app_client, enabled):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await _member("boss", tenant.id, ROLE_ADMIN)
    headers = await login(app_client, "boss")
    res = await app_client.post(
        f"{API_PREFIX}/invites",
        json={"username": "hire", "tenant_id": str(tenant.id), "role": ROLE_ADMIN},
        headers=headers,
    )
    assert res.status_code == 201, res.text


async def test_an_api_token_cannot_invite(orch_ctx, app_client, enabled):
    """An invite creates an account — the permanent foothold a leaked token
    must not be able to mint for itself."""
    tenant = await store.create_tenant(slug="acme", name="Acme")
    boss = await _member("boss", tenant.id, ROLE_ADMIN)
    created = await api_tokens.create_token(
        boss.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    res = await app_client.post(
        f"{API_PREFIX}/invites",
        json={"username": "nope", "tenant_id": str(tenant.id), "role": ROLE_VIEWER},
        headers={"Authorization": f"Bearer {created.token}"},
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"
