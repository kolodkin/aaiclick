from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER, SCOPE_ADMIN, Role
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.server.app import API_PREFIX

from ..conftest import login


async def _user(username: str, role: Role):
    return await users.create_user(CreateUserRequest(username=username, password="pw", role=role))


async def test_invite_returns_user_and_link(orch_ctx, app_client, enabled):
    await _user("root", ROLE_ADMIN)
    headers = await login(app_client, "root")
    res = await app_client.post(
        f"{API_PREFIX}/invites", json={"username": "invitee", "role": ROLE_VIEWER}, headers=headers
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["user"]["username"] == "invitee" and body["user"]["has_password"] is False
    assert body["user"]["role"] == ROLE_VIEWER and body["link"]["token"]


async def test_viewer_is_forbidden(orch_ctx, app_client, enabled):
    await _user("viewer", ROLE_VIEWER)
    headers = await login(app_client, "viewer")
    res = await app_client.post(f"{API_PREFIX}/invites", json={"username": "nope"}, headers=headers)
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_an_api_token_cannot_invite(orch_ctx, app_client, enabled):
    """An invite creates an account — the permanent foothold a leaked token
    must not be able to mint for itself."""
    boss = await _user("boss", ROLE_ADMIN)
    created = await api_tokens.create_token(boss.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN))
    res = await app_client.post(
        f"{API_PREFIX}/invites", json={"username": "nope"}, headers={"Authorization": f"Bearer {created.token}"}
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"
