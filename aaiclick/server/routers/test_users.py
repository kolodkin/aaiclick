import pytest

from aaiclick.auth import security
from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

SECRET = "router-users-test-secret-key-32-plus-bytes"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


def _admin_header():
    token = security.encode_access_token(user_id=1, superadmin=True, tenants={}, secret=SECRET, ttl=60)
    return {"Authorization": f"Bearer {token}"}


def _viewer_header():
    token = security.encode_access_token(user_id=2, superadmin=False, tenants={9: "admin"}, secret=SECRET, ttl=60)
    return {"Authorization": f"Bearer {token}"}


async def test_admin_can_create_user(orch_ctx, app_client, enabled):
    res = await app_client.post(
        f"{API_PREFIX}/users",
        json={"username": "newbie", "password": "pw"},
        headers=_admin_header(),
    )
    assert res.status_code == 201 and res.json()["username"] == "newbie"


async def test_viewer_forbidden(orch_ctx, app_client, enabled):
    res = await app_client.post(
        f"{API_PREFIX}/users",
        json={"username": "x", "password": "pw"},
        headers=_viewer_header(),
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_list_users(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    res = await app_client.get(f"{API_PREFIX}/users", headers=_admin_header())
    assert res.status_code == 200 and res.json()["total"] >= 1


async def test_get_enable_and_email_routes(orch_ctx, app_client, enabled):
    created = await users.create_user(CreateUserRequest(username="carol", password="pw"))
    got = await app_client.get(f"{API_PREFIX}/users/{created.id}", headers=_admin_header())
    assert got.status_code == 200 and got.json()["username"] == "carol"

    await app_client.post(f"{API_PREFIX}/users/{created.id}/disable", headers=_admin_header())
    back = await app_client.post(f"{API_PREFIX}/users/{created.id}/enable", headers=_admin_header())
    assert back.status_code == 200 and back.json()["disabled"] is False

    mail = await app_client.put(
        f"{API_PREFIX}/users/{created.id}/email", json={"email": "c@example.com"}, headers=_admin_header()
    )
    assert mail.status_code == 200 and mail.json()["email"] == "c@example.com"
    assert (await app_client.get(f"{API_PREFIX}/users/0", headers=_admin_header())).status_code == 404
