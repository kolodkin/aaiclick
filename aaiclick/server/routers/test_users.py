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
