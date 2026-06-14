import pytest

from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

SECRET = "router-auth-test-secret-key-32-plus-bytes"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


async def test_login_then_access_protected(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw", role="admin"))

    login = await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "pw"})
    assert login.status_code == 200
    access = login.json()["access_token"]

    me = await app_client.get(f"{API_PREFIX}/auth/me", headers={"Authorization": f"Bearer {access}"})
    assert me.status_code == 200 and me.json()["role"] == "admin"


async def test_login_bad_password_401(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    res = await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "x"})
    assert res.status_code == 401
    assert res.json()["code"] == "unauthorized"


async def test_refresh_flow(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    login = (await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "pw"})).json()
    res = await app_client.post(f"{API_PREFIX}/auth/refresh", json={"refresh_token": login["refresh_token"]})
    assert res.status_code == 200 and res.json()["refresh_token"] != login["refresh_token"]


async def test_protected_route_requires_token_when_enabled(orch_ctx, app_client, enabled):
    res = await app_client.get(f"{API_PREFIX}/workers")
    assert res.status_code == 401
    assert res.headers["www-authenticate"] == "Bearer"
