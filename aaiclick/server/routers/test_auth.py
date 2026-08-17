import pytest

from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

SECRET = "router-auth-test-secret-key-32-plus-bytes"


@pytest.fixture
def enabled(monkeypatch):
    # Force distributed mode (auth on) + a signing secret, independent of the
    # local/dist test matrix the suite happens to run under.
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
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


async def test_change_own_password_as_viewer(orch_ctx, app_client, enabled):
    """A viewer can reach ``/auth/me/password`` — ``/users`` is admin-only, so
    this is their only route to a password change."""
    await users.create_user(CreateUserRequest(username="vw", password="pw", role="viewer"))
    login = (await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "vw", "password": "pw"})).json()

    res = await app_client.put(
        f"{API_PREFIX}/auth/me/password",
        json={"current_password": "pw", "new_password": "pw2"},
        headers={"Authorization": f"Bearer {login['access_token']}"},
    )

    assert res.status_code == 204


async def test_change_own_password_wrong_current_401(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="vw2", password="pw", role="viewer"))
    login = (await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "vw2", "password": "pw"})).json()

    res = await app_client.put(
        f"{API_PREFIX}/auth/me/password",
        json={"current_password": "wrong", "new_password": "pw2"},
        headers={"Authorization": f"Bearer {login['access_token']}"},
    )

    assert res.status_code == 401
    assert res.json()["code"] == "unauthorized"


async def test_protected_route_requires_token_when_enabled(orch_ctx, anon_client, enabled):
    res = await anon_client.get(f"{API_PREFIX}/execution-workers")
    assert res.status_code == 401
    assert res.headers["www-authenticate"] == "Bearer"
