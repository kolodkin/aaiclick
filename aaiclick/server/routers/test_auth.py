from aaiclick.auth import security
from aaiclick.auth.view_models import CreateUserRequest, MfaEnableRequest
from aaiclick.internal_api import auth as auth_api
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

from ..conftest import login


async def test_login_then_access_protected(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw", superadmin=True))

    login = await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "pw"})
    assert login.status_code == 200
    access = login.json()["access_token"]

    me = await app_client.get(f"{API_PREFIX}/auth/me", headers={"Authorization": f"Bearer {access}"})
    assert me.status_code == 200
    body = me.json()
    assert body["superadmin"] is True and body["username"] == "alice" and body["tenants"] == []


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
    await users.create_user(CreateUserRequest(username="vw", password="pw"))
    login = (await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "vw", "password": "pw"})).json()

    res = await app_client.put(
        f"{API_PREFIX}/auth/me/password",
        json={"current_password": "pw", "new_password": "pw2"},
        headers={"Authorization": f"Bearer {login['access_token']}"},
    )

    assert res.status_code == 204


async def test_change_own_password_wrong_current_401(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="vw2", password="pw"))
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


async def test_api_token_lifecycle(orch_ctx, app_client, enabled):
    """Session mints a token → the token authenticates → revoke → 401."""
    await users.create_user(CreateUserRequest(username="alice", password="pw", superadmin=True))
    session = await login(app_client, "alice")

    created = await app_client.post(f"{API_PREFIX}/auth/tokens", json={"name": "ci", "scope": "write"}, headers=session)
    assert created.status_code == 201
    body = created.json()
    assert body["token"].startswith("aaic_") and body["scope"] == "write"
    token_header = {"Authorization": f"Bearer {body['token']}"}

    me = await app_client.get(f"{API_PREFIX}/auth/me", headers=token_header)
    assert me.status_code == 200 and me.json()["username"] == "alice"

    listed = await app_client.get(f"{API_PREFIX}/auth/tokens", headers=session)
    assert listed.status_code == 200 and listed.json()["total"] == 1 and "token" not in listed.json()["items"][0]

    gone = await app_client.delete(f"{API_PREFIX}/auth/tokens/{body['id']}", headers=session)
    assert gone.status_code == 204
    assert (await app_client.get(f"{API_PREFIX}/auth/me", headers=token_header)).status_code == 401


async def test_read_token_cannot_write_or_manage_tokens(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw", superadmin=True))
    session = await login(app_client, "alice")
    created = await app_client.post(f"{API_PREFIX}/auth/tokens", json={"name": "ro"}, headers=session)
    token_header = {"Authorization": f"Bearer {created.json()['token']}", "X-Tenant-Id": "1"}

    assert (await app_client.get(f"{API_PREFIX}/jobs", headers=token_header)).status_code == 200
    denied = await app_client.post(
        f"{API_PREFIX}/users", json={"username": "x", "password": "pw"}, headers=token_header
    )
    assert denied.status_code == 403 and denied.json()["code"] == "forbidden"
    # Even a write token may not touch token management — the scope check
    # happens first for this read token, so use the listing to prove it.
    assert (await app_client.get(f"{API_PREFIX}/auth/tokens", headers=token_header)).status_code == 403


async def test_token_routes_422_in_local_mode(orch_ctx, app_client, monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)
    res = await app_client.get(f"{API_PREFIX}/auth/tokens")
    assert res.status_code == 422 and res.json()["code"] == "invalid"


async def test_oidc_config_is_public(orch_ctx, anon_client, enabled, monkeypatch):
    monkeypatch.delenv("AAICLICK_OIDC_ISSUER", raising=False)
    res = await anon_client.get(f"{API_PREFIX}/auth/oidc/config")
    assert res.status_code == 200 and res.json()["enabled"] is False
    res = await anon_client.post(f"{API_PREFIX}/auth/oidc/start")
    assert res.status_code == 422 and res.json()["code"] == "invalid"


async def test_login_mfa_required_problem_code(orch_ctx, app_client, enabled):
    view = await users.create_user(CreateUserRequest(username="mfa", password="pw"))
    setup = await auth_api.mfa_setup(view.id)
    await auth_api.mfa_enable(view.id, MfaEnableRequest(code=security.totp_code(setup.secret)))

    res = await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "mfa", "password": "pw"})
    assert res.status_code == 401 and res.json()["code"] == "mfa_required"
    ok = await app_client.post(
        f"{API_PREFIX}/auth/login",
        json={"username": "mfa", "password": "pw", "totp_code": security.totp_code(setup.secret)},
    )
    assert ok.status_code == 200


async def test_password_reset_routes(orch_ctx, enabled, anon_client, app_client):
    """``enabled`` precedes ``app_client`` so the fixture mints its admin header."""
    view = await users.create_user(CreateUserRequest(username="reset_me", password="old"))
    # Self-service request is public and always 204.
    assert (
        await anon_client.post(f"{API_PREFIX}/auth/password-reset/request", json={"username": "nobody"})
    ).status_code == 204
    # Superadmin mints a link; anonymous redeems it.
    link = await app_client.post(f"{API_PREFIX}/users/{view.id}/password-reset")
    assert link.status_code == 200 and link.json()["token"]
    redeemed = await anon_client.post(
        f"{API_PREFIX}/auth/password-reset", json={"token": link.json()["token"], "new_password": "new"}
    )
    assert redeemed.status_code == 204
    again = await anon_client.post(
        f"{API_PREFIX}/auth/password-reset", json={"token": link.json()["token"], "new_password": "x"}
    )
    assert again.status_code == 401
