import pytest

from aaiclick.auth import security, store
from aaiclick.auth.models import ROLE_VIEWER
from aaiclick.auth.view_models import (
    ChangePasswordRequest,
    CreateUserRequest,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    TokenPair,
)
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Unauthorized

SECRET = "internal-api-test-secret-key-32-plus-bytes"


async def _make_user(username="alice", password="pw"):
    await users.create_user(CreateUserRequest(username=username, password=password))


async def test_login_success(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    assert isinstance(pair, TokenPair)
    assert pair.access_token and pair.refresh_token


async def test_login_token_carries_memberships(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="member", password="pw", superadmin=True))
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await store.set_membership(tenant_id=tenant.id, user_id=view.id, role=ROLE_VIEWER)

    pair = await auth.login(LoginRequest(username="member", password="pw"), secret=SECRET)
    claims = security.decode_access_token(pair.access_token, SECRET)
    assert claims.superadmin is True
    assert claims.tenants == {tenant.id: ROLE_VIEWER}


async def test_login_bad_password_raises(orch_ctx):
    await _make_user()
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="alice", password="nope"), secret=SECRET)


async def test_login_unknown_user_raises(orch_ctx):
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="ghost", password="pw"), secret=SECRET)


async def test_disabled_user_cannot_login(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="dis", password="pw"))
    await users.disable_user(view.id, True)
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="dis", password="pw"), secret=SECRET)


async def test_refresh_rotates_and_rejects_reuse(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    rotated = await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)
    assert rotated.refresh_token != pair.refresh_token
    with pytest.raises(Unauthorized):  # reuse of the old token
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)


async def test_change_password_swaps_the_credential(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="pw_user", password="old"))
    await auth.change_password(view.id, ChangePasswordRequest(current_password="old", new_password="new"))

    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="pw_user", password="old"), secret=SECRET)
    assert await auth.login(LoginRequest(username="pw_user", password="new"), secret=SECRET)


async def test_change_password_wrong_current_raises(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="pw_bad", password="old"))
    with pytest.raises(Unauthorized):
        await auth.change_password(view.id, ChangePasswordRequest(current_password="guess", new_password="new"))


async def test_change_password_revokes_existing_sessions(orch_ctx):
    """The reason the endpoint exists: a leaked refresh token must not survive
    the password change that was meant to shut it out."""
    view = await users.create_user(CreateUserRequest(username="pw_sessions", password="old"))
    pair = await auth.login(LoginRequest(username="pw_sessions", password="old"), secret=SECRET)

    await auth.change_password(view.id, ChangePasswordRequest(current_password="old", new_password="new"))

    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)


async def test_logout_revokes_refresh(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    await auth.logout(LogoutRequest(refresh_token=pair.refresh_token))
    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)
