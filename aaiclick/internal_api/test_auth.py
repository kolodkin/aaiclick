import pytest

from aaiclick.auth.view_models import (
    CreateUserRequest,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    TokenPair,
)
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Unauthorized

SECRET = "internal-api-test-secret-key-32-plus-bytes"


async def _make_user(username="alice", password="pw", role="admin"):
    await users.create_user(CreateUserRequest(username=username, password=password, role=role))


async def test_login_success(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    assert isinstance(pair, TokenPair)
    assert pair.access_token and pair.refresh_token


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


async def test_logout_revokes_refresh(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    await auth.logout(LogoutRequest(refresh_token=pair.refresh_token))
    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)
