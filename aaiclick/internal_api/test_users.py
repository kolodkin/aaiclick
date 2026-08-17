import pytest

from aaiclick.auth.view_models import CreateUserRequest, LoginRequest, RefreshRequest, UserView
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Conflict, NotFound, Unauthorized
from aaiclick.view_models import Page

SECRET = "internal-api-users-test-secret-key-32-plus-bytes"


async def _logged_in(username: str) -> tuple[UserView, str]:
    view = await users.create_user(CreateUserRequest(username=username, password="pw", role="admin"))
    pair = await auth.login(LoginRequest(username=username, password="pw"), secret=SECRET)
    return view, pair.refresh_token


async def test_create_user_returns_view(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="alice", password="pw", role="admin"))
    assert isinstance(view, UserView)
    assert view.username == "alice" and view.role == "admin"


async def test_create_duplicate_raises_conflict(orch_ctx):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    with pytest.raises(Conflict):
        await users.create_user(CreateUserRequest(username="alice", password="pw"))


async def test_list_users_paginated(orch_ctx):
    await users.create_user(CreateUserRequest(username="a", password="pw"))
    await users.create_user(CreateUserRequest(username="b", password="pw"))
    page = await users.list_users()
    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2


async def test_set_role_missing_raises_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await users.set_role(12345, "admin")


async def test_demotion_revokes_sessions(orch_ctx):
    """Without revocation a demoted admin's refresh token would keep minting
    tokens, and the demotion would only bind at its natural expiry."""
    view, refresh_token = await _logged_in("demoted")

    await users.set_role(view.id, "viewer")

    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=refresh_token), secret=SECRET)


async def test_disable_revokes_sessions(orch_ctx):
    view, refresh_token = await _logged_in("disabled_user")

    await users.disable_user(view.id, True)

    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=refresh_token), secret=SECRET)


async def test_admin_password_reset_revokes_sessions(orch_ctx):
    view, refresh_token = await _logged_in("reset_user")

    await users.set_password(view.id, "brand-new")

    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=refresh_token), secret=SECRET)


async def test_reenabling_a_user_leaves_sessions_alone(orch_ctx):
    """Re-enabling is not a security event, so it does not need to revoke — and
    the user has no live sessions to protect at that point anyway."""
    view, _ = await _logged_in("reenabled")
    await users.disable_user(view.id, True)

    restored = await users.disable_user(view.id, False)

    assert restored.disabled is False
    assert await auth.login(LoginRequest(username="reenabled", password="pw"), secret=SECRET)
