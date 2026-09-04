import pytest

from aaiclick.auth.view_models import CreateUserRequest, LoginRequest, RefreshRequest, UserView
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Conflict, NotFound, Unauthorized
from aaiclick.view_models import Page

SECRET = "internal-api-users-test-secret-key-32-plus-bytes"


async def _logged_in(username: str) -> tuple[UserView, str]:
    view = await users.create_user(CreateUserRequest(username=username, password="pw", superadmin=True))
    pair = await auth.login(LoginRequest(username=username, password="pw"), secret=SECRET)
    return view, pair.refresh_token


async def test_create_user_returns_view(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="alice", password="pw", superadmin=True))
    assert isinstance(view, UserView)
    assert view.username == "alice" and view.superadmin is True


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


async def test_set_superadmin_missing_raises_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await users.set_superadmin(12345, True)


async def test_demotion_revokes_sessions(orch_ctx):
    """Without revocation a demoted admin's refresh token would keep minting
    tokens, and the demotion would only bind at its natural expiry."""
    view, refresh_token = await _logged_in("demoted")

    await users.set_superadmin(view.id, False)

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


async def test_create_user_without_password_cannot_login(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="sso_only", email="s@example.com"))
    assert view.has_password is False and view.email == "s@example.com"
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="sso_only", password=""), secret=SECRET)


async def test_enable_and_set_email(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="flip", password="pw"))
    assert (await users.disable_user(view.id, True)).disabled is True
    assert (await users.disable_user(view.id, False)).disabled is False
    assert (await users.set_email(view.id, "f@example.com")).email == "f@example.com"
    assert (await users.set_email(view.id, None)).email is None
    with pytest.raises(NotFound):
        await users.set_email(12345, "x@example.com")
