import pytest

from aaiclick.auth.view_models import CreateUserRequest, LoginRequest, PasswordResetRedeem, RefreshRequest
from aaiclick.internal_api import auth, password_reset, users
from aaiclick.internal_api.errors import NotFound, Unauthorized

SECRET = "internal-api-reset-test-secret-key-32-plus-bytes"


async def test_admin_link_redeems_once_and_revokes_sessions(orch_ctx, monkeypatch):
    monkeypatch.setenv("AAICLICK_PUBLIC_URL", "https://aaiclick.example.com")
    view = await users.create_user(CreateUserRequest(username="alice", password="old"))
    pair = await auth.login(LoginRequest(username="alice", password="old"), secret=SECRET)

    link = await password_reset.create(view.id)
    assert link.url == f"https://aaiclick.example.com/?p=reset%20{link.token}"

    await password_reset.redeem(PasswordResetRedeem(token=link.token, new_password="new"))
    assert await auth.login(LoginRequest(username="alice", password="new"), secret=SECRET)
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="alice", password="old"), secret=SECRET)
    with pytest.raises(Unauthorized):  # sessions ended
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)
    with pytest.raises(Unauthorized):  # single use
        await password_reset.redeem(PasswordResetRedeem(token=link.token, new_password="again"))


async def test_link_without_public_url_and_unknown_user(orch_ctx, monkeypatch):
    monkeypatch.delenv("AAICLICK_PUBLIC_URL", raising=False)
    view = await users.create_user(CreateUserRequest(username="bob", password="pw"))
    assert (await password_reset.create(view.id)).url is None
    with pytest.raises(NotFound):
        await password_reset.create(12345)


async def test_bad_token_rejected(orch_ctx):
    with pytest.raises(Unauthorized):
        await password_reset.redeem(PasswordResetRedeem(token="nope", new_password="x"))
