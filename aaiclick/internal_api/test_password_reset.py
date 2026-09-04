from unittest.mock import AsyncMock

import pytest

from aaiclick.auth import mail
from aaiclick.auth.view_models import (
    CreateUserRequest,
    LoginRequest,
    PasswordResetRedeem,
    PasswordResetRequest,
    RefreshRequest,
)
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


async def test_request_mails_link_when_configured(orch_ctx, monkeypatch):
    monkeypatch.setenv("AAICLICK_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("AAICLICK_SMTP_FROM", "noreply@example.com")
    monkeypatch.setenv("AAICLICK_PUBLIC_URL", "https://aaiclick.example.com")
    sent = AsyncMock()
    monkeypatch.setattr(mail, "send_mail", sent)
    await users.create_user(CreateUserRequest(username="carol", password="pw", email="carol@example.com"))

    await password_reset.request(PasswordResetRequest(username="carol"))

    sent.assert_awaited_once()
    kwargs = sent.await_args.kwargs if sent.await_args else {}
    body = kwargs["body"]
    assert kwargs["to"] == "carol@example.com"
    token = body.split("?p=reset%20")[1].split()[0]
    await password_reset.redeem(PasswordResetRedeem(token=token, new_password="mailed"))
    assert await auth.login(LoginRequest(username="carol", password="mailed"), secret=SECRET)


@pytest.mark.parametrize(
    "smtp, username, email",
    [
        pytest.param(True, "ghost", None, id="unknown-user"),
        pytest.param(True, "noemail", None, id="user-without-email"),
        pytest.param(False, "withemail", "w@example.com", id="mail-not-configured"),
    ],
)
async def test_request_is_silent_when_undeliverable(orch_ctx, monkeypatch, smtp, username, email):
    if smtp:
        monkeypatch.setenv("AAICLICK_SMTP_HOST", "smtp.example.com")
        monkeypatch.setenv("AAICLICK_SMTP_FROM", "noreply@example.com")
    else:
        monkeypatch.delenv("AAICLICK_SMTP_HOST", raising=False)
    sent = AsyncMock()
    monkeypatch.setattr(mail, "send_mail", sent)
    if username != "ghost":
        await users.create_user(CreateUserRequest(username=username, password="pw", email=email))

    await password_reset.request(PasswordResetRequest(username=username))

    sent.assert_not_awaited()
