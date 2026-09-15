"""Inviting a user: create without a password, mint the link that sets one,
and grant the role — only an admin may do it."""

from __future__ import annotations

import pytest

from aaiclick.auth.models import ROLE_ADMIN, ROLE_MEMBER, ROLE_VIEWER, Role
from aaiclick.auth.view_models import (
    CreateUserRequest,
    InviteUserRequest,
    LoginRequest,
    PasswordResetRedeem,
)
from aaiclick.internal_api import auth, invites, password_reset, users
from aaiclick.internal_api.errors import Conflict, Forbidden, NotFound, Unauthorized

SECRET = "internal-api-invites-test-secret-key-32-plus-bytes"


async def _user(username: str, role: Role = ROLE_VIEWER):
    return await users.create_user(CreateUserRequest(username=username, password="pw", role=role))


async def _admin(username="root"):
    return await _user(username, ROLE_ADMIN)


# --- what an invite produces --------------------------------------------


async def test_invite_creates_a_passwordless_user_with_a_link(orch_ctx):
    root = await _admin()
    invite = await invites.invite(root.id, InviteUserRequest(username="alice", email="a@example.com"))
    assert invite.user.username == "alice" and invite.user.has_password is False
    assert invite.user.email == "a@example.com" and invite.user.role == ROLE_VIEWER
    assert invite.link.token and invite.link.expires_at is not None


async def test_invited_user_cannot_log_in_until_the_link_is_redeemed(orch_ctx):
    """The whole point: an invite grants no access on its own."""
    root = await _admin()
    invite = await invites.invite(root.id, InviteUserRequest(username="bob"))
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="bob", password="anything"), secret=SECRET)

    await password_reset.redeem(PasswordResetRedeem(token=invite.link.token, new_password="chosen-pw"))
    pair = await auth.login(LoginRequest(username="bob", password="chosen-pw"), secret=SECRET)
    assert pair.access_token
    assert (await users.get_user(invite.user.id)).has_password is True


async def test_inviting_a_taken_username_conflicts(orch_ctx):
    root = await _admin()
    req = InviteUserRequest(username="dup")
    await invites.invite(root.id, req)
    with pytest.raises(Conflict):
        await invites.invite(root.id, req)


# --- the ceiling ---------------------------------------------------------


@pytest.mark.parametrize("role", [ROLE_VIEWER, ROLE_MEMBER, ROLE_ADMIN])
async def test_admin_invites_any_role(orch_ctx, role):
    root = await _admin()
    invite = await invites.invite(root.id, InviteUserRequest(username=f"u_{role}", role=role))
    assert invite.user.role == role


@pytest.mark.parametrize("role", [ROLE_VIEWER, ROLE_MEMBER])
async def test_only_an_admin_may_invite(orch_ctx, role):
    inviter = await _user("inviter", role)
    with pytest.raises(Forbidden, match="admin role required"):
        await invites.invite(inviter.id, InviteUserRequest(username="nope"))


async def test_unknown_inviter_is_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await invites.invite(999, InviteUserRequest(username="nope"))


async def test_the_cli_is_unrestricted(orch_ctx):
    """``inviter_id=None`` is local mode's synthetic admin and the in-process CLI."""
    invite = await invites.invite(None, InviteUserRequest(username="cli", role=ROLE_ADMIN))
    assert invite.user.username == "cli" and invite.user.role == ROLE_ADMIN
