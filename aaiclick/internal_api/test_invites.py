"""Inviting a user: create without a password, mint the link that sets one,
and grant the tenant role — capped at the inviter's own authority."""

from __future__ import annotations

import pytest

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_MEMBER, ROLE_VIEWER
from aaiclick.auth.view_models import (
    CreateUserRequest,
    InviteUserRequest,
    LoginRequest,
    PasswordResetRedeem,
)
from aaiclick.internal_api import auth, invites, password_reset, users
from aaiclick.internal_api.errors import Conflict, Forbidden, Invalid, NotFound, Unauthorized

SECRET = "internal-api-invites-test-secret-key-32-plus-bytes"


async def _tenant(slug="acme"):
    return await store.create_tenant(slug=slug, name=slug.title())


async def _member(username: str, tenant_id: int, role: str):
    view = await users.create_user(CreateUserRequest(username=username, password="pw"))
    await store.set_membership(tenant_id=tenant_id, user_id=view.id, role=role)
    return view


async def _root(username="root"):
    return await users.create_user(CreateUserRequest(username=username, password="pw", superadmin=True))


# --- what an invite produces --------------------------------------------


async def test_invite_creates_a_passwordless_member_with_a_link(orch_ctx):
    tenant = await _tenant()
    root = await _root()
    invite = await invites.invite(
        root.id,
        InviteUserRequest(username="alice", email="a@example.com", tenant_id=tenant.id, role=ROLE_VIEWER),
    )
    assert invite.user.username == "alice" and invite.user.has_password is False
    assert invite.user.email == "a@example.com"
    assert invite.link.token and invite.link.expires_at is not None
    membership = await store.get_membership(tenant_id=tenant.id, user_id=invite.user.id)
    assert membership is not None and membership.role == ROLE_VIEWER


async def test_invited_user_cannot_log_in_until_the_link_is_redeemed(orch_ctx):
    """The whole point: an invite grants no access on its own."""
    tenant = await _tenant()
    root = await _root()
    invite = await invites.invite(root.id, InviteUserRequest(username="bob", tenant_id=tenant.id, role=ROLE_VIEWER))
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="bob", password="anything"), secret=SECRET)

    await password_reset.redeem(PasswordResetRedeem(token=invite.link.token, new_password="chosen-pw"))
    pair = await auth.login(LoginRequest(username="bob", password="chosen-pw"), secret=SECRET)
    assert pair.access_token
    assert (await users.get_user(invite.user.id)).has_password is True


async def test_inviting_a_taken_username_conflicts(orch_ctx):
    tenant = await _tenant()
    root = await _root()
    req = InviteUserRequest(username="dup", tenant_id=tenant.id, role=ROLE_VIEWER)
    await invites.invite(root.id, req)
    with pytest.raises(Conflict):
        await invites.invite(root.id, req)


# --- the ceiling ---------------------------------------------------------


async def test_superadmin_invites_any_role_in_any_tenant(orch_ctx):
    root = await _root()
    one, two = await _tenant("one"), await _tenant("two")
    for i, (tenant, role) in enumerate(((one, ROLE_ADMIN), (two, ROLE_VIEWER))):
        invite = await invites.invite(root.id, InviteUserRequest(username=f"u{i}", tenant_id=tenant.id, role=role))
        membership = await store.get_membership(tenant_id=tenant.id, user_id=invite.user.id)
        assert membership is not None and membership.role == role


async def test_only_a_superadmin_may_invite_a_superadmin(orch_ctx):
    tenant = await _tenant()
    root, admin = await _root(), await _member("admin", tenant.id, ROLE_ADMIN)

    invite = await invites.invite(root.id, InviteUserRequest(username="root2", superadmin=True))
    assert invite.user.superadmin is True and invite.user.id is not None

    with pytest.raises(Forbidden, match="superadmin"):
        await invites.invite(admin.id, InviteUserRequest(username="nope", superadmin=True))


async def test_tenant_admin_invites_both_roles_in_their_own_tenant(orch_ctx):
    tenant = await _tenant()
    admin = await _member("admin", tenant.id, ROLE_ADMIN)
    for i, role in enumerate((ROLE_ADMIN, ROLE_VIEWER)):
        invite = await invites.invite(admin.id, InviteUserRequest(username=f"peer{i}", tenant_id=tenant.id, role=role))
        membership = await store.get_membership(tenant_id=tenant.id, user_id=invite.user.id)
        assert membership is not None and membership.role == role


async def test_tenant_admin_invites_a_member(orch_ctx):
    """The middle rung: member reads and makes its own writes."""
    tenant = await _tenant()
    admin = await _member("admin", tenant.id, ROLE_ADMIN)
    invite = await invites.invite(admin.id, InviteUserRequest(username="hire", tenant_id=tenant.id, role=ROLE_MEMBER))
    membership = await store.get_membership(tenant_id=tenant.id, user_id=invite.user.id)
    assert membership is not None and membership.role == ROLE_MEMBER


async def test_tenant_admin_cannot_invite_into_another_tenant(orch_ctx):
    """404, never 403 — an admin must not be able to probe for tenants."""
    mine, theirs = await _tenant("mine"), await _tenant("theirs")
    admin = await _member("admin", mine.id, ROLE_ADMIN)
    with pytest.raises(NotFound):
        await invites.invite(admin.id, InviteUserRequest(username="nope", tenant_id=theirs.id, role=ROLE_VIEWER))


async def test_viewer_cannot_invite_at_all(orch_ctx):
    tenant = await _tenant()
    viewer = await _member("viewer", tenant.id, ROLE_VIEWER)
    with pytest.raises(Forbidden, match="tenant admin"):
        await invites.invite(viewer.id, InviteUserRequest(username="nope", tenant_id=tenant.id, role=ROLE_VIEWER))


async def test_a_stranger_to_the_tenant_gets_not_found(orch_ctx):
    tenant = await _tenant()
    stranger = await users.create_user(CreateUserRequest(username="stranger", password="pw"))
    with pytest.raises(NotFound):
        await invites.invite(stranger.id, InviteUserRequest(username="nope", tenant_id=tenant.id, role=ROLE_VIEWER))


# --- request shape -------------------------------------------------------


async def test_a_tenant_invite_must_name_a_tenant_and_role(orch_ctx):
    root = await _root()
    with pytest.raises(Invalid, match="tenant"):
        await invites.invite(root.id, InviteUserRequest(username="nope"))


async def test_a_superadmin_invite_must_not_name_a_tenant(orch_ctx):
    tenant = await _tenant()
    root = await _root()
    with pytest.raises(Invalid, match="superadmin"):
        await invites.invite(
            root.id,
            InviteUserRequest(username="nope", superadmin=True, tenant_id=tenant.id, role=ROLE_ADMIN),
        )


async def test_the_cli_is_unrestricted(orch_ctx):
    """``inviter_id=None`` is local mode's synthetic admin and the in-process CLI."""
    tenant = await _tenant()
    invite = await invites.invite(None, InviteUserRequest(username="cli", tenant_id=tenant.id, role=ROLE_ADMIN))
    assert invite.user.username == "cli"
    assert (await invites.invite(None, InviteUserRequest(username="cliroot", superadmin=True))).user.superadmin
