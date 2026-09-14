"""Inviting a user: create the account without a password, grant the tenant
role, and mint the one-time link that sets the password.

Its own module rather than a function on ``users``: ``password_reset`` already
imports ``users``, so composing the two there would close an import cycle.
"""

from __future__ import annotations

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_SCOPES, SCOPE_ADMIN, scope_admits
from aaiclick.auth.view_models import CreateUserRequest, InviteUserRequest, InviteView

from . import password_reset, users
from .errors import Forbidden, Invalid, NotFound


def _check_shape(request: InviteUserRequest) -> None:
    """An invite is instance-level or tenant-level, never both and never neither."""
    if request.superadmin:
        if request.tenant_id is not None or request.role is not None:
            raise Invalid("a superadmin invite names no tenant or role — the flag is instance-wide")
        return
    if request.tenant_id is None or request.role is None:
        raise Invalid("a tenant invite must name both tenant_id and role")


async def _check_ceiling(inviter_id: int | None, request: InviteUserRequest) -> None:
    """Cap the invite at the inviter's own authority.

    ``inviter_id is None`` is local mode's synthetic admin and the in-process
    CLI, both superadmin-equivalent — the same latitude they have everywhere
    else.
    """
    if inviter_id is None:
        return
    inviter = await store.get_user_by_id(inviter_id)
    if inviter is None:
        raise NotFound(f"user {inviter_id} not found")
    if inviter.superadmin:
        return
    if request.superadmin:
        raise Forbidden("only a superadmin may invite a superadmin")
    assert request.tenant_id is not None  # _check_shape ran first
    membership = await store.get_membership(tenant_id=request.tenant_id, user_id=inviter_id)
    if membership is None:
        # Missing, never forbidden — an inviter must not be able to probe for
        # tenants they have no part in.
        raise NotFound(f"tenant {request.tenant_id} not found")
    # Inviting delegates authority, so it is capped like every other gate: the
    # granted role's scope may not exceed the inviter's own.
    if not scope_admits(ROLE_SCOPES[membership.role], SCOPE_ADMIN):
        raise Forbidden("tenant admin role required to invite")
    assert request.role is not None  # _check_shape ran first
    if not scope_admits(ROLE_SCOPES[membership.role], ROLE_SCOPES[request.role]):
        raise Forbidden(f"cannot invite '{request.role}' — your role here is '{membership.role}'")


async def invite(inviter_id: int | None, request: InviteUserRequest) -> InviteView:
    """Invite ``request.username``, as ``inviter_id`` is allowed to.

    The account grants no access until the link is redeemed — ``login`` refuses
    any user whose ``password_hash`` is ``None`` — so an invite left unredeemed
    is inert rather than an open door.
    """
    _check_shape(request)
    await _check_ceiling(inviter_id, request)
    if request.tenant_id is not None and await store.get_tenant_by_id(request.tenant_id) is None:
        # Before the account exists: a membership that fails the foreign key
        # afterwards would strand a user row nobody asked for.
        raise NotFound(f"tenant {request.tenant_id} not found")
    user = await users.create_user(
        CreateUserRequest(username=request.username, superadmin=request.superadmin, email=request.email)
    )
    if request.tenant_id is not None and request.role is not None:
        await store.set_membership(tenant_id=request.tenant_id, user_id=user.id, role=request.role)
    return InviteView(user=user, link=await password_reset.create(user.id))
