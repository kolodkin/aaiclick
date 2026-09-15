"""Inviting a user: create the account without a password, grant the role,
and mint the one-time link that sets the password.

Its own module rather than a function on ``users``: ``password_reset`` already
imports ``users``, so composing the two there would close an import cycle.
"""

from __future__ import annotations

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_SCOPES, SCOPE_ADMIN, scope_admits
from aaiclick.auth.view_models import CreateUserRequest, InviteUserRequest, InviteView

from . import password_reset, users
from .errors import Forbidden, NotFound


async def _check_ceiling(inviter_id: int | None) -> None:
    """Only an admin invites. ``inviter_id is None`` is local mode's synthetic
    admin and the in-process CLI — the same latitude they have everywhere."""
    if inviter_id is None:
        return
    inviter = await store.get_user_by_id(inviter_id)
    if inviter is None:
        raise NotFound(f"user {inviter_id} not found")
    if not scope_admits(ROLE_SCOPES[inviter.role], SCOPE_ADMIN):
        raise Forbidden("admin role required to invite")


async def invite(inviter_id: int | None, request: InviteUserRequest) -> InviteView:
    """Invite ``request.username`` with ``request.role``, as ``inviter_id`` is allowed to.

    The account grants no access until the link is redeemed — ``login`` refuses
    any user whose ``password_hash`` is ``None`` — so an invite left unredeemed
    is inert rather than an open door.
    """
    await _check_ceiling(inviter_id)
    user = await users.create_user(CreateUserRequest(username=request.username, role=request.role, email=request.email))
    return InviteView(user=user, link=await password_reset.create(user.id))
