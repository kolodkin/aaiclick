"""Invite routes.

Its own router rather than a path under ``/users``: that router is admin-only
at the router level and takes ``orch_scope``; an invite only needs a session,
and the ceiling lives in ``internal_api.invites`` so the CLI and the HTTP
surface enforce one rule.

``require_session`` rather than ``require_principal``: an invite creates an
account, which is exactly the permanent foothold a leaked API token must not be
able to mint for itself — the same reason token management needs a session.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth.view_models import InviteUserRequest, InviteView
from aaiclick.internal_api import invites as invites_api

from ..auth import Principal, require_session
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/invites", tags=["invites"], dependencies=[Depends(orch_scope)])


@router.post("", response_model=InviteView, status_code=201, responses=problem_responses(403, 404, 409, 422))
async def invite_user(request: InviteUserRequest, principal: Principal = Depends(require_session)) -> InviteView:
    """Create a passwordless user with the given role, and mint their link."""
    return await invites_api.invite(principal.user_id, request)
