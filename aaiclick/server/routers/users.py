"""User administration routes (admin-only)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth.view_models import (
    CreateUserRequest,
    PasswordResetLinkView,
    SetEmailRequest,
    SetPasswordRequest,
    SetSuperadminRequest,
    UserListFilter,
    UserView,
)
from aaiclick.internal_api import users as users_api
from aaiclick.view_models import Page

from ..auth import require_superadmin
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(
    prefix="/users",
    tags=["users"],
    dependencies=[Depends(orch_scope), Depends(require_superadmin)],
)


@router.get("", response_model=Page[UserView])
async def list_users(filter: UserListFilter = Depends()) -> Page[UserView]:
    return await users_api.list_users(filter)


@router.post("", response_model=UserView, status_code=201, responses=problem_responses(409))
async def create_user(request: CreateUserRequest) -> UserView:
    return await users_api.create_user(request)


@router.get("/{user_id}", response_model=UserView, responses=problem_responses(404))
async def get_user(user_id: int) -> UserView:
    return await users_api.get_user(user_id)


@router.put("/{user_id}/email", response_model=UserView, responses=problem_responses(404))
async def set_email(user_id: int, request: SetEmailRequest) -> UserView:
    return await users_api.set_email(user_id, request.email)


@router.put("/{user_id}/superadmin", response_model=UserView, responses=problem_responses(404))
async def set_superadmin(user_id: int, request: SetSuperadminRequest) -> UserView:
    return await users_api.set_superadmin(user_id, request.superadmin)


@router.put("/{user_id}/password", response_model=UserView, responses=problem_responses(404))
async def set_password(user_id: int, request: SetPasswordRequest) -> UserView:
    return await users_api.set_password(user_id, request.password)


@router.post("/{user_id}/disable", response_model=UserView, responses=problem_responses(404))
async def disable_user(user_id: int) -> UserView:
    return await users_api.disable_user(user_id, True)


@router.post("/{user_id}/enable", response_model=UserView, responses=problem_responses(404))
async def enable_user(user_id: int) -> UserView:
    return await users_api.disable_user(user_id, False)


@router.post("/{user_id}/mfa/reset", response_model=UserView, responses=problem_responses(404))
async def reset_mfa(user_id: int) -> UserView:
    """Lost-authenticator recovery — there are no recovery codes."""
    return await users_api.reset_mfa(user_id)


@router.post("/{user_id}/password-reset", response_model=PasswordResetLinkView, responses=problem_responses(404))
async def create_password_reset(user_id: int) -> PasswordResetLinkView:
    """Mint a one-time reset link to hand to the user out of band."""
    return await users_api.create_password_reset(user_id)
