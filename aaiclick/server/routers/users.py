"""User administration routes (admin-only)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth.view_models import (
    CreateUserRequest,
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


@router.put("/{user_id}/superadmin", response_model=UserView, responses=problem_responses(404))
async def set_superadmin(user_id: int, request: SetSuperadminRequest) -> UserView:
    return await users_api.set_superadmin(user_id, request.superadmin)


@router.put("/{user_id}/password", response_model=UserView, responses=problem_responses(404))
async def set_password(user_id: int, request: SetPasswordRequest) -> UserView:
    return await users_api.set_password(user_id, request.password)


@router.post("/{user_id}/disable", response_model=UserView, responses=problem_responses(404))
async def disable_user(user_id: int) -> UserView:
    return await users_api.disable_user(user_id, True)
