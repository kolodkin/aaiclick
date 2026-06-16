"""Auth routes: login, refresh, logout, me."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth import config
from aaiclick.auth.view_models import (
    LoginRequest,
    LogoutRequest,
    MeView,
    RefreshRequest,
    TokenPair,
)
from aaiclick.internal_api import auth as auth_api

from ..auth import Principal, require_principal
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/auth", tags=["auth"], dependencies=[Depends(orch_scope)])


@router.post("/login", response_model=TokenPair, responses=problem_responses(401))
async def login(request: LoginRequest) -> TokenPair:
    return await auth_api.login(request, secret=config.require_jwt_secret())


@router.post("/refresh", response_model=TokenPair, responses=problem_responses(401))
async def refresh(request: RefreshRequest) -> TokenPair:
    return await auth_api.refresh(request, secret=config.require_jwt_secret())


@router.post("/logout", status_code=204)
async def logout(request: LogoutRequest) -> None:
    await auth_api.logout(request)


@router.get("/me", response_model=MeView)
async def me(principal: Principal = Depends(require_principal)) -> MeView:
    return MeView(
        id=principal.user_id or 0,
        username=principal.username or "admin",
        role=principal.role,
    )
