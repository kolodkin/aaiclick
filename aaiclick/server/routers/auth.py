"""Auth routes: login, refresh, logout, me, and the caller's API tokens."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from aaiclick.auth import config
from aaiclick.auth.view_models import (
    ApiTokenCreated,
    ApiTokenView,
    ChangePasswordRequest,
    CreateApiTokenRequest,
    LoginRequest,
    LogoutRequest,
    MeView,
    MfaDisableRequest,
    MfaEnableRequest,
    MfaSetupView,
    OidcCallbackRequest,
    OidcConfigView,
    OidcStartView,
    PasswordResetRedeem,
    PasswordResetRequest,
    RefreshRequest,
    TokenPair,
)
from aaiclick.internal_api import api_tokens as api_tokens_api
from aaiclick.internal_api import auth as auth_api
from aaiclick.internal_api import oidc as oidc_api
from aaiclick.internal_api import password_reset as reset_api
from aaiclick.internal_api import users as users_api
from aaiclick.view_models import Page

from ..auth import Principal, require_principal, require_user_id
from ..deps import orch_scope
from ..errors import problem_responses
from ..request_state import audit_state

router = APIRouter(prefix="/auth", tags=["auth"], dependencies=[Depends(orch_scope)])


@router.post("/login", response_model=TokenPair, responses=problem_responses(401))
async def login(request: LoginRequest, http_request: Request) -> TokenPair:
    """``401 code="mfa_required"`` means the password was accepted but the
    account needs ``totp_code`` — retry with it."""
    # Attributes the attempt in the audit log whether or not it succeeds.
    audit_state(http_request.scope).username = request.username
    return await auth_api.login(request, secret=config.require_jwt_secret())


@router.post("/refresh", response_model=TokenPair, responses=problem_responses(401))
async def refresh(request: RefreshRequest) -> TokenPair:
    return await auth_api.refresh(request, secret=config.require_jwt_secret())


@router.post("/logout", status_code=204)
async def logout(request: LogoutRequest) -> None:
    await auth_api.logout(request)


@router.get("/me", response_model=MeView)
async def me(principal: Principal = Depends(require_principal)) -> MeView:
    if principal.user_id is None:  # local mode — synthetic superadmin, no user row
        return MeView(id=None, username=None, superadmin=principal.superadmin, tenants=[])
    user = await users_api.get_user(principal.user_id)
    return MeView(
        id=user.id,
        username=user.username,
        superadmin=principal.superadmin,
        mfa_enabled=user.mfa_enabled,
        tenants=await auth_api.my_tenants(principal.user_id),
    )


@router.put("/me/password", status_code=204, responses=problem_responses(401, 403, 422))
async def change_password(request: ChangePasswordRequest, user_id: int = Depends(require_user_id)) -> None:
    """Any role may change their own password — ``/users`` is admin-only."""
    await auth_api.change_password(user_id, request)


# --- Password reset -------------------------------------------------------
# Public: the caller has no session by definition.


@router.post("/password-reset/request", status_code=204)
async def request_password_reset(request: PasswordResetRequest) -> None:
    """Always ``204`` — whether a mail went out is not disclosed."""
    await reset_api.request(request)


@router.post("/password-reset", status_code=204, responses=problem_responses(401))
async def redeem_password_reset(request: PasswordResetRedeem) -> None:
    await reset_api.redeem(request)


# --- MFA ------------------------------------------------------------------
# Session-only: an API token bypasses MFA by design, so it must not be able
# to reconfigure it either.


@router.post("/me/mfa/setup", response_model=MfaSetupView, responses=problem_responses(403, 409, 422))
async def mfa_setup(user_id: int = Depends(require_user_id)) -> MfaSetupView:
    return await auth_api.mfa_setup(user_id)


@router.post("/me/mfa/enable", status_code=204, responses=problem_responses(401, 403, 409, 422))
async def mfa_enable(request: MfaEnableRequest, user_id: int = Depends(require_user_id)) -> None:
    await auth_api.mfa_enable(user_id, request)


@router.post("/me/mfa/disable", status_code=204, responses=problem_responses(401, 403, 409, 422))
async def mfa_disable(request: MfaDisableRequest, user_id: int = Depends(require_user_id)) -> None:
    await auth_api.mfa_disable(user_id, request)


# --- OIDC / SSO ---------------------------------------------------------
# Public: the browser has no session yet. ``/start`` is a POST because it
# writes a login-state row.


@router.get("/oidc/config", response_model=OidcConfigView)
async def oidc_config() -> OidcConfigView:
    return oidc_api.oidc_config()


@router.post("/oidc/start", response_model=OidcStartView, responses=problem_responses(409, 422))
async def oidc_start() -> OidcStartView:
    return await oidc_api.oidc_start()


@router.post("/oidc/callback", response_model=TokenPair, responses=problem_responses(401, 422))
async def oidc_callback(request: OidcCallbackRequest) -> TokenPair:
    return await oidc_api.oidc_callback(request, secret=config.require_jwt_secret())


# --- API tokens ---------------------------------------------------------
# Session-only (``require_user_id`` builds on ``require_session``): an API
# token cannot mint or revoke tokens. Local mode has no user row → 422.


@router.get("/tokens", response_model=Page[ApiTokenView], responses=problem_responses(403, 422))
async def list_tokens(user_id: int = Depends(require_user_id)) -> Page[ApiTokenView]:
    return await api_tokens_api.list_tokens(user_id)


@router.post("/tokens", response_model=ApiTokenCreated, status_code=201, responses=problem_responses(403, 422))
async def create_token(request: CreateApiTokenRequest, user_id: int = Depends(require_user_id)) -> ApiTokenCreated:
    """The raw ``token`` appears in this response and nowhere else."""
    return await api_tokens_api.create_token(user_id, request)


@router.delete("/tokens/{token_id}", status_code=204, responses=problem_responses(403, 404, 422))
async def revoke_token(token_id: int, user_id: int = Depends(require_user_id)) -> None:
    await api_tokens_api.revoke_token(user_id, token_id)
