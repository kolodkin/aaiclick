"""Pydantic request/response models for the auth + users surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import Field

from ..api_model import ApiModel
from ..log_models import PageLimit, PageOffset, SnowflakeId, UtcDateTime
from .models import ROLE_VIEWER, SCOPE_READ, Role, ScopeLevel


class LoginRequest(ApiModel):
    username: str
    password: str
    totp_code: str | None = None
    """Required once the account has MFA enabled (``401 code="mfa_required"`` otherwise)."""


class RefreshRequest(ApiModel):
    refresh_token: str


class LogoutRequest(ApiModel):
    refresh_token: str


class TokenPair(ApiModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class MeView(ApiModel):
    """Current principal. ``id``/``username`` are ``None`` in local mode
    (auth disabled — the synthetic admin has no user row)."""

    id: SnowflakeId | None
    username: str | None
    role: Role
    mfa_enabled: bool = False


class UserView(ApiModel):
    id: SnowflakeId
    username: str
    role: Role
    disabled: bool
    email: str | None
    mfa_enabled: bool
    has_password: bool
    created_at: datetime


class CreateUserRequest(ApiModel):
    username: str = Field(min_length=1, max_length=100)
    password: str | None = None
    """``None`` creates a user who can only sign in after redeeming a reset link."""
    role: Role = ROLE_VIEWER
    email: str | None = None


class SetEmailRequest(ApiModel):
    email: str | None


class SetRoleRequest(ApiModel):
    role: Role


class SetPasswordRequest(ApiModel):
    password: str


class ChangePasswordRequest(ApiModel):
    """Self-service password change. ``current_password`` is required so a
    stolen access token cannot take over the account on its own."""

    current_password: str
    new_password: str


class UserListFilter(ApiModel):
    limit: PageLimit = 50
    offset: PageOffset = 0
    cursor: str | None = None


class ApiTokenView(ApiModel):
    """A token as listed — never carries the secret."""

    id: SnowflakeId
    name: str
    prefix: str
    scope: ScopeLevel
    expires_at: datetime | None
    last_used_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


class ApiTokenCreated(ApiTokenView):
    """Create response: the only time the raw ``token`` is ever returned."""

    token: str


class CreateApiTokenRequest(ApiModel):
    name: str = Field(min_length=1, max_length=100)
    scope: ScopeLevel = SCOPE_READ
    expires_at: UtcDateTime | None = None


class MfaSetupView(ApiModel):
    """A pending TOTP secret; MFA turns on only after ``/auth/me/mfa/enable``
    proves the authenticator has it."""

    secret: str
    otpauth_uri: str


class MfaEnableRequest(ApiModel):
    code: str


class MfaDisableRequest(ApiModel):
    """Both factors are needed to turn MFA off."""

    password: str
    code: str


class PasswordResetLinkView(ApiModel):
    """An admin-minted reset token. ``url`` is set when ``AAICLICK_PUBLIC_URL``
    is configured; either way the raw ``token`` appears here only."""

    token: str
    expires_at: datetime
    url: str | None


class InviteUserRequest(ApiModel):
    """Create a user who sets their own password by redeeming the link."""

    username: str = Field(min_length=1, max_length=100)
    role: Role = ROLE_VIEWER
    email: str | None = None


class InviteView(ApiModel):
    """The new user and the one-time link that lets them in."""

    user: UserView
    link: PasswordResetLinkView


class PasswordResetRedeem(ApiModel):
    token: str
    new_password: str = Field(min_length=1)
