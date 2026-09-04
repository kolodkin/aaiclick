"""Pydantic request/response models for the auth + users surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from ..log_models import SnowflakeId
from .models import Role, TokenScope


class LoginRequest(BaseModel):
    username: str
    password: str
    totp_code: str | None = None
    """Required once the account has MFA enabled (``401 code="mfa_required"`` otherwise)."""


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class TenantView(BaseModel):
    id: SnowflakeId
    slug: str
    name: str
    created_at: datetime


class CreateTenantRequest(BaseModel):
    # Pattern as a field constraint, not a validator, so it reaches the
    # generated OpenAPI schema the SPA types are built from.
    slug: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str


class MemberView(BaseModel):
    user_id: SnowflakeId
    username: str
    role: Role


class SetMemberRequest(BaseModel):
    role: Role


class TenantRoleView(BaseModel):
    tenant_id: SnowflakeId
    slug: str
    name: str
    role: Role


class MeView(BaseModel):
    """Current principal. ``id``/``username`` are ``None`` in local mode
    (auth disabled — the synthetic superadmin has no user row)."""

    id: SnowflakeId | None
    username: str | None
    superadmin: bool
    mfa_enabled: bool = False
    tenants: list[TenantRoleView]


class UserView(BaseModel):
    id: SnowflakeId
    username: str
    superadmin: bool
    disabled: bool
    email: str | None
    mfa_enabled: bool
    sso_linked: bool
    """The user has signed in through OIDC at least once."""
    has_password: bool
    created_at: datetime


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str | None = None
    """``None`` creates a user who can only sign in via SSO or a password-reset link."""
    superadmin: bool = False
    email: str | None = None


class SetEmailRequest(BaseModel):
    email: str | None


class SetSuperadminRequest(BaseModel):
    superadmin: bool


class SetPasswordRequest(BaseModel):
    password: str


class ChangePasswordRequest(BaseModel):
    """Self-service password change. ``current_password`` is required so a
    stolen access token cannot take over the account on its own."""

    current_password: str
    new_password: str


class UserListFilter(BaseModel):
    limit: int = 50
    offset: int = 0
    cursor: str | None = None


class TenantListFilter(BaseModel):
    limit: int = 50
    offset: int = 0
    cursor: str | None = None


class ApiTokenView(BaseModel):
    """A token as listed — never carries the secret."""

    id: SnowflakeId
    name: str
    prefix: str
    scope: TokenScope
    expires_at: datetime | None
    last_used_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


class ApiTokenCreated(ApiTokenView):
    """Create response: the only time the raw ``token`` is ever returned."""

    token: str


class CreateApiTokenRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    scope: TokenScope = "read"
    expires_at: datetime | None = None


class OidcConfigView(BaseModel):
    """Whether SSO is configured, so the login screen can offer the button."""

    enabled: bool
    label: str


class OidcStartView(BaseModel):
    authorization_url: str


class OidcCallbackRequest(BaseModel):
    code: str
    state: str


class MfaSetupView(BaseModel):
    """A pending TOTP secret; MFA turns on only after ``/auth/me/mfa/enable``
    proves the authenticator has it."""

    secret: str
    otpauth_uri: str


class MfaEnableRequest(BaseModel):
    code: str


class MfaDisableRequest(BaseModel):
    """Both factors are needed to turn MFA off."""

    password: str
    code: str
