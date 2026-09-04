"""Internal API for login/refresh/logout and MFA. Transport-agnostic; the
server router supplies the JWT secret from aaiclick.auth.config. The OIDC
flow lives in ``internal_api.oidc`` (needs ``httpx``, a ``server`` extra
dependency) and password reset in ``internal_api.password_reset``."""

from __future__ import annotations

import asyncio

from aaiclick.auth import config, security, store
from aaiclick.auth.models import User
from aaiclick.auth.view_models import (
    ChangePasswordRequest,
    LoginRequest,
    LogoutRequest,
    MfaDisableRequest,
    MfaEnableRequest,
    MfaSetupView,
    RefreshRequest,
    TenantRoleView,
    TokenPair,
)

from . import users
from .errors import Conflict, Invalid, MfaRequired, Unauthorized


async def _mint_pair(*, user: User, secret: str) -> TokenPair:
    access_ttl = config.access_ttl()
    refresh_secret = security.generate_secret()
    await store.create_refresh_token(
        user_id=user.id, token_hash=security.sha256_hex(refresh_secret), ttl=config.refresh_ttl()
    )
    tenants = await store.tenant_roles_for_user(user.id)
    return TokenPair(
        access_token=security.encode_access_token(
            user_id=user.id, superadmin=user.superadmin, tenants=tenants, secret=secret, ttl=access_ttl
        ),
        refresh_token=refresh_secret,
        expires_in=access_ttl,
    )


async def my_tenants(user_id: int) -> list[TenantRoleView]:
    """Resolve the user's memberships to tenant views for ``/auth/me``."""
    return [
        TenantRoleView(tenant_id=tenant.id, slug=tenant.slug, name=tenant.name, role=membership.role)
        for membership, tenant in await store.list_user_tenants(user_id)
    ]


async def _authenticates(user: User, password: str) -> bool:
    """Whether ``password`` admits this user — the one definition of the rule,
    so login, MFA disable, and the password change cannot drift apart. bcrypt
    runs on a worker thread so the event loop keeps serving."""
    if user.disabled or user.password_hash is None:  # SSO-only users have no password
        return False
    return await asyncio.to_thread(security.verify_password, password, user.password_hash)


async def login(request: LoginRequest, *, secret: str) -> TokenPair:
    user = await store.get_user_by_username(request.username)
    if user is None or not await _authenticates(user, request.password):
        raise Unauthorized("invalid username or password")
    if user.mfa_enabled and user.totp_secret is not None:
        if request.totp_code is None:
            raise MfaRequired("multi-factor code required")
        if not security.verify_totp(user.totp_secret, request.totp_code):
            raise Unauthorized("invalid multi-factor code")
    return await _mint_pair(user=user, secret=secret)


async def refresh(request: RefreshRequest, *, secret: str) -> TokenPair:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is None:
        raise Unauthorized("invalid refresh token")
    user = await store.get_user_by_id(row.user_id)
    if user is None or user.disabled:
        raise Unauthorized("user is disabled")
    await store.rotate_refresh(row.id)  # rotation: old token becomes inactive
    return await _mint_pair(user=user, secret=secret)


async def change_password(user_id: int, request: ChangePasswordRequest) -> None:
    """Change the caller's own password, then end all their sessions.

    Revoking is the point of the feature: someone changing their password
    because they suspect a leak needs the other party's refresh token dead. The
    caller's own client is logged out too and must sign in again.
    """
    user = await _require_current_user(user_id)
    if not await _authenticates(user, request.current_password):
        raise Unauthorized("invalid current password")
    # Delegate the write so "a password change revokes sessions" has one home.
    await users.set_password(user_id, request.new_password)


async def logout(request: LogoutRequest) -> None:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is not None:
        await store.revoke_refresh(row.id)


# --- MFA ------------------------------------------------------------------


async def _require_current_user(user_id: int) -> User:
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise Unauthorized("user not found")
    return user


async def mfa_setup(user_id: int) -> MfaSetupView:
    """Issue a fresh pending secret. Re-running replaces an unconfirmed one;
    an enabled account must disable MFA first so a stolen session cannot
    silently swap the authenticator."""
    user = await _require_current_user(user_id)
    if user.mfa_enabled:
        raise Conflict("MFA is already enabled — disable it before setting up a new authenticator")
    secret = security.generate_totp_secret()
    await store.set_totp(user.id, totp_secret=secret, mfa_enabled=False)
    return MfaSetupView(secret=secret, otpauth_uri=security.totp_uri(secret, user.username))


async def mfa_enable(user_id: int, request: MfaEnableRequest) -> None:
    """Confirm the pending secret with a live code, then end the user's other
    sessions so every open client re-authenticates with the second factor."""
    user = await _require_current_user(user_id)
    if user.totp_secret is None:
        raise Invalid("run MFA setup first")
    if user.mfa_enabled:
        raise Conflict("MFA is already enabled")
    if not security.verify_totp(user.totp_secret, request.code):
        raise Unauthorized("invalid multi-factor code")
    await store.set_totp(user.id, totp_secret=user.totp_secret, mfa_enabled=True)
    await store.revoke_all_for_user(user.id)


async def mfa_disable(user_id: int, request: MfaDisableRequest) -> None:
    user = await _require_current_user(user_id)
    if not user.mfa_enabled or user.totp_secret is None:
        raise Conflict("MFA is not enabled")
    if not await _authenticates(user, request.password) or not security.verify_totp(user.totp_secret, request.code):
        raise Unauthorized("invalid password or multi-factor code")
    await store.set_totp(user.id, totp_secret=None, mfa_enabled=False)
