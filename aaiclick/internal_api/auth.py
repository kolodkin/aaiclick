"""Internal API for login/refresh/logout and the OIDC login flow.
Transport-agnostic; the server router supplies the JWT secret from
aaiclick.auth.config."""

from __future__ import annotations

import logging

import httpx

from aaiclick.auth import config, mail, oidc, security, store
from aaiclick.auth.models import User
from aaiclick.auth.view_models import (
    ChangePasswordRequest,
    LoginRequest,
    LogoutRequest,
    MfaDisableRequest,
    MfaEnableRequest,
    MfaSetupView,
    OidcCallbackRequest,
    OidcConfigView,
    OidcStartView,
    PasswordResetRedeem,
    PasswordResetRequest,
    RefreshRequest,
    TenantRoleView,
    TokenPair,
)

from . import users
from .errors import Conflict, Invalid, MfaRequired, Unauthorized

logger = logging.getLogger(__name__)


async def _mint_pair(*, user: User, secret: str) -> TokenPair:
    access_ttl = config.access_ttl()
    refresh_secret = security.generate_secret()
    await store.create_refresh_token(
        user_id=user.id, token_hash=security.sha256_hex(refresh_secret), ttl=config.refresh_ttl()
    )
    memberships = await store.list_memberships_for_user(user.id)
    tenants = {m.tenant_id: m.role for m in memberships}
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


def _authenticates(user: User, password: str) -> bool:
    """Whether ``password`` admits this user — the one definition of the rule,
    so login and the self-service password change cannot drift apart."""
    if user.disabled or user.password_hash is None:  # SSO-only users have no password
        return False
    return security.verify_password(password, user.password_hash)


async def login(request: LoginRequest, *, secret: str) -> TokenPair:
    user = await store.get_user_by_username(request.username)
    if user is None or not _authenticates(user, request.password):
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


async def change_password(user_id: int | None, request: ChangePasswordRequest) -> None:
    """Change the caller's own password, then end all their sessions.

    Revoking is the point of the feature: someone changing their password
    because they suspect a leak needs the other party's refresh token dead. The
    caller's own client is logged out too and must sign in again.
    """
    if user_id is None:
        raise Invalid("auth is disabled — there is no current user to change a password for")
    user = await store.get_user_by_id(user_id)
    if user is None or not _authenticates(user, request.current_password):
        raise Unauthorized("invalid current password")
    # Delegate the write so "a password change revokes sessions" has one home.
    await users.set_password(user_id, request.new_password)


async def logout(request: LogoutRequest) -> None:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is not None:
        await store.revoke_refresh(row.id)


# --- MFA ------------------------------------------------------------------


async def _require_current_user(user_id: int | None) -> User:
    if user_id is None:
        raise Invalid("auth is disabled — there is no current user")
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise Unauthorized("user not found")
    return user


async def mfa_setup(user_id: int | None) -> MfaSetupView:
    """Issue a fresh pending secret. Re-running replaces an unconfirmed one;
    an enabled account must disable MFA first so a stolen session cannot
    silently swap the authenticator."""
    user = await _require_current_user(user_id)
    if user.mfa_enabled:
        raise Conflict("MFA is already enabled — disable it before setting up a new authenticator")
    secret = security.generate_totp_secret()
    await store.set_totp(user.id, totp_secret=secret, mfa_enabled=False)
    return MfaSetupView(secret=secret, otpauth_uri=security.totp_uri(secret, user.username))


async def mfa_enable(user_id: int | None, request: MfaEnableRequest) -> None:
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


async def mfa_disable(user_id: int | None, request: MfaDisableRequest) -> None:
    user = await _require_current_user(user_id)
    if not user.mfa_enabled or user.totp_secret is None:
        raise Conflict("MFA is not enabled")
    if not _authenticates(user, request.password) or not security.verify_totp(user.totp_secret, request.code):
        raise Unauthorized("invalid password or multi-factor code")
    await store.set_totp(user.id, totp_secret=None, mfa_enabled=False)


# --- Password reset -------------------------------------------------------


async def request_password_reset(request: PasswordResetRequest) -> None:
    """Self-service reset: mail a link when SMTP is configured and the user has
    an email. Silent otherwise — the caller learns nothing about the account.
    """
    settings = config.smtp_settings()
    user = await store.get_user_by_username(request.username)
    if user is None or user.disabled or user.email is None:
        logger.info("password reset requested for %r: no deliverable account", request.username)
        return
    if settings is None:
        logger.warning("password reset requested for %r but mail is not configured", request.username)
        return
    link = await users.create_password_reset(user.id)
    body = (
        f"A password reset was requested for your aaiclick account '{user.username}'.\n\n"
        f"Open this link to choose a new password (valid until {link.expires_at:%Y-%m-%d %H:%M} UTC):\n\n"
        f"{link.url or link.token}\n\n"
        "If you did not request this, ignore this message."
    )
    await mail.send_mail(settings, to=user.email, subject="aaiclick password reset", body=body)


async def redeem_password_reset(request: PasswordResetRedeem) -> None:
    """Consume a reset token and set the password; sessions are revoked like an
    admin reset."""
    row = await store.consume_password_reset(security.sha256_hex(request.token))
    if row is None:
        raise Unauthorized("unknown, expired, or already used reset token")
    await users.set_password(row.user_id, request.new_password)


# --- OIDC / SSO ---------------------------------------------------------


def oidc_config() -> OidcConfigView:
    settings = config.oidc_settings()
    if settings is None:
        return OidcConfigView(enabled=False, label=config.DEFAULT_OIDC_LABEL)
    return OidcConfigView(enabled=True, label=settings.label)


def _require_oidc() -> tuple[config.OidcSettings, str]:
    settings = config.oidc_settings()
    if settings is None:
        raise Invalid("OIDC login is not configured")
    return settings, config.require_public_url() + "/"


async def oidc_start() -> OidcStartView:
    """Begin an SSO login: persist state / nonce / PKCE verifier, return the
    provider URL for the browser to visit."""
    settings, redirect_uri = _require_oidc()
    state, nonce, pkce = oidc.generate_state(), oidc.generate_nonce(), oidc.generate_pkce()
    async with oidc.http_client() as client:
        try:
            metadata = await oidc.discover(client, settings.issuer)
        except (oidc.OidcError, httpx.HTTPError) as exc:
            raise Conflict(f"OIDC provider unavailable: {exc}") from exc
    await store.create_oidc_state(
        state_hash=security.sha256_hex(state), nonce=nonce, code_verifier=pkce.verifier, ttl=config.OIDC_STATE_TTL
    )
    return OidcStartView(
        authorization_url=oidc.authorization_url(
            metadata,
            client_id=settings.client_id,
            redirect_uri=redirect_uri,
            scopes=settings.scopes,
            state=state,
            nonce=nonce,
            pkce=pkce,
        )
    )


async def _resolve_oidc_user(settings: config.OidcSettings, identity: oidc.IdentityClaims) -> User:
    """Map validated claims to a user: linked subject → username match (link
    it) → auto-provision; anything else is ``Unauthorized``."""
    key = oidc.subject_key(settings.issuer, identity.subject)
    user = await store.get_user_by_oidc_subject(key)
    if user is None and identity.username:
        user = await store.get_user_by_username(identity.username)
        if user is not None:
            user = await store.set_oidc_subject(user.id, key)
    if user is None:
        if not settings.auto_provision or not identity.username:
            raise Unauthorized("no aaiclick user for this identity")
        user = await store.create_user(
            username=identity.username, password_hash=None, email=identity.email, oidc_subject=key
        )
        logger.info("provisioned user %r from OIDC subject", identity.username)
    if user.disabled:
        raise Unauthorized("user is disabled")
    return user


async def oidc_callback(request: OidcCallbackRequest, *, secret: str) -> TokenPair:
    """Complete an SSO login: consume the state, exchange the code, validate
    the ``id_token``, resolve the user, and mint the regular token pair."""
    settings, redirect_uri = _require_oidc()
    state = await store.consume_oidc_state(security.sha256_hex(request.state))
    if state is None:
        raise Unauthorized("unknown or expired login state")
    async with oidc.http_client() as client:
        try:
            metadata = await oidc.discover(client, settings.issuer)
            id_token = await oidc.exchange_code(
                client,
                metadata,
                code=request.code,
                redirect_uri=redirect_uri,
                client_id=settings.client_id,
                client_secret=settings.client_secret,
                code_verifier=state.code_verifier,
            )
            jwks = await oidc.fetch_jwks(client, metadata)
        except (oidc.OidcError, httpx.HTTPError) as exc:
            raise Unauthorized(f"SSO login failed: {exc}") from exc
    try:
        identity = oidc.validate_id_token(
            id_token,
            jwks,
            issuer=settings.issuer,
            client_id=settings.client_id,
            nonce=state.nonce,
            username_claim=settings.username_claim,
        )
    except oidc.OidcError as exc:
        raise Unauthorized(str(exc)) from exc
    user = await _resolve_oidc_user(settings, identity)
    return await _mint_pair(user=user, secret=secret)
