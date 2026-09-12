"""Internal API for the OIDC / SSO login flow. Kept apart from
``internal_api.auth`` because it needs ``httpx`` (``server`` extra)."""

from __future__ import annotations

import logging

import httpx

from aaiclick.auth import config, oidc, security, store
from aaiclick.auth.models import User
from aaiclick.auth.view_models import OidcCallbackRequest, OidcConfigView, OidcStartView, TokenPair

from .auth import _mint_pair
from .errors import Conflict, Invalid, Unauthorized

logger = logging.getLogger(__name__)


def oidc_config() -> OidcConfigView:
    settings = config.oidc_settings()
    if settings is None:
        return OidcConfigView(enabled=False, label=config.DEFAULT_OIDC_LABEL)
    return OidcConfigView(enabled=True, label=settings.label)


def _require_oidc() -> config.OidcSettings:
    settings = config.oidc_settings()
    if settings is None:
        raise Invalid("OIDC login is not configured")
    return settings


async def oidc_start() -> OidcStartView:
    """Begin an SSO login: persist state / nonce / PKCE verifier, return the
    provider URL for the browser to visit."""
    settings, redirect_uri = _require_oidc(), config.spa_url()
    state, nonce, pkce = security.generate_secret(), security.generate_secret(), oidc.generate_pkce()
    async with oidc.http_client() as client:
        try:
            metadata = await oidc.discover(client, settings.issuer)
        except (oidc.OidcError, httpx.HTTPError) as exc:
            raise Conflict(f"OIDC provider unavailable: {exc}") from exc
    await store.create_oidc_state(
        token_hash=security.sha256_hex(state), nonce=nonce, code_verifier=pkce.verifier, ttl=config.OIDC_STATE_TTL
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
    settings, redirect_uri = _require_oidc(), config.spa_url()
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
