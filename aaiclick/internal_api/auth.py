"""Internal API for login/refresh/logout. Transport-agnostic; the server
router supplies the JWT secret + TTLs from aaiclick.auth.config."""

from __future__ import annotations

from aaiclick.auth import config, security, store
from aaiclick.auth.view_models import LoginRequest, LogoutRequest, RefreshRequest, TokenPair

from .errors import Unauthorized


def _issue(*, user_id: int, role: str, secret: str, access_ttl: int) -> str:
    return security.encode_access_token(user_id=user_id, role=role, secret=secret, ttl=access_ttl)


async def _mint_pair(
    *, user_id: int, role: str, secret: str, access_ttl: int, refresh_ttl: int
) -> TokenPair:
    refresh_secret = security.generate_secret()
    await store.create_refresh_token(
        user_id=user_id, token_hash=security.sha256_hex(refresh_secret), ttl=refresh_ttl
    )
    return TokenPair(
        access_token=_issue(user_id=user_id, role=role, secret=secret, access_ttl=access_ttl),
        refresh_token=refresh_secret,
        expires_in=access_ttl,
    )


async def login(
    request: LoginRequest,
    *,
    secret: str,
    access_ttl: int | None = None,
    refresh_ttl: int | None = None,
) -> TokenPair:
    access_ttl = access_ttl if access_ttl is not None else config.access_ttl()
    refresh_ttl = refresh_ttl if refresh_ttl is not None else config.refresh_ttl()
    user = await store.get_user_by_username(request.username)
    if (
        user is None
        or user.disabled
        or not security.verify_password(request.password, user.password_hash)
    ):
        raise Unauthorized("invalid username or password")
    return await _mint_pair(
        user_id=user.id,
        role=user.role,
        secret=secret,
        access_ttl=access_ttl,
        refresh_ttl=refresh_ttl,
    )


async def refresh(
    request: RefreshRequest,
    *,
    secret: str,
    access_ttl: int | None = None,
    refresh_ttl: int | None = None,
) -> TokenPair:
    access_ttl = access_ttl if access_ttl is not None else config.access_ttl()
    refresh_ttl = refresh_ttl if refresh_ttl is not None else config.refresh_ttl()
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is None:
        raise Unauthorized("invalid refresh token")
    user = await store.get_user_by_id(row.user_id)
    if user is None or user.disabled:
        raise Unauthorized("user is disabled")
    await store.rotate_refresh(row.id)  # rotation: old token becomes inactive
    return await _mint_pair(
        user_id=user.id,
        role=user.role,
        secret=secret,
        access_ttl=access_ttl,
        refresh_ttl=refresh_ttl,
    )


async def logout(request: LogoutRequest) -> None:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is not None:
        await store.revoke_refresh(row.id)
