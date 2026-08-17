"""Internal API for login/refresh/logout. Transport-agnostic; the server
router supplies the JWT secret from aaiclick.auth.config."""

from __future__ import annotations

from aaiclick.auth import config, security, store
from aaiclick.auth.view_models import (
    ChangePasswordRequest,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    TokenPair,
)

from .errors import Invalid, Unauthorized


async def _mint_pair(*, user_id: int, role: str, secret: str) -> TokenPair:
    access_ttl = config.access_ttl()
    refresh_secret = security.generate_secret()
    await store.create_refresh_token(
        user_id=user_id, token_hash=security.sha256_hex(refresh_secret), ttl=config.refresh_ttl()
    )
    return TokenPair(
        access_token=security.encode_access_token(user_id=user_id, role=role, secret=secret, ttl=access_ttl),
        refresh_token=refresh_secret,
        expires_in=access_ttl,
    )


async def login(request: LoginRequest, *, secret: str) -> TokenPair:
    user = await store.get_user_by_username(request.username)
    if user is None or user.disabled or not security.verify_password(request.password, user.password_hash):
        raise Unauthorized("invalid username or password")
    return await _mint_pair(user_id=user.id, role=user.role, secret=secret)


async def refresh(request: RefreshRequest, *, secret: str) -> TokenPair:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is None:
        raise Unauthorized("invalid refresh token")
    user = await store.get_user_by_id(row.user_id)
    if user is None or user.disabled:
        raise Unauthorized("user is disabled")
    await store.rotate_refresh(row.id)  # rotation: old token becomes inactive
    return await _mint_pair(user_id=user.id, role=user.role, secret=secret)


async def change_password(user_id: int | None, request: ChangePasswordRequest) -> None:
    """Change the caller's own password, then end all their sessions.

    Revoking is the point of the feature: someone changing their password
    because they suspect a leak needs the other party's refresh token dead. The
    caller's own client is logged out too and must sign in again.
    """
    if user_id is None:
        raise Invalid("auth is disabled — there is no current user to change a password for")
    user = await store.get_user_by_id(user_id)
    if user is None or user.disabled or not security.verify_password(request.current_password, user.password_hash):
        raise Unauthorized("invalid current password")
    await store.set_password_hash(user_id, security.hash_password(request.new_password))
    await store.revoke_all_for_user(user_id)


async def logout(request: LogoutRequest) -> None:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is not None:
        await store.revoke_refresh(row.id)
