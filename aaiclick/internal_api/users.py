"""Internal API for user administration (admin-only at the HTTP layer)."""

from __future__ import annotations

from sqlmodel import col

from aaiclick.auth import config, security, store
from aaiclick.auth.models import User
from aaiclick.auth.view_models import CreateUserRequest, PasswordResetLinkView, UserListFilter, UserView
from aaiclick.view_models import Page

from .errors import Conflict, NotFound
from .pagination import paginate


def _to_view(user: User) -> UserView:
    return UserView(
        id=user.id,
        username=user.username,
        superadmin=user.superadmin,
        disabled=user.disabled,
        email=user.email,
        mfa_enabled=user.mfa_enabled,
        sso_linked=user.oidc_subject is not None,
        has_password=user.password_hash is not None,
        created_at=user.created_at,
    )


async def create_user(request: CreateUserRequest) -> UserView:
    try:
        user = await store.create_user(
            username=request.username,
            password_hash=security.hash_password(request.password) if request.password is not None else None,
            superadmin=request.superadmin,
            email=request.email,
        )
    except store.UsernameTaken as exc:
        raise Conflict(str(exc)) from exc
    return _to_view(user)


async def list_users(filter: UserListFilter | None = None) -> Page[UserView]:
    filter = filter or UserListFilter()
    page = await paginate(User, order_by=col(User.username).asc(), limit=filter.limit, offset=filter.offset)
    return Page[UserView](items=[_to_view(u) for u in page.rows], total=page.total)


async def get_user(user_id: int) -> UserView:
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise NotFound(f"user {user_id} not found")
    return _to_view(user)


async def set_superadmin(user_id: int, superadmin: bool) -> UserView:
    """Change a user's superadmin flag and end their sessions, so a demotion
    cannot be outlived by a refresh token still minting the old claims."""
    try:
        user = await store.set_superadmin(user_id, superadmin)
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc
    await store.revoke_all_for_user(user_id)
    return _to_view(user)


async def disable_user(user_id: int, disabled: bool = True) -> UserView:
    try:
        user = await store.set_disabled(user_id, disabled)
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc
    if disabled:
        await store.revoke_all_for_user(user_id)
    return _to_view(user)


async def set_password(user_id: int, password: str) -> UserView:
    """Admin password reset — also ends the user's sessions, so resetting a
    suspected-compromised account actually locks the other party out."""
    try:
        user = await store.set_password_hash(user_id, security.hash_password(password))
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc
    await store.revoke_all_for_user(user_id)
    return _to_view(user)


async def set_email(user_id: int, email: str | None) -> UserView:
    try:
        user = await store.set_email(user_id, email)
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc
    return _to_view(user)


async def reset_mfa(user_id: int) -> UserView:
    """Superadmin recovery for a lost authenticator: clear the secret and flag,
    and end the user's sessions so the account is re-verified on next login."""
    try:
        user = await store.set_totp(user_id, totp_secret=None, mfa_enabled=False)
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc
    await store.revoke_all_for_user(user_id)
    return _to_view(user)


def reset_url(token: str) -> str | None:
    """The SPA link that opens the new-password form, when the public URL is known."""
    base = config.public_url()
    return f"{base}/?p=reset%20{token}" if base else None


async def create_password_reset(user_id: int) -> PasswordResetLinkView:
    """Mint a one-time reset token for a user (superadmin / CLI recovery path)."""
    if await store.get_user_by_id(user_id) is None:
        raise NotFound(f"user {user_id} not found")
    token = security.generate_secret()
    row = await store.create_password_reset(
        user_id=user_id, token_hash=security.sha256_hex(token), ttl=config.password_reset_ttl()
    )
    return PasswordResetLinkView(token=token, expires_at=row.expires_at, url=reset_url(token))
