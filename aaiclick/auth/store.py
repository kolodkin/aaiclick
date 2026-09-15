"""Raw DB access for users, refresh tokens, API tokens, and password-reset
tokens. Domain errors only; the internal_api layer maps these to
InternalApiError / Problem responses."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, NamedTuple, Protocol, TypeVar, cast

from sqlalchemy import CursorResult, update
from sqlmodel import SQLModel, col, select

from ..datetime_utils import utc_now
from ..orchestration.orch_context import get_sql_session
from ..snowflake import get_snowflake_id
from .models import (
    ROLE_VIEWER,
    ApiToken,
    PasswordResetToken,
    RefreshToken,
    Role,
    ScopeLevel,
    User,
)

API_TOKEN_LAST_USED_GRANULARITY = timedelta(seconds=60)
"""``last_used_at`` is refreshed at most this often — one write per minute per
token instead of one per request."""

RowT = TypeVar("RowT", bound=SQLModel)


async def _insert(row: RowT) -> RowT:
    """Add + commit. Sessions never expire on commit and the new rows carry no
    server defaults, so the object is complete without a refresh."""
    async with get_sql_session() as session:
        session.add(row)
        await session.commit()
    return row


class UsernameTaken(ValueError):
    """A user with this username already exists."""



class UserNotFound(ValueError):
    """No user matches the given id/username."""


class RefreshInvalid(ValueError):
    """Refresh token is missing, expired, rotated, or revoked."""


async def create_user(
    *,
    username: str,
    password_hash: str | None,
    role: Role = ROLE_VIEWER,
    email: str | None = None,
) -> User:
    user = User(
        id=get_snowflake_id(),
        username=username,
        password_hash=password_hash,
        role=role,
        email=email,
    )
    async with get_sql_session() as session:
        existing = await session.execute(select(User).where(User.username == username))
        if existing.scalar_one_or_none() is not None:
            raise UsernameTaken(f"username '{username}' already exists")
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


async def get_user_by_username(username: str) -> User | None:
    async with get_sql_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()


async def get_user_by_id(user_id: int) -> User | None:
    async with get_sql_session() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()


async def has_users() -> bool:
    """True if any user exists — for the first-run admin seed (no full count)."""
    async with get_sql_session() as session:
        result = await session.execute(select(User.id).limit(1))
        return result.first() is not None


async def set_role(user_id: int, role: Role) -> User:
    return await _update_user(user_id, role=role)


async def set_disabled(user_id: int, disabled: bool) -> User:
    return await _update_user(user_id, disabled=disabled)


async def set_password_hash(user_id: int, password_hash: str) -> User:
    return await _update_user(user_id, password_hash=password_hash)


async def set_email(user_id: int, email: str | None) -> User:
    return await _update_user(user_id, email=email)


async def set_totp(user_id: int, *, totp_secret: str | None, mfa_enabled: bool) -> User:
    return await _update_user(user_id, totp_secret=totp_secret, mfa_enabled=mfa_enabled)


async def _update_user(user_id: int, **fields) -> User:
    async with get_sql_session() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if user is None:
            raise UserNotFound(f"user {user_id} not found")
        for key, value in fields.items():
            setattr(user, key, value)
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


async def create_refresh_token(*, user_id: int, token_hash: str, ttl: int) -> RefreshToken:
    token = RefreshToken(
        id=get_snowflake_id(),
        user_id=user_id,
        token_hash=token_hash,
        expires_at=utc_now() + timedelta(seconds=ttl),
    )
    async with get_sql_session() as session:
        session.add(token)
        await session.commit()
        await session.refresh(token)
    return token


async def get_active_refresh(token_hash: str) -> RefreshToken | None:
    """Return the row only if it is unrotated, unrevoked, and unexpired."""
    async with get_sql_session() as session:
        row = (
            await session.execute(select(RefreshToken).where(RefreshToken.token_hash == token_hash))
        ).scalar_one_or_none()
    if row is None or row.rotated_at is not None or row.revoked_at is not None:
        return None
    if row.expires_at <= utc_now():
        return None
    return row


async def rotate_refresh(token_id: int) -> None:
    await _stamp_refresh(token_id, "rotated_at")


async def revoke_refresh(token_id: int) -> None:
    await _stamp_refresh(token_id, "revoked_at")


async def revoke_all_for_user(user_id: int) -> int:
    """Revoke every still-active refresh token for a user; returns the count.

    Ends the user's sessions at the refresh boundary — an already-issued access
    JWT stays valid until it expires, because it is verified by signature alone
    (see ``server/auth.py``). Used when a role change, disable, or password
    change should stop the user renewing.
    """
    async with get_sql_session() as session:
        # An UPDATE always yields a CursorResult; ``execute`` is just typed
        # for the general case, and ``rowcount`` is how the count comes free.
        result = cast(
            "CursorResult[Any]",
            await session.execute(
                update(RefreshToken)
                .where(
                    col(RefreshToken.user_id) == user_id,
                    col(RefreshToken.rotated_at).is_(None),
                    col(RefreshToken.revoked_at).is_(None),
                )
                .values(revoked_at=utc_now())
            ),
        )
        await session.commit()
    return result.rowcount


async def _stamp_refresh(token_id: int, field: str) -> None:
    async with get_sql_session() as session:
        row = (await session.execute(select(RefreshToken).where(RefreshToken.id == token_id))).scalar_one_or_none()
        if row is None:
            raise RefreshInvalid(f"refresh token {token_id} not found")
        setattr(row, field, utc_now())
        session.add(row)
        await session.commit()


# --- API tokens ---------------------------------------------------------


class ResolvedApiToken(NamedTuple):
    token: ApiToken
    user: User


async def create_api_token(
    *,
    user_id: int,
    name: str,
    prefix: str,
    token_hash: str,
    scope: ScopeLevel,
    expires_at: datetime | None,
) -> ApiToken:
    return await _insert(
        ApiToken(
            id=get_snowflake_id(),
            user_id=user_id,
            name=name,
            prefix=prefix,
            token_hash=token_hash,
            scope=scope,
            expires_at=expires_at,
        )
    )


async def list_api_tokens(user_id: int) -> list[ApiToken]:
    """Every token of a user, newest first — revoked ones included so the owner
    can see what was revoked when."""
    async with get_sql_session() as session:
        result = await session.execute(
            select(ApiToken).where(ApiToken.user_id == user_id).order_by(col(ApiToken.created_at).desc())
        )
        return list(result.scalars().all())


def _token_active(token: ApiToken, now: datetime) -> bool:
    return token.revoked_at is None and (token.expires_at is None or token.expires_at > now)


async def get_active_api_token(token_hash: str) -> ApiToken | None:
    """Return the row only if it is unrevoked and unexpired."""
    async with get_sql_session() as session:
        row = (await session.execute(select(ApiToken).where(ApiToken.token_hash == token_hash))).scalar_one_or_none()
    return row if row is not None and _token_active(row, utc_now()) else None


async def resolve_api_token(token_hash: str) -> ResolvedApiToken | None:
    """Everything a request needs to authenticate an API token, in one session:
    the active token and its owner. Also stamps ``last_used_at`` (throttled)
    without a further session."""
    now = utc_now()
    async with get_sql_session() as session:
        pair = (
            await session.execute(
                select(ApiToken, User)
                .join(User, col(User.id) == col(ApiToken.user_id))
                .where(ApiToken.token_hash == token_hash)
            )
        ).first()
        if pair is None:
            return None
        token, user = pair
        if not _token_active(token, now):
            return None
        if token.last_used_at is None or now - token.last_used_at >= API_TOKEN_LAST_USED_GRANULARITY:
            token.last_used_at = now
            session.add(token)
            await session.commit()
    return ResolvedApiToken(token=token, user=user)


async def revoke_api_token(token_id: int, *, user_id: int) -> bool:
    """Revoke a token owned by ``user_id``; False if no such active token exists.

    Scoping by owner in the query (rather than checking after a lookup) means a
    caller can neither revoke nor even confirm the existence of another user's
    token.
    """
    async with get_sql_session() as session:
        result = cast(
            "CursorResult[Any]",
            await session.execute(
                update(ApiToken)
                .where(
                    col(ApiToken.id) == token_id, col(ApiToken.user_id) == user_id, col(ApiToken.revoked_at).is_(None)
                )
                .values(revoked_at=utc_now())
            ),
        )
        await session.commit()
    return result.rowcount > 0


# --- Single-use tokens (password reset) ---------------------------------


class _SingleUse(Protocol):
    token_hash: str
    expires_at: datetime
    consumed_at: datetime | None


SingleUseT = TypeVar("SingleUseT", bound=_SingleUse)


async def _consume(model: type[SingleUseT], token_hash: str) -> SingleUseT | None:
    """Mark a single-use row consumed and return it; ``None`` if missing,
    expired, or already consumed."""
    async with get_sql_session() as session:
        row = (await session.execute(select(model).where(model.token_hash == token_hash))).scalar_one_or_none()
        if row is None or row.consumed_at is not None or row.expires_at <= utc_now():
            return None
        row.consumed_at = utc_now()
        session.add(row)
        await session.commit()
    return row


async def create_password_reset(*, user_id: int, token_hash: str, ttl: int) -> PasswordResetToken:
    return await _insert(
        PasswordResetToken(
            id=get_snowflake_id(), user_id=user_id, token_hash=token_hash, expires_at=utc_now() + timedelta(seconds=ttl)
        )
    )


async def consume_password_reset(token_hash: str) -> PasswordResetToken | None:
    return await _consume(PasswordResetToken, token_hash)
