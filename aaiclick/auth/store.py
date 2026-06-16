"""Raw DB access for users and refresh tokens. Domain errors only; the
internal_api layer maps these to InternalApiError / Problem responses."""

from __future__ import annotations

from datetime import timedelta

from sqlmodel import select

from ..datetime_utils import utc_now
from ..orchestration.orch_context import get_sql_session
from ..snowflake import get_snowflake_id
from .models import RefreshToken, Role, User


class UsernameTaken(ValueError):
    """A user with this username already exists."""


class UserNotFound(ValueError):
    """No user matches the given id/username."""


class RefreshInvalid(ValueError):
    """Refresh token is missing, expired, rotated, or revoked."""


async def create_user(*, username: str, password_hash: str, role: Role) -> User:
    user = User(id=get_snowflake_id(), username=username, password_hash=password_hash, role=role)
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


async def _stamp_refresh(token_id: int, field: str) -> None:
    async with get_sql_session() as session:
        row = (await session.execute(select(RefreshToken).where(RefreshToken.id == token_id))).scalar_one_or_none()
        if row is None:
            raise RefreshInvalid(f"refresh token {token_id} not found")
        setattr(row, field, utc_now())
        session.add(row)
        await session.commit()
