"""Raw DB access for users and refresh tokens. Domain errors only; the
internal_api layer maps these to InternalApiError / Problem responses."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

from sqlalchemy import CursorResult, update
from sqlmodel import col, select

from ..datetime_utils import utc_now
from ..orchestration.orch_context import get_sql_session
from ..snowflake import get_snowflake_id
from .models import RefreshToken, Role, Tenant, TenantMembership, User


class UsernameTaken(ValueError):
    """A user with this username already exists."""


class SlugTaken(ValueError):
    """A tenant with this slug already exists."""


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


async def create_tenant(*, slug: str, name: str, tenant_id: int | None = None) -> Tenant:
    """Create a tenant. ``tenant_id`` lets setup/migration seed the fixed
    default-tenant id; everyone else gets a snowflake."""
    tenant = Tenant(id=tenant_id if tenant_id is not None else get_snowflake_id(), slug=slug, name=name)
    async with get_sql_session() as session:
        existing = await session.execute(select(Tenant).where(Tenant.slug == slug))
        if existing.scalar_one_or_none() is not None:
            raise SlugTaken(f"tenant slug '{slug}' already exists")
        session.add(tenant)
        await session.commit()
        await session.refresh(tenant)
    return tenant


async def get_tenant_by_id(tenant_id: int) -> Tenant | None:
    async with get_sql_session() as session:
        result = await session.execute(select(Tenant).where(Tenant.id == tenant_id))
        return result.scalar_one_or_none()


async def get_tenant_by_slug(slug: str) -> Tenant | None:
    async with get_sql_session() as session:
        result = await session.execute(select(Tenant).where(Tenant.slug == slug))
        return result.scalar_one_or_none()


async def list_tenants() -> list[Tenant]:
    async with get_sql_session() as session:
        result = await session.execute(select(Tenant).order_by(col(Tenant.slug).asc()))
        return list(result.scalars().all())


async def set_membership(*, tenant_id: int, user_id: int, role: Role) -> TenantMembership:
    """Add a user to a tenant, or update their role if already a member."""
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(TenantMembership).where(
                    TenantMembership.tenant_id == tenant_id, TenantMembership.user_id == user_id
                )
            )
        ).scalar_one_or_none()
        row = existing if existing is not None else TenantMembership(
            id=get_snowflake_id(), tenant_id=tenant_id, user_id=user_id, role=role
        )
        row.role = role
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return row


async def remove_membership(*, tenant_id: int, user_id: int) -> bool:
    """Remove a user from a tenant; True if a row was deleted."""
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(TenantMembership).where(
                    TenantMembership.tenant_id == tenant_id, TenantMembership.user_id == user_id
                )
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        await session.delete(row)
        await session.commit()
    return True


async def list_memberships_for_user(user_id: int) -> list[TenantMembership]:
    async with get_sql_session() as session:
        result = await session.execute(select(TenantMembership).where(TenantMembership.user_id == user_id))
        return list(result.scalars().all())


async def list_memberships_for_tenant(tenant_id: int) -> list[TenantMembership]:
    async with get_sql_session() as session:
        result = await session.execute(select(TenantMembership).where(TenantMembership.tenant_id == tenant_id))
        return list(result.scalars().all())


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
