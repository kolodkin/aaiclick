"""SQLModel tables for users, sessions, API tokens, and password resets.
See docs/designs/auth.md."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String, UniqueConstraint
from sqlmodel import Field, SQLModel

from ..datetime_utils import utc_now

ROLE_VIEWER = "viewer"
ROLE_MEMBER = "member"
ROLE_ADMIN = "admin"
ROLE_SUPERADMIN = "superadmin"
Role = Literal["viewer", "member", "admin", "superadmin"]
"""Every rung of authority, including the instance-wide one."""

TenantRole = Literal["viewer", "member", "admin"]
"""What a ``tenant_memberships`` row may hold, and what the membership and
invite request models accept.

``superadmin`` is a property of the *user*, not of a membership: it is
instance-wide, so it names no tenant. Keeping it out of this type is what makes
the boundary reject it, rather than relying on the ``users.superadmin`` check
further down to make such a row harmless.
"""

TENANT_ROLES: tuple[TenantRole, ...] = (ROLE_VIEWER, ROLE_MEMBER, ROLE_ADMIN)

SCOPE_READ = "read"
SCOPE_WRITE = "write"
SCOPE_ADMIN = "admin"
SCOPE_SUPERADMIN = "superadmin"
ScopeLevel = Literal["read", "write", "admin", "superadmin"]
SCOPE_LEVELS: tuple[ScopeLevel, ...] = (SCOPE_READ, SCOPE_WRITE, SCOPE_ADMIN, SCOPE_SUPERADMIN)
"""Ordered low to high — the index is the comparison in ``scope_admits``."""


def scope_admits(held: ScopeLevel, required: ScopeLevel) -> bool:
    """Whether a token holding ``held`` may perform a ``required``-level operation."""
    return SCOPE_LEVELS.index(held) >= SCOPE_LEVELS.index(required)


ROLE_SCOPES: dict[Role, ScopeLevel] = {
    ROLE_VIEWER: SCOPE_READ,
    ROLE_MEMBER: SCOPE_WRITE,
    ROLE_ADMIN: SCOPE_ADMIN,
    ROLE_SUPERADMIN: SCOPE_SUPERADMIN,
}
"""The one bridge between the two vocabularies.

A role says *who someone is* in a tenant; a scope says *how much a credential
may do*. Authorization compares scopes only — this map is where a role becomes
one, so there is a single ordered ladder to gate on rather than two.
"""


class User(SQLModel, table=True):
    __tablename__: ClassVar[str] = "users"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    username: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    password_hash: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    """``None`` for a user created without one — they can only sign in after a reset."""
    superadmin: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    disabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    email: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    totp_secret: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    """Base32 TOTP seed; pending until ``mfa_enabled`` confirms it."""
    mfa_enabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    created_at: datetime = Field(default_factory=utc_now)


class Tenant(SQLModel, table=True):
    __tablename__: ClassVar[str] = "tenants"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    slug: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    name: str = Field(sa_column=Column(String, nullable=False))
    created_at: datetime = Field(default_factory=utc_now)


class TenantMembership(SQLModel, table=True):
    __tablename__: ClassVar[str] = "tenant_memberships"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tenant_id: int = Field(sa_column=Column(BigInteger, ForeignKey("tenants.id"), nullable=False, index=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    role: TenantRole = Field(sa_column=Column(String, nullable=False))
    created_at: datetime = Field(default_factory=utc_now)


class RefreshToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "refresh_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime
    rotated_at: datetime | None = Field(default=None)
    revoked_at: datetime | None = Field(default=None)


class ApiToken(SQLModel, table=True):
    """Long-lived bearer credential for unattended clients; only the hash is stored."""

    __tablename__: ClassVar[str] = "api_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    name: str = Field(sa_column=Column(String, nullable=False))
    prefix: str = Field(sa_column=Column(String, nullable=False))
    """Leading characters of the secret, so a user can tell tokens apart in a list."""
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    scope: ScopeLevel = Field(sa_column=Column(String, nullable=False))
    tenant_id: int | None = Field(sa_column=Column(BigInteger, nullable=True, index=True), default=None)
    """The tenant this token acts in; ``None`` only for ``superadmin`` scope.

    A plain column, not a DB FK — matching ``jobs`` and ``table_registry``,
    where the reference is enforced at the API boundary.
    """
    expires_at: datetime | None = Field(default=None)
    last_used_at: datetime | None = Field(default=None)
    revoked_at: datetime | None = Field(default=None)
    created_at: datetime = Field(default_factory=utc_now)


class PasswordResetToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "password_reset_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime
    consumed_at: datetime | None = Field(default=None)
    created_at: datetime = Field(default_factory=utc_now)
