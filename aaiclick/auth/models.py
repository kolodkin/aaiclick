"""SQLModel tables for users, sessions, API tokens, and password resets.
See docs/designs/auth.md."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String
from sqlmodel import Field, SQLModel

from ..datetime_utils import utc_field, utc_now

ROLE_VIEWER = "viewer"
ROLE_MEMBER = "member"
ROLE_ADMIN = "admin"
Role = Literal["viewer", "member", "admin"]
"""Every rung of authority in an installation. ``admin`` runs it."""

ROLES: tuple[Role, ...] = (ROLE_VIEWER, ROLE_MEMBER, ROLE_ADMIN)

SCOPE_READ = "read"
SCOPE_WRITE = "write"
SCOPE_ADMIN = "admin"
ScopeLevel = Literal["read", "write", "admin"]
SCOPE_LEVELS: tuple[ScopeLevel, ...] = (SCOPE_READ, SCOPE_WRITE, SCOPE_ADMIN)
"""Ordered low to high — the index is the comparison in ``scope_admits``."""


def scope_admits(held: ScopeLevel, required: ScopeLevel) -> bool:
    """Whether a token holding ``held`` may perform a ``required``-level operation."""
    return SCOPE_LEVELS.index(held) >= SCOPE_LEVELS.index(required)


ROLE_SCOPES: dict[Role, ScopeLevel] = {
    ROLE_VIEWER: SCOPE_READ,
    ROLE_MEMBER: SCOPE_WRITE,
    ROLE_ADMIN: SCOPE_ADMIN,
}
"""The one bridge between the two vocabularies.

A role says *who someone is*; a scope says *how much a credential may do*.
Authorization compares scopes only — this map is where a role becomes one, so
there is a single ordered ladder to gate on rather than two.
"""


class User(SQLModel, table=True):
    __tablename__: ClassVar[str] = "users"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    username: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    password_hash: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    """``None`` for a user created without one — they can only sign in after a reset."""
    role: Role = Field(sa_column=Column(String, nullable=False, server_default=ROLE_VIEWER), default=ROLE_VIEWER)
    disabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    email: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    totp_secret: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    """Base32 TOTP seed; pending until ``mfa_enabled`` confirms it."""
    mfa_enabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    created_at: datetime = utc_field(default_factory=utc_now)


class RefreshToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "refresh_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime = utc_field()
    rotated_at: datetime | None = utc_field(default=None)
    revoked_at: datetime | None = utc_field(default=None)


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
    expires_at: datetime | None = utc_field(default=None)
    last_used_at: datetime | None = utc_field(default=None)
    revoked_at: datetime | None = utc_field(default=None)
    created_at: datetime = utc_field(default_factory=utc_now)


class PasswordResetToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "password_reset_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime = utc_field()
    consumed_at: datetime | None = utc_field(default=None)
    created_at: datetime = utc_field(default_factory=utc_now)
