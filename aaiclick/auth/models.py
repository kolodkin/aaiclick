"""SQLModel tables for users, sessions, API tokens, SSO state, and password
resets. See docs/designs/auth.md."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String, UniqueConstraint
from sqlmodel import Field, SQLModel

from ..datetime_utils import utc_now

ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"
Role = Literal["admin", "viewer"]
ROLES: tuple[Role, ...] = (ROLE_ADMIN, ROLE_VIEWER)

TOKEN_SCOPE_READ = "read"
TOKEN_SCOPE_WRITE = "write"
TokenScope = Literal["read", "write"]
TOKEN_SCOPES: tuple[TokenScope, ...] = (TOKEN_SCOPE_READ, TOKEN_SCOPE_WRITE)


class User(SQLModel, table=True):
    __tablename__: ClassVar[str] = "users"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    username: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    password_hash: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    """``None`` for SSO-provisioned users — they can never pass the password login."""
    superadmin: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    disabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    email: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    oidc_subject: str | None = Field(sa_column=Column(String, nullable=True, unique=True, index=True), default=None)
    """``"<issuer>|<sub>"`` once the user has signed in through OIDC."""
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
    role: Role = Field(sa_column=Column(String, nullable=False))
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
    scope: TokenScope = Field(sa_column=Column(String, nullable=False))
    expires_at: datetime | None = Field(default=None)
    last_used_at: datetime | None = Field(default=None)
    revoked_at: datetime | None = Field(default=None)
    created_at: datetime = Field(default_factory=utc_now)


class OidcState(SQLModel, table=True):
    """One in-flight SSO login: the PKCE verifier and nonce the callback must match."""

    __tablename__: ClassVar[str] = "oidc_states"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    state_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    nonce: str = Field(sa_column=Column(String, nullable=False))
    code_verifier: str = Field(sa_column=Column(String, nullable=False))
    expires_at: datetime
    consumed_at: datetime | None = Field(default=None)


class PasswordResetToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "password_reset_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime
    consumed_at: datetime | None = Field(default=None)
    created_at: datetime = Field(default_factory=utc_now)
