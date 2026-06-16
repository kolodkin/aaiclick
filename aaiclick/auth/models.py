"""SQLModel tables for users and refresh tokens. See docs/auth.md."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal, get_args

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String
from sqlmodel import Field, SQLModel

from ..datetime_utils import utc_now
from ..orchestration.models import _enum_check

ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"
Role = Literal["admin", "viewer"]
ROLES: tuple[Role, ...] = (ROLE_ADMIN, ROLE_VIEWER)


class User(SQLModel, table=True):
    __tablename__: ClassVar[str] = "users"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    username: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    password_hash: str = Field(sa_column=Column(String, nullable=False))
    role: Role = Field(
        sa_column=Column(
            String,
            _enum_check("role", get_args(Role), "ck_users_role"),
            nullable=False,
        ),
    )
    disabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    created_at: datetime = Field(default_factory=utc_now)


class RefreshToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "refresh_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime
    rotated_at: datetime | None = Field(default=None)
    revoked_at: datetime | None = Field(default=None)
