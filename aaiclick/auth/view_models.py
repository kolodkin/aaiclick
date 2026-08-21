"""Pydantic request/response models for the auth + users surface."""

from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, field_validator

from .models import Role


class LoginRequest(BaseModel):
    username: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


SLUG_RE = re.compile(r"^[a-z0-9_]+$")


class TenantView(BaseModel):
    id: int
    slug: str
    name: str
    created_at: datetime


class CreateTenantRequest(BaseModel):
    slug: str
    name: str

    @field_validator("slug")
    @classmethod
    def _validate_slug(cls, value: str) -> str:
        if not SLUG_RE.fullmatch(value):
            raise ValueError("slug must match [a-z0-9_]+")
        return value


class MemberView(BaseModel):
    user_id: int
    username: str
    role: Role


class SetMemberRequest(BaseModel):
    role: Role


class TenantRoleView(BaseModel):
    tenant_id: int
    slug: str
    name: str
    role: Role


class MeView(BaseModel):
    """Current principal. ``id``/``username`` are ``None`` in local mode
    (auth disabled — the synthetic superadmin has no user row)."""

    id: int | None
    username: str | None
    superadmin: bool
    tenants: list[TenantRoleView]


class UserView(BaseModel):
    id: int
    username: str
    superadmin: bool
    disabled: bool
    created_at: datetime


class CreateUserRequest(BaseModel):
    username: str
    password: str
    superadmin: bool = False


class SetSuperadminRequest(BaseModel):
    superadmin: bool


class SetPasswordRequest(BaseModel):
    password: str


class ChangePasswordRequest(BaseModel):
    """Self-service password change. ``current_password`` is required so a
    stolen access token cannot take over the account on its own."""

    current_password: str
    new_password: str


class UserListFilter(BaseModel):
    limit: int = 50
    offset: int = 0
    cursor: str | None = None
