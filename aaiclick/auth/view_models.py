"""Pydantic request/response models for the auth + users surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

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
