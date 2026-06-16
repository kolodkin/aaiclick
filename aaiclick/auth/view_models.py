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


class MeView(BaseModel):
    id: int
    username: str
    role: Role


class UserView(BaseModel):
    id: int
    username: str
    role: Role
    disabled: bool
    created_at: datetime


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: Role = "viewer"


class SetRoleRequest(BaseModel):
    role: Role


class SetPasswordRequest(BaseModel):
    password: str


class UserListFilter(BaseModel):
    limit: int = 50
    offset: int = 0
    cursor: str | None = None
