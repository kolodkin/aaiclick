"""``audit_log`` — one row per audited HTTP request. See docs/designs/auth.md — Audit Log."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from sqlalchemy import BigInteger, Column, Integer, String
from sqlmodel import Field, SQLModel

from ..datetime_utils import utc_now


class AuditLog(SQLModel, table=True):
    __tablename__: ClassVar[str] = "audit_log"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    at: datetime = Field(default_factory=utc_now, index=True)
    # Plain columns, not FKs: rows must outlive the user and tenant they name.
    user_id: int | None = Field(sa_column=Column(BigInteger, nullable=True, index=True), default=None)
    username: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    auth_kind: str = Field(sa_column=Column(String, nullable=False))
    tenant_id: int | None = Field(sa_column=Column(BigInteger, nullable=True), default=None)
    method: str = Field(sa_column=Column(String, nullable=False))
    path: str = Field(sa_column=Column(String, nullable=False, index=True))
    action: str | None = Field(sa_column=Column(String, nullable=True), default=None)
    """MCP tool name for ``/mcp`` calls."""
    status: int = Field(sa_column=Column(Integer, nullable=False))
    duration_ms: int = Field(sa_column=Column(Integer, nullable=False))
    client_ip: str | None = Field(sa_column=Column(String, nullable=True), default=None)
