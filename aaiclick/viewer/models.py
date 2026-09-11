"""SQLModel tables for saved viewer queries and dashboards.

``tenant_id`` is a plain ``BigInteger`` (no FK), and free-text config
(``cell_view`` YAML, ``fields`` / ``order_by`` / ``queries`` JSON) is stored
verbatim — the SPA interprets it; the server validates shape only. Same
conventions as ``aaiclick/orchestration/models.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from sqlalchemy import BigInteger, DateTime, String, Text, UniqueConstraint
from sqlmodel import Column, Field, SQLModel

from ..datetime_utils import utc_now
from ..tenancy import DEFAULT_TENANT_ID


class SavedQueryRow(SQLModel, table=True):
    """A saved object query: scope, object, filter, presentation, cell views."""

    __tablename__: ClassVar[str] = "viewer_queries"
    __table_args__ = (UniqueConstraint("tenant_id", "name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tenant_id: int = Field(
        default=DEFAULT_TENANT_ID,
        sa_column=Column(BigInteger, nullable=False, index=True, server_default=str(DEFAULT_TENANT_ID)),
    )
    name: str = Field(sa_column=Column(String, nullable=False, index=True))
    scope: str | None = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    object: str = Field(sa_column=Column(String, nullable=False, index=True))
    where: str | None = Field(default=None, sa_column=Column(Text, nullable=True))
    fields: str | None = Field(default=None, sa_column=Column(Text, nullable=True))  # JSON list
    order_by: str | None = Field(default=None, sa_column=Column(Text, nullable=True))  # JSON list of [name, dir]
    cell_view: str | None = Field(default=None, sa_column=Column(Text, nullable=True))  # raw YAML
    updated_at: datetime = Field(default_factory=utc_now, sa_column=Column(DateTime, nullable=False))


class DashboardRow(SQLModel, table=True):
    """An agent-authored HTML dashboard over named object queries."""

    __tablename__: ClassVar[str] = "viewer_dashboards"
    __table_args__ = (UniqueConstraint("tenant_id", "name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tenant_id: int = Field(
        default=DEFAULT_TENANT_ID,
        sa_column=Column(BigInteger, nullable=False, index=True, server_default=str(DEFAULT_TENANT_ID)),
    )
    name: str = Field(sa_column=Column(String, nullable=False, index=True))
    scope: str = Field(sa_column=Column(String, nullable=False))
    html: str = Field(sa_column=Column(Text, nullable=False))
    queries: str = Field(sa_column=Column(Text, nullable=False))  # JSON {panel: ObjectQuery}
    updated_at: datetime = Field(default_factory=utc_now, sa_column=Column(DateTime, nullable=False))
