"""SQLModel table for viewer dashboards.

Free-text config (``queries`` / ``params`` JSON) is stored verbatim — the SPA interprets it; the server validates shape
only. Same conventions as ``aaiclick/orchestration/models.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from sqlalchemy import BigInteger, DateTime, String, Text, UniqueConstraint
from sqlmodel import Column, Field, SQLModel

from ..datetime_utils import utc_now


class DashboardRow(SQLModel, table=True):
    """An agent-authored HTML dashboard over named object queries."""

    __tablename__: ClassVar[str] = "viewer_dashboards"
    __table_args__ = (UniqueConstraint("name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    name: str = Field(sa_column=Column(String, nullable=False, index=True))
    scope: str = Field(sa_column=Column(String, nullable=False))
    html: str = Field(sa_column=Column(Text, nullable=False))
    queries: str = Field(sa_column=Column(Text, nullable=False))  # JSON {panel: ObjectQuery}
    params: str = Field(default="[]", sa_column=Column(Text, nullable=False, server_default="[]"))  # JSON list
    updated_at: datetime = Field(default_factory=utc_now, sa_column=Column(DateTime, nullable=False))
