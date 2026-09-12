from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from ..log_models import SnowflakeId


class AuditEntryView(BaseModel):
    id: SnowflakeId
    at: datetime
    user_id: SnowflakeId | None
    username: str | None
    auth_kind: str
    tenant_id: SnowflakeId | None
    method: str
    path: str
    action: str | None
    status: int
    duration_ms: int
    client_ip: str | None


class AuditListFilter(BaseModel):
    """Filters for ``internal_api.list_audit``; ``path`` is a prefix match."""

    user_id: int | None = None
    username: str | None = None
    method: str | None = None
    path: str | None = None
    since: datetime | None = None
    limit: int = 50
    offset: int = 0
