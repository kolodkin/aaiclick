from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from ..log_models import PageLimit, PageOffset, SnowflakeId, UtcDateTime


class AuditEntryView(BaseModel):
    id: SnowflakeId
    at: datetime
    user_id: SnowflakeId | None
    username: str | None
    auth_kind: str
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
    since: UtcDateTime | None = None
    limit: PageLimit = 50
    offset: PageOffset = 0
