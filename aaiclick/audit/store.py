"""Raw DB access for ``audit_log``."""

from __future__ import annotations

from datetime import datetime

from ..orchestration.orch_context import get_sql_session
from ..snowflake import get_snowflake_id
from .models import AuditLog


async def insert(
    *,
    at: datetime,
    user_id: int | None,
    username: str | None,
    auth_kind: str,
    tenant_id: int | None,
    method: str,
    path: str,
    action: str | None,
    status: int,
    duration_ms: int,
    client_ip: str | None,
) -> AuditLog:
    row = AuditLog(
        id=get_snowflake_id(),
        at=at,
        user_id=user_id,
        username=username,
        auth_kind=auth_kind,
        tenant_id=tenant_id,
        method=method,
        path=path,
        action=action,
        status=status,
        duration_ms=duration_ms,
        client_ip=client_ip,
    )
    async with get_sql_session() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return row
