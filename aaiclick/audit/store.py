"""Raw DB access for ``audit_log``."""

from __future__ import annotations

from ..orchestration.orch_context import get_sql_session
from .models import AuditLog


async def insert(row: AuditLog) -> None:
    async with get_sql_session() as session:
        session.add(row)
        await session.commit()
