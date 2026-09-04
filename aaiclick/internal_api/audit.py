"""Internal API over the audit log (superadmin-only at the HTTP layer)."""

from __future__ import annotations

from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import col

from aaiclick.audit.models import AuditLog
from aaiclick.audit.view_models import AuditEntryView, AuditListFilter
from aaiclick.view_models import Page

from .pagination import paginate


def _to_view(row: AuditLog) -> AuditEntryView:
    return AuditEntryView(
        id=row.id,
        at=row.at,
        user_id=row.user_id,
        username=row.username,
        auth_kind=row.auth_kind,
        tenant_id=row.tenant_id,
        method=row.method,
        path=row.path,
        action=row.action,
        status=row.status,
        duration_ms=row.duration_ms,
        client_ip=row.client_ip,
    )


async def list_audit(filter: AuditListFilter | None = None) -> Page[AuditEntryView]:
    """Newest first. ``path`` is a prefix filter, ``since`` is ``at >= since``."""
    filter = filter or AuditListFilter()
    predicates: list[ColumnElement[bool]] = []
    if filter.user_id is not None:
        predicates.append(col(AuditLog.user_id) == filter.user_id)
    if filter.username is not None:
        predicates.append(col(AuditLog.username) == filter.username)
    if filter.method is not None:
        predicates.append(col(AuditLog.method) == filter.method.upper())
    if filter.path is not None:
        predicates.append(col(AuditLog.path).startswith(filter.path))
    if filter.since is not None:
        predicates.append(col(AuditLog.at) >= filter.since)
    page = await paginate(
        AuditLog, where=predicates, order_by=col(AuditLog.id).desc(), limit=filter.limit, offset=filter.offset
    )
    return Page[AuditEntryView](items=[_to_view(r) for r in page.rows], total=page.total)
