"""Audit-log routes (superadmin-only)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.audit.view_models import AuditEntryView, AuditListFilter
from aaiclick.internal_api import audit as audit_api
from aaiclick.view_models import Page

from ..auth import require_superadmin
from ..deps import orch_scope

router = APIRouter(
    prefix="/audit",
    tags=["audit"],
    dependencies=[Depends(orch_scope), Depends(require_superadmin)],
)


@router.get("", response_model=Page[AuditEntryView])
async def list_audit(filter: AuditListFilter = Depends()) -> Page[AuditEntryView]:
    return await audit_api.list_audit(filter)
