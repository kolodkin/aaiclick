"""Tenant administration routes.

Tenant CRUD is superadmin-only; membership routes also admit that tenant's
admins. The ``X-Tenant-Id`` header is not used here — the path names the
tenant explicitly.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth.models import ROLE_ADMIN
from aaiclick.auth.view_models import CreateTenantRequest, MemberView, SetMemberRequest, TenantView
from aaiclick.internal_api import tenants as tenants_api
from aaiclick.internal_api.errors import Forbidden, NotFound
from aaiclick.view_models import Page

from ..auth import Principal, require_principal, require_superadmin
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/tenants", tags=["tenants"], dependencies=[Depends(orch_scope)])


def _require_tenant_admin(principal: Principal, tenant_id: int) -> None:
    if principal.superadmin or principal.tenants.get(tenant_id) == ROLE_ADMIN:
        return
    raise Forbidden("tenant admin role required")


def _require_member(principal: Principal, tenant_id: int) -> None:
    """Non-members read a tenant as missing — no existence leak."""
    if principal.superadmin or tenant_id in principal.tenants:
        return
    raise NotFound(f"tenant {tenant_id} not found")


@router.get("", response_model=Page[TenantView], dependencies=[Depends(require_superadmin)])
async def list_tenants() -> Page[TenantView]:
    return await tenants_api.list_tenants()


@router.post(
    "",
    response_model=TenantView,
    status_code=201,
    responses=problem_responses(409),
    dependencies=[Depends(require_superadmin)],
)
async def create_tenant(request: CreateTenantRequest) -> TenantView:
    return await tenants_api.create_tenant(request)


@router.get("/{tenant_id}", response_model=TenantView, responses=problem_responses(404))
async def get_tenant(tenant_id: int, principal: Principal = Depends(require_principal)) -> TenantView:
    _require_member(principal, tenant_id)
    return await tenants_api.get_tenant(tenant_id)


@router.get("/{tenant_id}/members", response_model=Page[MemberView], responses=problem_responses(404))
async def list_members(tenant_id: int, principal: Principal = Depends(require_principal)) -> Page[MemberView]:
    _require_tenant_admin(principal, tenant_id)
    return await tenants_api.list_members(tenant_id)


@router.put(
    "/{tenant_id}/members/{user_id}",
    response_model=MemberView,
    responses=problem_responses(404),
)
async def set_member(
    tenant_id: int,
    user_id: int,
    request: SetMemberRequest,
    principal: Principal = Depends(require_principal),
) -> MemberView:
    _require_tenant_admin(principal, tenant_id)
    return await tenants_api.set_member(tenant_id, user_id, request.role)


@router.delete(
    "/{tenant_id}/members/{user_id}",
    status_code=204,
    responses=problem_responses(404),
)
async def remove_member(
    tenant_id: int,
    user_id: int,
    principal: Principal = Depends(require_principal),
) -> None:
    _require_tenant_admin(principal, tenant_id)
    await tenants_api.remove_member(tenant_id, user_id)
