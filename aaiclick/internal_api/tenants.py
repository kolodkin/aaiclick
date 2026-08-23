"""Internal API for tenant and membership administration.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models. HTTP-layer
guards (superadmin / tenant admin) live in the router; these functions
assume the caller is authorized.
"""

from __future__ import annotations

from sqlmodel import col

from aaiclick.auth import store
from aaiclick.auth.models import Role, Tenant, TenantMembership, User
from aaiclick.auth.view_models import CreateTenantRequest, MemberView, TenantListFilter, TenantView
from aaiclick.view_models import Page

from .errors import Conflict, NotFound
from .pagination import paginate


def _to_view(tenant: Tenant) -> TenantView:
    return TenantView(id=tenant.id, slug=tenant.slug, name=tenant.name, created_at=tenant.created_at)


def _to_member_view(membership: TenantMembership, user: User) -> MemberView:
    return MemberView(user_id=user.id, username=user.username, role=membership.role)


async def create_tenant(request: CreateTenantRequest) -> TenantView:
    try:
        tenant = await store.create_tenant(slug=request.slug, name=request.name)
    except store.SlugTaken as exc:
        raise Conflict(str(exc)) from exc
    return _to_view(tenant)


async def list_tenants(filter: TenantListFilter | None = None) -> Page[TenantView]:
    filter = filter or TenantListFilter()
    page = await paginate(Tenant, order_by=col(Tenant.slug).asc(), limit=filter.limit, offset=filter.offset)
    return Page[TenantView](items=[_to_view(t) for t in page.rows], total=page.total)


async def get_tenant(tenant_id: int) -> TenantView:
    return _to_view(await _require_tenant_row(tenant_id))


async def _require_tenant_row(tenant_id: int) -> Tenant:
    tenant = await store.get_tenant_by_id(tenant_id)
    if tenant is None:
        raise NotFound(f"tenant {tenant_id} not found")
    return tenant


async def list_members(tenant_id: int) -> Page[MemberView]:
    await _require_tenant_row(tenant_id)
    rows = await store.list_tenant_members(tenant_id)
    items = [_to_member_view(membership, user) for membership, user in rows]
    return Page[MemberView](items=items, total=len(items))


async def set_member(tenant_id: int, user_id: int, role: Role) -> MemberView:
    """Add a member or change their role; ends the user's sessions so the
    membership change binds at the refresh boundary."""
    await _require_tenant_row(tenant_id)
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise NotFound(f"user {user_id} not found")
    membership = await store.set_membership(tenant_id=tenant_id, user_id=user_id, role=role)
    await store.revoke_all_for_user(user_id)
    return _to_member_view(membership, user)


async def remove_member(tenant_id: int, user_id: int) -> None:
    """Remove a member; ends the user's sessions (see ``set_member``)."""
    await _require_tenant_row(tenant_id)
    if not await store.remove_membership(tenant_id=tenant_id, user_id=user_id):
        raise NotFound(f"user {user_id} is not a member of tenant {tenant_id}")
    await store.revoke_all_for_user(user_id)
