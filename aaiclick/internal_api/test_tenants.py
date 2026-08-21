import pytest

from aaiclick.auth import security, store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER
from aaiclick.auth.view_models import CreateTenantRequest, CreateUserRequest, LoginRequest, MemberView, TenantView
from aaiclick.internal_api import auth, tenants, users
from aaiclick.internal_api.errors import Conflict, NotFound
from aaiclick.view_models import Page

SECRET = "internal-api-tenants-test-secret-key-32-plus-bytes"


async def test_create_tenant_returns_view(orch_ctx):
    view = await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Acme Corp"))
    assert isinstance(view, TenantView)
    assert view.slug == "acme" and view.name == "Acme Corp"


async def test_create_duplicate_slug_raises_conflict(orch_ctx):
    await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    with pytest.raises(Conflict):
        await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Other"))


async def test_create_tenant_rejects_bad_slug(orch_ctx):
    with pytest.raises(ValueError):
        CreateTenantRequest(slug="Not A Slug!", name="Bad")


async def test_list_and_get_tenants(orch_ctx):
    created = await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    page = await tenants.list_tenants()
    assert isinstance(page, Page)
    assert any(t.slug == "acme" for t in page.items)
    assert (await tenants.get_tenant(created.id)).slug == "acme"
    with pytest.raises(NotFound):
        await tenants.get_tenant(123456)


async def test_member_lifecycle(orch_ctx):
    tenant = await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    user = await users.create_user(CreateUserRequest(username="ivy", password="pw"))

    member = await tenants.set_member(tenant.id, user.id, ROLE_VIEWER)
    assert isinstance(member, MemberView)
    assert member.username == "ivy" and member.role == ROLE_VIEWER

    promoted = await tenants.set_member(tenant.id, user.id, ROLE_ADMIN)
    assert promoted.role == ROLE_ADMIN

    page = await tenants.list_members(tenant.id)
    assert [m.user_id for m in page.items] == [user.id]

    await tenants.remove_member(tenant.id, user.id)
    assert (await tenants.list_members(tenant.id)).items == []


async def test_member_unknown_tenant_or_user_raises(orch_ctx):
    tenant = await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    user = await users.create_user(CreateUserRequest(username="jon", password="pw"))
    with pytest.raises(NotFound):
        await tenants.set_member(999, user.id, ROLE_VIEWER)
    with pytest.raises(NotFound):
        await tenants.set_member(tenant.id, 999, ROLE_VIEWER)
    with pytest.raises(NotFound):
        await tenants.list_members(999)
    with pytest.raises(NotFound):
        await tenants.remove_member(tenant.id, user.id)


async def test_membership_change_revokes_sessions(orch_ctx):
    """A revoked membership must not be outlived by a refresh token still
    minting the old membership map."""
    tenant = await tenants.create_tenant(CreateTenantRequest(slug="acme", name="Acme"))
    user = await users.create_user(CreateUserRequest(username="kai", password="pw"))
    pair = await auth.login(LoginRequest(username="kai", password="pw"), secret=SECRET)

    await tenants.set_member(tenant.id, user.id, ROLE_VIEWER)

    assert await store.get_active_refresh(security.sha256_hex(pair.refresh_token)) is None
