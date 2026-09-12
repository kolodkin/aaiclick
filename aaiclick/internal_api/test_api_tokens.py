from datetime import timedelta

import pytest

from aaiclick.auth import security, store
from aaiclick.auth.models import (
    ROLE_ADMIN,
    ROLE_VIEWER,
    SCOPE_ADMIN,
    SCOPE_READ,
    SCOPE_SUPERADMIN,
    SCOPE_WRITE,
    Role,
)
from aaiclick.auth.view_models import ApiTokenCreated, CreateApiTokenRequest, CreateUserRequest
from aaiclick.datetime_utils import utc_now
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Invalid, NotFound

HOME_SLUG = "home"
"""A real tenant these tests mint into.

Not ``DEFAULT_TENANT_ID``: that constant is the data plane's fallback and has
no ``tenants`` row, so a membership naming it violates the
``tenant_memberships`` foreign key under Postgres.
"""


async def _home() -> int:
    tenant = await store.get_tenant_by_slug(HOME_SLUG)
    if tenant is None:
        tenant = await store.create_tenant(slug=HOME_SLUG, name="Home")
    return tenant.id


async def _user(username="alice"):
    """A plain member of the home tenant — enough to mint up to ``write``."""
    view = await users.create_user(CreateUserRequest(username=username, password="pw"))
    await store.set_membership(tenant_id=await _home(), user_id=view.id, role=ROLE_VIEWER)
    return view


async def _member(username: str, tenant_id: int, role: Role):
    view = await users.create_user(CreateUserRequest(username=username, password="pw"))
    await store.set_membership(tenant_id=tenant_id, user_id=view.id, role=role)
    return view


async def test_create_returns_secret_once_and_list_hides_it(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="ci", scope="write", tenant_id=await _home())
    )
    assert isinstance(created, ApiTokenCreated)
    assert security.is_api_token(created.token)
    assert created.prefix == created.token[:12]

    page = await api_tokens.list_tokens(user.id)
    assert page.total == 1 and page.items[0].id == created.id
    assert not hasattr(page.items[0], "token")


async def test_created_token_resolves_by_hash(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci", tenant_id=await _home()))
    row = await store.get_active_api_token(security.sha256_hex(created.token))
    assert row is not None and row.user_id == user.id and row.scope == "read"


async def test_expiry_validated_and_revoke_deactivates(orch_ctx):
    user = await _user()
    with pytest.raises(Invalid):
        await api_tokens.create_token(
            user.id,
            CreateApiTokenRequest(name="old", tenant_id=await _home(), expires_at=utc_now() - timedelta(seconds=1)),
        )
    created = await api_tokens.create_token(
        user.id,
        CreateApiTokenRequest(name="soon", tenant_id=await _home(), expires_at=utc_now() + timedelta(seconds=1)),
    )
    assert await store.get_active_api_token(security.sha256_hex(created.token)) is not None
    await api_tokens.revoke_token(user.id, created.id)
    assert await store.get_active_api_token(security.sha256_hex(created.token)) is None


async def test_revoke_other_users_token_is_not_found(orch_ctx):
    alice = await _user("alice")
    bob = await _user("bob")
    created = await api_tokens.create_token(alice.id, CreateApiTokenRequest(name="ci", tenant_id=await _home()))
    with pytest.raises(NotFound):
        await api_tokens.revoke_token(bob.id, created.id)
    assert await store.get_active_api_token(security.sha256_hex(created.token)) is not None


async def test_resolve_returns_the_live_role_in_the_tokens_tenant(orch_ctx):
    user = await _user()
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await store.set_membership(tenant_id=tenant.id, user_id=user.id, role=ROLE_ADMIN)
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    token_hash = security.sha256_hex(created.token)

    first = await store.resolve_api_token(token_hash)
    assert first is not None
    assert first.token.tenant_id == tenant.id and first.role == ROLE_ADMIN
    assert first.token.last_used_at is not None

    second = await store.resolve_api_token(token_hash)  # inside the window: no second write
    assert second is not None and second.token.last_used_at == first.token.last_used_at
    assert await store.resolve_api_token("nope") is None


async def test_resolve_role_is_none_once_membership_is_gone(orch_ctx):
    """The ceiling collapses the moment the owner loses the tenant."""
    user = await _user()
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await store.set_membership(tenant_id=tenant.id, user_id=user.id, role=ROLE_ADMIN)
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    await store.remove_membership(tenant_id=tenant.id, user_id=user.id)

    resolved = await store.resolve_api_token(security.sha256_hex(created.token))
    assert resolved is not None and resolved.role is None


async def test_member_may_mint_up_to_write(orch_ctx):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    viewer = await _member("v", tenant.id, ROLE_VIEWER)

    ok = await api_tokens.create_token(
        viewer.id, CreateApiTokenRequest(name="ok", scope=SCOPE_WRITE, tenant_id=tenant.id)
    )
    assert ok.scope == SCOPE_WRITE and ok.tenant_id == tenant.id

    with pytest.raises(Invalid, match="write"):
        await api_tokens.create_token(
            viewer.id, CreateApiTokenRequest(name="no", scope=SCOPE_ADMIN, tenant_id=tenant.id)
        )


async def test_tenant_admin_may_mint_admin_but_not_superadmin(orch_ctx):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    admin = await _member("a", tenant.id, ROLE_ADMIN)

    assert (
        await api_tokens.create_token(
            admin.id, CreateApiTokenRequest(name="ok", scope=SCOPE_ADMIN, tenant_id=tenant.id)
        )
    ).scope == SCOPE_ADMIN
    with pytest.raises(Invalid):
        await api_tokens.create_token(admin.id, CreateApiTokenRequest(name="no", scope=SCOPE_SUPERADMIN))


async def test_non_member_tenant_reads_as_missing(orch_ctx):
    """404, never 403 — a token must not be able to probe for tenants."""
    tenant = await store.create_tenant(slug="acme", name="Acme")
    stranger = await users.create_user(CreateUserRequest(username="s", password="pw"))
    with pytest.raises(NotFound):
        await api_tokens.create_token(
            stranger.id, CreateApiTokenRequest(name="no", scope=SCOPE_READ, tenant_id=tenant.id)
        )


async def test_superadmin_mints_anywhere_and_untenanted(orch_ctx):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    root = await users.create_user(CreateUserRequest(username="root", password="pw", superadmin=True))

    bound = await api_tokens.create_token(
        root.id, CreateApiTokenRequest(name="b", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    assert bound.tenant_id == tenant.id

    instance = await api_tokens.create_token(root.id, CreateApiTokenRequest(name="i", scope=SCOPE_SUPERADMIN))
    assert instance.tenant_id is None


async def test_tenant_required_below_superadmin(orch_ctx):
    user = await _user()
    with pytest.raises(Invalid, match="tenant"):
        await api_tokens.create_token(user.id, CreateApiTokenRequest(name="no", scope=SCOPE_READ))
