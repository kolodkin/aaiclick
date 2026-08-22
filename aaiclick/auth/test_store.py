import pytest

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER


async def test_create_and_get_user(orch_ctx):
    created = await store.create_user(username="alice", password_hash="h", superadmin=True)
    fetched = await store.get_user_by_username("alice")
    assert fetched is not None and fetched.id == created.id


async def test_duplicate_username_raises(orch_ctx):
    await store.create_user(username="alice", password_hash="h")
    with pytest.raises(store.UsernameTaken):
        await store.create_user(username="alice", password_hash="h2")


async def test_set_superadmin_and_disable(orch_ctx):
    u = await store.create_user(username="bob", password_hash="h")
    await store.set_superadmin(u.id, True)
    await store.set_disabled(u.id, True)
    again = await store.get_user_by_id(u.id)
    assert again is not None
    assert again.superadmin is True and again.disabled is True


async def test_set_superadmin_missing_user_raises(orch_ctx):
    with pytest.raises(store.UserNotFound):
        await store.set_superadmin(999, True)


async def test_revoke_all_for_user_kills_active_tokens(orch_ctx):
    u = await store.create_user(username="dave", password_hash="h")
    await store.create_refresh_token(user_id=u.id, token_hash="d1", ttl=3600)
    await store.create_refresh_token(user_id=u.id, token_hash="d2", ttl=3600)

    assert await store.revoke_all_for_user(u.id) == 2
    assert await store.get_active_refresh("d1") is None
    assert await store.get_active_refresh("d2") is None


async def test_revoke_all_for_user_skips_rotated_and_other_users(orch_ctx):
    """Only this user's still-active rows are counted — already-rotated rows are
    inactive, and another user's session must survive."""
    u = await store.create_user(username="erin", password_hash="h")
    other = await store.create_user(username="frank", password_hash="h")
    rotated = await store.create_refresh_token(user_id=u.id, token_hash="e_rotated", ttl=3600)
    await store.rotate_refresh(rotated.id)
    await store.create_refresh_token(user_id=u.id, token_hash="e_active", ttl=3600)
    await store.create_refresh_token(user_id=other.id, token_hash="f_active", ttl=3600)

    assert await store.revoke_all_for_user(u.id) == 1
    assert await store.get_active_refresh("f_active") is not None


async def test_refresh_token_lifecycle(orch_ctx):
    u = await store.create_user(username="carol", password_hash="h")
    rt = await store.create_refresh_token(user_id=u.id, token_hash="hash1", ttl=3600)
    found = await store.get_active_refresh("hash1")
    assert found is not None and found.id == rt.id
    await store.rotate_refresh(rt.id)
    assert await store.get_active_refresh("hash1") is None  # rotated => inactive


async def test_create_and_get_tenant(orch_ctx):
    created = await store.create_tenant(slug="acme", name="Acme Corp")
    by_slug = await store.get_tenant_by_slug("acme")
    by_id = await store.get_tenant_by_id(created.id)
    assert by_slug is not None and by_slug.id == created.id
    assert by_id is not None and by_id.slug == "acme"


async def test_duplicate_slug_raises(orch_ctx):
    await store.create_tenant(slug="acme", name="Acme")
    with pytest.raises(store.SlugTaken):
        await store.create_tenant(slug="acme", name="Other")


async def test_list_user_tenants_joins_tenant_rows(orch_ctx):
    zeta = await store.create_tenant(slug="zeta", name="Z")
    alpha = await store.create_tenant(slug="alpha", name="A")
    u = await store.create_user(username="joins", password_hash="h")
    await store.set_membership(tenant_id=zeta.id, user_id=u.id, role=ROLE_VIEWER)
    await store.set_membership(tenant_id=alpha.id, user_id=u.id, role=ROLE_ADMIN)

    rows = await store.list_user_tenants(u.id)
    assert [(t.slug, m.role) for m, t in rows] == [("alpha", ROLE_ADMIN), ("zeta", ROLE_VIEWER)]


async def test_set_membership_creates_then_updates(orch_ctx):
    t = await store.create_tenant(slug="acme", name="Acme")
    u = await store.create_user(username="gail", password_hash="h")
    first = await store.set_membership(tenant_id=t.id, user_id=u.id, role=ROLE_VIEWER)
    second = await store.set_membership(tenant_id=t.id, user_id=u.id, role=ROLE_ADMIN)
    assert second.id == first.id and second.role == ROLE_ADMIN
    members = await store.list_tenant_members(t.id)
    assert [(user.username, m.role) for m, user in members] == [("gail", ROLE_ADMIN)]


async def test_remove_membership(orch_ctx):
    t = await store.create_tenant(slug="acme", name="Acme")
    u = await store.create_user(username="hank", password_hash="h")
    await store.set_membership(tenant_id=t.id, user_id=u.id, role=ROLE_VIEWER)
    assert await store.remove_membership(tenant_id=t.id, user_id=u.id) is True
    assert await store.remove_membership(tenant_id=t.id, user_id=u.id) is False
    assert await store.list_tenant_members(t.id) == []
