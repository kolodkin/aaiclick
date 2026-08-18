import pytest

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER


async def test_create_and_get_user(orch_ctx):
    created = await store.create_user(username="alice", password_hash="h", role=ROLE_ADMIN)
    fetched = await store.get_user_by_username("alice")
    assert fetched is not None and fetched.id == created.id


async def test_duplicate_username_raises(orch_ctx):
    await store.create_user(username="alice", password_hash="h", role=ROLE_VIEWER)
    with pytest.raises(store.UsernameTaken):
        await store.create_user(username="alice", password_hash="h2", role=ROLE_VIEWER)


async def test_set_role_and_disable(orch_ctx):
    u = await store.create_user(username="bob", password_hash="h", role=ROLE_VIEWER)
    await store.set_role(u.id, ROLE_ADMIN)
    await store.set_disabled(u.id, True)
    again = await store.get_user_by_id(u.id)
    assert again is not None
    assert again.role == ROLE_ADMIN and again.disabled is True


async def test_set_role_missing_user_raises(orch_ctx):
    with pytest.raises(store.UserNotFound):
        await store.set_role(999, ROLE_ADMIN)


async def test_revoke_all_for_user_kills_active_tokens(orch_ctx):
    u = await store.create_user(username="dave", password_hash="h", role=ROLE_VIEWER)
    await store.create_refresh_token(user_id=u.id, token_hash="d1", ttl=3600)
    await store.create_refresh_token(user_id=u.id, token_hash="d2", ttl=3600)

    assert await store.revoke_all_for_user(u.id) == 2
    assert await store.get_active_refresh("d1") is None
    assert await store.get_active_refresh("d2") is None


async def test_revoke_all_for_user_skips_rotated_and_other_users(orch_ctx):
    """Only this user's still-active rows are counted — already-rotated rows are
    inactive, and another user's session must survive."""
    u = await store.create_user(username="erin", password_hash="h", role=ROLE_VIEWER)
    other = await store.create_user(username="frank", password_hash="h", role=ROLE_VIEWER)
    rotated = await store.create_refresh_token(user_id=u.id, token_hash="e_rotated", ttl=3600)
    await store.rotate_refresh(rotated.id)
    await store.create_refresh_token(user_id=u.id, token_hash="e_active", ttl=3600)
    await store.create_refresh_token(user_id=other.id, token_hash="f_active", ttl=3600)

    assert await store.revoke_all_for_user(u.id) == 1
    assert await store.get_active_refresh("f_active") is not None


async def test_refresh_token_lifecycle(orch_ctx):
    u = await store.create_user(username="carol", password_hash="h", role=ROLE_VIEWER)
    rt = await store.create_refresh_token(user_id=u.id, token_hash="hash1", ttl=3600)
    found = await store.get_active_refresh("hash1")
    assert found is not None and found.id == rt.id
    await store.rotate_refresh(rt.id)
    assert await store.get_active_refresh("hash1") is None  # rotated => inactive
