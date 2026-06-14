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


async def test_refresh_token_lifecycle(orch_ctx):
    u = await store.create_user(username="carol", password_hash="h", role=ROLE_VIEWER)
    rt = await store.create_refresh_token(user_id=u.id, token_hash="hash1", ttl=3600)
    found = await store.get_active_refresh("hash1")
    assert found is not None and found.id == rt.id
    await store.rotate_refresh(rt.id)
    assert await store.get_active_refresh("hash1") is None  # rotated => inactive
