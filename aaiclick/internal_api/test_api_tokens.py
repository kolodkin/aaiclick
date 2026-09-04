from datetime import timedelta

import pytest

from aaiclick.auth import security, store
from aaiclick.auth.view_models import ApiTokenCreated, CreateApiTokenRequest, CreateUserRequest
from aaiclick.datetime_utils import utc_now
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Invalid, NotFound


async def _user(username="alice"):
    return await users.create_user(CreateUserRequest(username=username, password="pw"))


async def test_create_returns_secret_once_and_list_hides_it(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci", scope="write"))
    assert isinstance(created, ApiTokenCreated)
    assert security.is_api_token(created.token)
    assert created.prefix == created.token[:12]

    page = await api_tokens.list_tokens(user.id)
    assert page.total == 1 and page.items[0].id == created.id
    assert not hasattr(page.items[0], "token")


async def test_created_token_resolves_by_hash(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci"))
    row = await store.get_active_api_token(security.sha256_hex(created.token))
    assert row is not None and row.user_id == user.id and row.scope == "read"


async def test_expiry_validated_and_revoke_deactivates(orch_ctx):
    user = await _user()
    with pytest.raises(Invalid):
        await api_tokens.create_token(
            user.id, CreateApiTokenRequest(name="old", expires_at=utc_now() - timedelta(seconds=1))
        )
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="soon", expires_at=utc_now() + timedelta(seconds=1))
    )
    assert await store.get_active_api_token(security.sha256_hex(created.token)) is not None
    await api_tokens.revoke_token(user.id, created.id)
    assert await store.get_active_api_token(security.sha256_hex(created.token)) is None


async def test_revoke_other_users_token_is_not_found(orch_ctx):
    alice = await _user("alice")
    bob = await _user("bob")
    created = await api_tokens.create_token(alice.id, CreateApiTokenRequest(name="ci"))
    with pytest.raises(NotFound):
        await api_tokens.revoke_token(bob.id, created.id)
    assert await store.get_active_api_token(security.sha256_hex(created.token)) is not None


async def test_resolve_stamps_last_used_once_per_window(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci"))
    token_hash = security.sha256_hex(created.token)

    first = await store.resolve_api_token(token_hash)
    assert first is not None and first.user.id == user.id and first.tenants == {}
    assert first.token.last_used_at is not None

    second = await store.resolve_api_token(token_hash)  # within the window: no second write
    assert second is not None and second.token.last_used_at == first.token.last_used_at
    assert await store.resolve_api_token("nope") is None
