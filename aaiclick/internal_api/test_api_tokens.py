from datetime import timedelta

import pytest

from aaiclick.auth import security, store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_MEMBER, ROLE_VIEWER, SCOPE_ADMIN, SCOPE_READ, SCOPE_WRITE, Role
from aaiclick.auth.view_models import ApiTokenCreated, CreateApiTokenRequest, CreateUserRequest
from aaiclick.datetime_utils import utc_now
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Invalid, NotFound


async def _user(username: str = "alice", role: Role = ROLE_MEMBER):
    return await users.create_user(CreateUserRequest(username=username, password="pw", role=role))


async def test_create_returns_secret_once_and_list_hides_it(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci", scope="write"))
    assert isinstance(created, ApiTokenCreated)
    assert security.is_api_token(created.token)
    assert created.prefix == created.token[:12] and created.scope == "write"

    page = await api_tokens.list_tokens(user.id)
    assert page.total == 1 and page.items[0].id == created.id
    assert not hasattr(page.items[0], "token")


async def test_created_token_resolves_by_hash(orch_ctx):
    user = await _user()
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci"))
    resolved = await store.resolve_api_token(security.sha256_hex(created.token))
    assert resolved is not None and resolved.user.id == user.id and resolved.token.scope == SCOPE_READ


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


@pytest.mark.parametrize(
    "role, allowed, refused",
    [
        pytest.param(ROLE_VIEWER, SCOPE_READ, SCOPE_WRITE, id="viewer-mints-read-only"),
        pytest.param(ROLE_MEMBER, SCOPE_WRITE, SCOPE_ADMIN, id="member-mints-up-to-write"),
    ],
)
async def test_mint_ceiling_is_the_owners_role(orch_ctx, role, allowed, refused):
    """You delegate what you hold: the scope your own role resolves to."""
    user = await _user(role=role)
    ok = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ok", scope=allowed))
    assert ok.scope == allowed
    with pytest.raises(Invalid, match="cannot mint"):
        await api_tokens.create_token(user.id, CreateApiTokenRequest(name="no", scope=refused))


async def test_admin_mints_every_level(orch_ctx):
    admin = await _user(role=ROLE_ADMIN)
    minted = await api_tokens.create_token(admin.id, CreateApiTokenRequest(name="a", scope=SCOPE_ADMIN))
    assert minted.scope == SCOPE_ADMIN


async def test_unknown_user_is_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await api_tokens.create_token(999, CreateApiTokenRequest(name="no"))
