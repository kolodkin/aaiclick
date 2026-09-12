"""Internal API for user-minted API tokens. Callers own the tokens they
manage — the HTTP layer passes the principal's ``user_id``."""

from __future__ import annotations

from aaiclick.auth import security, store
from aaiclick.auth.models import (
    ROLE_ADMIN,
    SCOPE_ADMIN,
    SCOPE_SUPERADMIN,
    SCOPE_WRITE,
    ApiToken,
    ScopeLevel,
    scope_admits,
)
from aaiclick.auth.view_models import ApiTokenCreated, ApiTokenView, CreateApiTokenRequest
from aaiclick.datetime_utils import utc_now
from aaiclick.view_models import Page

from .errors import Invalid, NotFound


def _to_view(token: ApiToken) -> ApiTokenView:
    return ApiTokenView(
        id=token.id,
        name=token.name,
        prefix=token.prefix,
        scope=token.scope,
        tenant_id=token.tenant_id,
        expires_at=token.expires_at,
        last_used_at=token.last_used_at,
        revoked_at=token.revoked_at,
        created_at=token.created_at,
    )


async def _mint_ceiling(user_id: int, tenant_id: int | None) -> ScopeLevel:
    """The highest level this caller may mint. A superadmin is unbounded; anyone
    else is capped by their role in the tenant they named."""
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise NotFound(f"user {user_id} not found")
    if user.superadmin:
        return SCOPE_SUPERADMIN
    if tenant_id is None:
        raise Invalid("a tenant is required below superadmin scope")
    membership = await store.get_membership(tenant_id=tenant_id, user_id=user_id)
    if membership is None:
        # Missing, never forbidden — a caller must not be able to probe for
        # tenants. Membership is the only check: a tenant that does not exist
        # has no members either, and the default tenant is implicit (no row).
        raise NotFound(f"tenant {tenant_id} not found")
    return SCOPE_ADMIN if membership.role == ROLE_ADMIN else SCOPE_WRITE


async def create_token(user_id: int, request: CreateApiTokenRequest) -> ApiTokenCreated:
    """Mint a token for ``user_id``. The raw secret is in the response and nowhere else."""
    if request.expires_at is not None and request.expires_at <= utc_now():
        raise Invalid("expires_at must be in the future")
    ceiling = await _mint_ceiling(user_id, request.tenant_id)
    if not scope_admits(ceiling, request.scope):
        raise Invalid(f"cannot mint scope '{request.scope}' — your level here is '{ceiling}'")
    if request.scope != SCOPE_SUPERADMIN and request.tenant_id is None:
        raise Invalid("a tenant is required below superadmin scope")
    tenant_id = None if request.scope == SCOPE_SUPERADMIN else request.tenant_id
    secret = security.generate_api_token()
    row = await store.create_api_token(
        user_id=user_id,
        name=request.name,
        prefix=security.api_token_display_prefix(secret),
        token_hash=security.sha256_hex(secret),
        scope=request.scope,
        tenant_id=tenant_id,
        expires_at=request.expires_at,
    )
    return ApiTokenCreated(**_to_view(row).model_dump(), token=secret)


async def list_tokens(user_id: int) -> Page[ApiTokenView]:
    items = [_to_view(t) for t in await store.list_api_tokens(user_id)]
    return Page[ApiTokenView](items=items, total=len(items))


async def revoke_token(user_id: int, token_id: int) -> None:
    """Revoke one of the caller's tokens; another user's token reads as missing."""
    if not await store.revoke_api_token(token_id, user_id=user_id):
        raise NotFound(f"api token {token_id} not found")
