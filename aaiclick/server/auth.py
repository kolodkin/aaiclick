"""Auth for the REST surface and the ``/mcp`` mount.

In local mode auth is disabled: every request is allowed (a synthetic admin
principal) and startup logs a ``WARNING``. In distributed mode an
``Authorization: Bearer`` credential is required — either an access JWT or an
``aaic_`` API token; ``HTTPBearer`` (with ``auto_error=False``) extracts it and
registers the OpenAPI scheme. The ``/mcp`` mount keeps an ASGI middleware
because ``Depends`` does not propagate into mounted sub-apps. See
``docs/designs/auth.md``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Literal, NamedTuple, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.auth import config, security, store
from aaiclick.auth.models import ROLE_ADMIN, TOKEN_SCOPE_READ, TOKEN_SCOPE_WRITE, Role, TokenScope
from aaiclick.internal_api.errors import Forbidden, Invalid, Unauthorized
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.tenancy import DEFAULT_TENANT_ID, active_tenant
from aaiclick.view_models import ProblemCode

from .errors import problem_response

BEARER_CHALLENGE = {"WWW-Authenticate": "Bearer"}
TENANT_HEADER = "X-Tenant-Id"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)

AUTH_KIND_NONE = "none"
AUTH_KIND_SESSION = "session"
AUTH_KIND_TOKEN = "token"
AuthKind = Literal["none", "session", "token"]
"""How the principal authenticated: local mode, an access JWT, or an API token."""


class Principal(NamedTuple):
    user_id: int | None
    superadmin: bool
    tenants: dict[int, Role]
    """Membership map ``tenant_id -> role`` — from the access JWT, or read live for an API token."""
    scope: TokenScope = TOKEN_SCOPE_WRITE
    """API-token scope; sessions always carry ``write``."""
    kind: AuthKind = AUTH_KIND_SESSION


_SYNTHETIC_ADMIN = Principal(user_id=None, superadmin=True, tenants={}, kind=AUTH_KIND_NONE)


def _principal_from_token(token: str) -> Principal:
    """Decode a raw access JWT into a Principal, or raise ``Unauthorized``."""
    try:
        claims = security.decode_access_token(token, config.require_jwt_secret())
    except security.TokenError as exc:
        raise Unauthorized(str(exc)) from exc
    tenants = cast("dict[int, Role]", claims.tenants)
    return Principal(user_id=claims.user_id, superadmin=claims.superadmin, tenants=tenants)


async def _principal_from_api_token(token: str) -> Principal:
    """Look an ``aaic_`` token up by hash and build a Principal from its owner's
    *current* flag and memberships, so revocation and demotion bind instantly."""
    async with orch_context(with_ch=False):
        row = await store.get_active_api_token(security.sha256_hex(token))
        if row is None:
            raise Unauthorized("invalid api token")
        user = await store.get_user_by_id(row.user_id)
        if user is None or user.disabled:
            raise Unauthorized("user is disabled")
        memberships = await store.list_memberships_for_user(user.id)
        await store.touch_api_token(row)
    tenants = cast("dict[int, Role]", {m.tenant_id: m.role for m in memberships})
    return Principal(
        user_id=user.id, superadmin=user.superadmin, tenants=tenants, scope=row.scope, kind=AUTH_KIND_TOKEN
    )


async def principal_from_credential(credential: str) -> Principal:
    """Resolve a bare bearer credential — API token by prefix, else access JWT."""
    if security.is_api_token(credential):
        return await _principal_from_api_token(credential)
    return _principal_from_token(credential)


async def resolve_principal(authorization: str | None) -> Principal:
    """Resolve from a raw ``Authorization`` header value (used by the /mcp middleware,
    which has no access to FastAPI's dependency injection)."""
    if not config.auth_enabled():
        return _SYNTHETIC_ADMIN
    scheme, credentials = get_authorization_scheme_param(authorization)
    if scheme.lower() != "bearer" or not credentials:
        raise Unauthorized("missing bearer token")
    return await principal_from_credential(credentials)


def enforce_scope(principal: Principal, method: str) -> None:
    """A ``read``-scoped token may only use safe HTTP methods."""
    if principal.scope == TOKEN_SCOPE_READ and method.upper() not in SAFE_METHODS:
        raise Forbidden(f"token scope 'read' cannot call {method.upper()}")


async def require_principal(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> Principal:
    """FastAPI dependency → resolve the Principal or raise ``Unauthorized``.

    ``HTTPBearer`` already extracted and scheme-checked the credential, so the
    token is decoded directly — no header re-parsing. The principal is stored
    on ``request.state`` for the audit middleware.
    """
    if not config.auth_enabled():
        principal = _SYNTHETIC_ADMIN
    elif creds is None or not creds.credentials:
        raise Unauthorized("missing bearer token")
    else:
        principal = await principal_from_credential(creds.credentials)
    enforce_scope(principal, request.method)
    request.state.principal = principal
    return principal


async def require_session(principal: Principal = Depends(require_principal)) -> Principal:
    """Guard for surfaces an API token must not reach (token management): a
    leaked token must not be able to mint itself a permanent foothold."""
    if principal.kind == AUTH_KIND_TOKEN:
        raise Forbidden("api tokens cannot manage tokens — sign in with a session")
    return principal


class TenantContext(NamedTuple):
    tenant_id: int
    role: Role


def role_in_tenant(principal: Principal, tenant_id: int) -> Role | None:
    """The principal's effective role in ``tenant_id``, or ``None`` if barred.

    The one place that encodes "a superadmin acts as tenant admin everywhere" —
    both the header-scoped routes and the path-scoped ``/tenants`` routes ask
    this rather than re-deriving it.
    """
    role = principal.tenants.get(tenant_id)
    if role is None and principal.superadmin:
        return ROLE_ADMIN
    return role


def resolve_tenant(principal: Principal, header_value: str | None) -> TenantContext:
    """Resolve the active tenant from the ``X-Tenant-Id`` header.

    A missing header is implied only when the principal has exactly one
    membership; superadmins (who can act in every tenant) must always name
    one. A tenant the principal cannot act in is ``Forbidden``.
    """
    if header_value is not None:
        try:
            tenant_id = int(header_value)
        except ValueError as exc:
            raise Invalid(f"{TENANT_HEADER} must be an integer") from exc
        role = role_in_tenant(principal, tenant_id)
        if role is None:
            raise Forbidden(f"no access to tenant {tenant_id}")
        return TenantContext(tenant_id=tenant_id, role=role)
    if len(principal.tenants) == 1:
        tenant_id, role = next(iter(principal.tenants.items()))
        return TenantContext(tenant_id=tenant_id, role=role)
    raise Invalid(f"{TENANT_HEADER} header required")


async def require_tenant(
    request: Request, principal: Principal = Depends(require_principal)
) -> AsyncIterator[TenantContext]:
    """Resolve the active tenant and pin the tenancy contextvar for the request."""
    if not config.auth_enabled():
        ctx = TenantContext(tenant_id=DEFAULT_TENANT_ID, role=ROLE_ADMIN)
    else:
        ctx = resolve_tenant(principal, request.headers.get(TENANT_HEADER))
    with active_tenant(ctx.tenant_id):
        yield ctx


async def require_admin(ctx: TenantContext = Depends(require_tenant)) -> TenantContext:
    """Tenant-admin guard for mutating tenant-scoped routes."""
    if ctx.role != ROLE_ADMIN:
        raise Forbidden("tenant admin role required")
    return ctx


async def require_superadmin(principal: Principal = Depends(require_principal)) -> Principal:
    if not principal.superadmin:
        raise Forbidden("superadmin required")
    return principal


def warn_if_open() -> None:
    if not config.auth_enabled():
        logger.warning("local mode — auth is disabled, server is open")


class PrincipalAuthMiddleware:
    """ASGI guard for the ``/mcp`` mount: any authenticated principal when auth
    is enabled. Per-tool RBAC happens inside FastMCP (``mcp_rbac.py``), which
    reads the principal this middleware stores on the scope."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Mounted at /mcp, so Starlette only ever routes http scopes here; the
        # app's lifespan runs at the root, not through the mount.
        authorization = Headers(scope=scope).get("authorization")
        try:
            principal = await resolve_principal(authorization)
        except Unauthorized as exc:
            response = problem_response("Unauthorized", 401, str(exc), ProblemCode.UNAUTHORIZED, BEARER_CHALLENGE)
            await response(scope, receive, send)
            return
        # ``request.state`` is backed by ``scope["state"]`` — the FastMCP
        # middleware and the audit middleware read it from there.
        scope.setdefault("state", {})["principal"] = principal
        await self.app(scope, receive, send)
