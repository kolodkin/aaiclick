"""Auth for the REST surface and the ``/mcp`` mount.

In local mode auth is disabled: every request is allowed (a synthetic admin
principal) and startup logs a ``WARNING``. In distributed mode the
``Authorization: Bearer`` access JWT is required; ``HTTPBearer`` (with
``auto_error=False``) extracts it and registers the OpenAPI scheme. The
``/mcp`` mount keeps an ASGI middleware (admin-only) because ``Depends`` does
not propagate into mounted sub-apps. See ``docs/designs/auth.md``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import NamedTuple, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.auth import config, security
from aaiclick.auth.models import ROLE_ADMIN, Role
from aaiclick.internal_api.errors import Forbidden, Invalid, Unauthorized
from aaiclick.tenancy import DEFAULT_TENANT_ID, active_tenant
from aaiclick.view_models import ProblemCode

from .errors import problem_response

BEARER_CHALLENGE = {"WWW-Authenticate": "Bearer"}
TENANT_HEADER = "X-Tenant-Id"
logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


class Principal(NamedTuple):
    user_id: int | None
    username: str | None
    superadmin: bool
    tenants: dict[int, Role]
    """Membership map ``tenant_id -> role`` from the access JWT."""


_SYNTHETIC_ADMIN = Principal(user_id=None, username=None, superadmin=True, tenants={})


def _principal_from_token(token: str) -> Principal:
    """Decode a raw access JWT into a Principal, or raise ``Unauthorized``."""
    try:
        claims = security.decode_access_token(token, config.require_jwt_secret())
    except security.TokenError as exc:
        raise Unauthorized(str(exc)) from exc
    tenants = cast("dict[int, Role]", claims.tenants)
    return Principal(user_id=claims.user_id, username=None, superadmin=claims.superadmin, tenants=tenants)


def resolve_principal(authorization: str | None) -> Principal:
    """Resolve from a raw ``Authorization`` header value (used by the /mcp middleware,
    which has no access to FastAPI's dependency injection)."""
    if not config.auth_enabled():
        return _SYNTHETIC_ADMIN
    scheme, credentials = get_authorization_scheme_param(authorization)
    if scheme.lower() != "bearer" or not credentials:
        raise Unauthorized("missing bearer token")
    return _principal_from_token(credentials)


async def require_principal(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> Principal:
    """FastAPI dependency → resolve the Principal or raise ``Unauthorized``.

    ``HTTPBearer`` already extracted and scheme-checked the credential, so the
    token is decoded directly — no header re-parsing.
    """
    if not config.auth_enabled():
        return _SYNTHETIC_ADMIN
    if creds is None or not creds.credentials:
        raise Unauthorized("missing bearer token")
    return _principal_from_token(creds.credentials)


class TenantContext(NamedTuple):
    tenant_id: int
    role: Role


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
        role = principal.tenants.get(tenant_id)
        if role is None and principal.superadmin:
            role = ROLE_ADMIN
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


class AdminAuthMiddleware:
    """ASGI guard for the ``/mcp`` mount: superadmin-only when auth is enabled."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Mounted at /mcp, so Starlette only ever routes http scopes here; the
        # app's lifespan runs at the root, not through the mount.
        authorization = Headers(scope=scope).get("authorization")
        try:
            principal = resolve_principal(authorization)
            if not principal.superadmin:
                raise Forbidden("superadmin required")
        except Unauthorized as exc:
            response = problem_response("Unauthorized", 401, str(exc), ProblemCode.UNAUTHORIZED, BEARER_CHALLENGE)
            await response(scope, receive, send)
            return
        except Forbidden as exc:
            response = problem_response("Forbidden", 403, str(exc), ProblemCode.FORBIDDEN)
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
