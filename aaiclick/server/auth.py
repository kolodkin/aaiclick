"""Auth for the REST surface and the ``/mcp`` mount.

In local mode auth is disabled: every request is allowed (a synthetic admin
principal) and startup logs a ``WARNING``. In distributed mode an
``Authorization: Bearer`` credential is required — either an access JWT or an
``aaic_`` API token; ``HTTPBearer`` (with ``auto_error=False``) extracts it and
registers the OpenAPI scheme. The ``/mcp`` mount keeps an ASGI middleware
because ``Depends`` does not propagate into mounted sub-apps. See
``docs/designs/auth.md``.

The scope rules are plain functions (``principal_to_scope``, ``check_scope``)
so the FastAPI dependencies here and the FastMCP middleware in ``mcp_rbac.py``
share one definition of each.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Literal, NamedTuple

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.auth import config, security, store
from aaiclick.auth.models import (
    ROLE_ADMIN,
    ROLE_SCOPES,
    SCOPE_ADMIN,
    SCOPE_WRITE,
    Role,
    ScopeLevel,
    scope_admits,
)
from aaiclick.internal_api.errors import Forbidden, Invalid, Unauthorized
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.view_models import ProblemCode

from .errors import BEARER_CHALLENGE, problem_response
from .request_state import audit_state

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
    role: Role
    """The user's installation-wide role — from the access JWT, or read live for an API token."""
    scope: ScopeLevel | None = None
    """API-token level; ``None`` means unscoped — a session or local mode, bounded by role alone."""
    kind: AuthKind = AUTH_KIND_SESSION


_SYNTHETIC_ADMIN = Principal(user_id=None, role=ROLE_ADMIN, kind=AUTH_KIND_NONE)


def _principal_from_token(token: str) -> Principal:
    """Decode a raw access JWT into a Principal, or raise ``Unauthorized``."""
    try:
        claims = security.decode_access_token(token, config.require_jwt_secret())
    except security.TokenError as exc:
        raise Unauthorized(str(exc)) from exc
    return Principal(user_id=claims.user_id, role=claims.role)


async def _principal_from_api_token(token: str) -> Principal:
    """Look an ``aaic_`` token up by hash and build a Principal from its owner's
    *current* role, so revocation and demotion bind instantly."""
    async with orch_context(with_ch=False):
        resolved = await store.resolve_api_token(security.sha256_hex(token))
    if resolved is None:
        raise Unauthorized("invalid api token")
    if resolved.user.disabled:
        raise Unauthorized("user is disabled")
    return Principal(
        user_id=resolved.user.id, role=resolved.user.role, scope=resolved.token.scope, kind=AUTH_KIND_TOKEN
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


async def require_principal(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> Principal:
    """FastAPI dependency → resolve the Principal or raise ``Unauthorized``.

    ``HTTPBearer`` already extracted and scheme-checked the credential, so the
    token is decoded directly — no header re-parsing. The principal is recorded
    on the request's audit carrier.

    This authenticates only. Scope is the route guard's call (``require_scope``
    below): the HTTP method says nothing about it — ``POST /viewer/query`` and a
    dashboard run are reads, and every read is open to a ``read`` token.
    """
    if not config.auth_enabled():
        principal = _SYNTHETIC_ADMIN
    elif creds is None or not creds.credentials:
        raise Unauthorized("missing bearer token")
    else:
        principal = await principal_from_credential(creds.credentials)
    audit_state(request.scope).principal = principal
    return principal


async def require_session(principal: Principal = Depends(require_principal)) -> Principal:
    """Guard for surfaces an API token must not reach (token and MFA
    management): a leaked token must not be able to mint itself a permanent
    foothold or reconfigure the second factor it bypasses."""
    if principal.kind == AUTH_KIND_TOKEN:
        raise Forbidden("api tokens cannot manage credentials — sign in with a session")
    return principal


async def require_user_id(principal: Principal = Depends(require_session)) -> int:
    """The current user's id — account surfaces need a real user row, which
    local mode's synthetic admin does not have (``422``)."""
    if principal.user_id is None:
        raise Invalid("auth is disabled — there is no current user")
    return principal.user_id


def principal_to_scope(principal: Principal) -> ScopeLevel:
    """The one bridge from a principal to the scope it holds. Every gate on
    both surfaces compares this against the level the route or tool declares —
    nothing else is consulted.

    A session resolves through its role via ``ROLE_SCOPES``. A token stands on
    its own scope like a GitHub PAT — the mint ceiling capped it, and demoting
    the owner does not shrink it; revoke it instead. Disabling the owner still
    stops it, which ``resolve_api_token`` checks.
    """
    if principal.scope is not None:
        return principal.scope
    return ROLE_SCOPES[principal.role]


def check_scope(principal: Principal, required: ScopeLevel) -> None:
    """The single authorization gate: ``Forbidden`` unless the principal's scope admits ``required``."""
    held = principal_to_scope(principal)
    if not scope_admits(held, required):
        raise Forbidden(f"'{required}' scope required — this request carries '{held}'")


def require_scope(required: ScopeLevel) -> Callable[..., Awaitable[Principal]]:
    """Route guard, declared by the level the route needs.

    Routes gate on scope, never on role: ``Depends(require_scope(SCOPE_ADMIN))``
    reads as the capability it protects, and one comparison covers sessions and
    tokens alike.
    """

    async def guard(principal: Principal = Depends(require_principal)) -> Principal:
        check_scope(principal, required)
        return principal

    return guard


require_write = require_scope(SCOPE_WRITE)
"""A member's own mutations — saved queries, dashboards."""
require_admin = require_scope(SCOPE_ADMIN)
"""Everything else that mutates or administers: jobs, objects, tasks, users, workers, audit."""


def warn_if_open() -> None:
    if not config.auth_enabled():
        logger.warning("local mode — auth is disabled, server is open")


class PrincipalAuthMiddleware:
    """ASGI guard for the ``/mcp`` mount: an API token, and only an API token,
    when auth is enabled.

    The surface is for unattended clients, an API token is their credential,
    and the restriction is what gives every MCP principal a real scope to gate
    on — a session JWT carries none. Per-tool RBAC happens inside FastMCP
    (``mcp_rbac.py``), which reads the principal this middleware records on the
    request.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Mounted at /mcp, so Starlette only ever routes http scopes here; the
        # app's lifespan runs at the root, not through the mount.
        authorization = Headers(scope=scope).get("authorization")
        try:
            if config.auth_enabled():
                scheme, credentials = get_authorization_scheme_param(authorization)
                if scheme.lower() != "bearer" or not credentials:
                    raise Unauthorized("missing bearer token")
                if not security.is_api_token(credentials):
                    raise Unauthorized("/mcp requires an API token, not a session")
            principal = await resolve_principal(authorization)
        except Unauthorized as exc:
            response = problem_response("Unauthorized", 401, str(exc), ProblemCode.UNAUTHORIZED, BEARER_CHALLENGE)
            await response(scope, receive, send)
            return
        audit_state(scope).principal = principal
        await self.app(scope, receive, send)
