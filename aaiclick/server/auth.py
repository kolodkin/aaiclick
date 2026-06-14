"""Auth for the REST surface and the ``/mcp`` mount.

When ``AAICLICK_AUTH_ENABLED`` is off, every request is allowed (a synthetic
admin principal) and startup logs a ``WARNING``. When on, the
``Authorization: Bearer`` access JWT is required; ``HTTPBearer`` (with
``auto_error=False``) extracts it and registers the OpenAPI scheme. The
``/mcp`` mount keeps an ASGI middleware (admin-only) because ``Depends`` does
not propagate into mounted sub-apps. See ``docs/auth.md``.
"""

from __future__ import annotations

import logging
from typing import NamedTuple

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.auth import config, security
from aaiclick.auth.models import ROLE_ADMIN, Role
from aaiclick.internal_api.errors import Forbidden, Unauthorized
from aaiclick.view_models import ProblemCode

from .errors import problem_response

BEARER_CHALLENGE = {"WWW-Authenticate": "Bearer"}
logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


class Principal(NamedTuple):
    user_id: int | None
    username: str | None
    role: Role


_SYNTHETIC_ADMIN = Principal(user_id=None, username=None, role=ROLE_ADMIN)


def resolve_principal(authorization: str | None) -> Principal:
    """Core principal resolution, shared by the dependency and the middleware."""
    if not config.auth_enabled():
        return _SYNTHETIC_ADMIN
    scheme, credentials = get_authorization_scheme_param(authorization)
    if scheme.lower() != "bearer" or not credentials:
        raise Unauthorized("missing bearer token")
    try:
        claims = security.decode_access_token(credentials, config.require_jwt_secret())
    except security.TokenError as exc:
        raise Unauthorized(str(exc)) from exc
    return Principal(user_id=claims.user_id, username=None, role=claims.role)


async def require_principal(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> Principal:
    """FastAPI dependency → resolve the Principal or raise ``Unauthorized``."""
    header = f"Bearer {creds.credentials}" if creds else None
    return resolve_principal(header)


async def require_admin(principal: Principal = Depends(require_principal)) -> Principal:
    if principal.role != ROLE_ADMIN:
        raise Forbidden("admin role required")
    return principal


def warn_if_open() -> None:
    if not config.auth_enabled():
        logger.warning("%s is off — server is open", config.ENV_ENABLED)


class AdminAuthMiddleware:
    """ASGI guard for the ``/mcp`` mount: admin-only when auth is enabled."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        authorization = Headers(scope=scope).get("authorization")
        try:
            principal = resolve_principal(authorization)
            if principal.role != ROLE_ADMIN:
                raise Forbidden("admin role required")
        except Unauthorized as exc:
            response = problem_response("Unauthorized", 401, str(exc), ProblemCode.UNAUTHORIZED, BEARER_CHALLENGE)
            await response(scope, receive, send)
            return
        except Forbidden as exc:
            response = problem_response("Forbidden", 403, str(exc), ProblemCode.FORBIDDEN)
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
