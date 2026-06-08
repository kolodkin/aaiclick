"""Shared bearer-token auth for the REST surface and the ``/mcp`` mount (v0).

One static token, read from ``AAICLICK_API_TOKEN`` per request so tests can
flip it with ``monkeypatch``. Unset → open-server mode (the check is a no-op
and ``warn_if_open`` logs a startup ``WARNING``). The CLI and the in-process
MCP client never go through this layer — authentication is an HTTP-transport
concern, not an internal-API one. See ``docs/api_server.md`` — Authentication.
"""

from __future__ import annotations

import hmac
import logging
import os

from fastapi import Header
from fastapi.responses import JSONResponse
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.internal_api.errors import Unauthorized
from aaiclick.view_models import Problem, ProblemCode

ENV_TOKEN = "AAICLICK_API_TOKEN"

logger = logging.getLogger(__name__)


def _configured_token() -> str | None:
    """The active bearer token, or ``None`` for open-server mode."""
    return os.environ.get(ENV_TOKEN)


def _rejection_detail(authorization: str | None) -> str | None:
    """Return a 401 detail string if the request must be rejected, else ``None``.

    ``None`` covers both open-server mode (no token configured) and a valid
    bearer token — in either case the request proceeds.
    """
    token = _configured_token()
    if token is None:
        return None
    if authorization is None or not authorization.startswith("Bearer "):
        return "missing bearer token"
    if not hmac.compare_digest(authorization.removeprefix("Bearer "), token):
        return "invalid bearer token"
    return None


async def require_bearer(authorization: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce the bearer token on ``/api/v0/*`` routes.

    Raises ``Unauthorized`` (→ 401 ``Problem`` with ``WWW-Authenticate: Bearer``
    via the registered exception handler) on a missing/invalid token.
    """
    detail = _rejection_detail(authorization)
    if detail is not None:
        raise Unauthorized(detail)


def warn_if_open() -> None:
    """Log a startup ``WARNING`` when no token is configured (open server)."""
    if _configured_token() is None:
        logger.warning("%s unset — server is open", ENV_TOKEN)


class BearerAuthMiddleware:
    """ASGI middleware applying the bearer check at the ``/mcp`` mount.

    ``Depends`` does not propagate into mounted sub-apps, so the FastMCP mount
    needs its own guard. Runs the same ``_rejection_detail`` check and emits the
    identical ``Problem`` envelope as the REST dependency before delegating.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        authorization = Headers(scope=scope).get("authorization")
        detail = _rejection_detail(authorization)
        if detail is not None:
            response = JSONResponse(
                status_code=401,
                content=Problem(
                    title="Unauthorized",
                    status=401,
                    detail=detail,
                    code=ProblemCode.UNAUTHORIZED,
                ).model_dump(mode="json"),
                headers={"WWW-Authenticate": "Bearer"},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)
