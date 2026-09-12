"""Per-request state shared across the auth dependencies, the ``/mcp``
middlewares, and the audit middleware.

Starlette's ``request.state`` is a view over ``scope["state"]``, so one typed
carrier stored there is visible to every layer of a request — the FastAPI
dependencies that resolve the principal and tenant, the FastMCP middleware
that knows the tool name, the login route that knows the attempted username,
and finally the audit middleware that writes the row.
"""

from __future__ import annotations

from dataclasses import dataclass

from starlette.types import Scope

# Module import (not ``from .auth import Principal``): ``auth`` imports this
# module, and with postponed annotations the type resolves lazily.
from . import auth as server_auth

_KEY = "aaiclick_audit"


@dataclass
class RequestAudit:
    principal: server_auth.Principal | None = None
    tenant_id: int | None = None
    action: str | None = None
    """MCP tool name for ``/mcp`` calls."""
    username: str | None = None
    """Attempted username on ``/auth/login`` — attributes failed logins too."""


def audit_state(scope: Scope) -> RequestAudit:
    """The request's carrier, created on first access."""
    state = scope.setdefault("state", {})
    carrier = state.get(_KEY)
    if carrier is None:
        carrier = state[_KEY] = RequestAudit()
    return carrier
