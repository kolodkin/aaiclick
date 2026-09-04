"""ASGI middleware writing one ``audit_log`` row per audited request.

Wraps the whole app: it sees the final status from ``http.response.start``
and, after the response is done, reads what the request left on
``scope["state"]`` — ``principal`` (from ``require_principal`` / the ``/mcp``
mount), ``tenant_id`` (from ``require_tenant`` / the MCP RBAC middleware),
``audit_action`` (the MCP tool name), and ``audit_username`` (the attempted
login name). Insert failures are logged, never raised: auditing must not turn
a served request into an error. See ``docs/designs/auth.md`` — Audit Log.
"""

from __future__ import annotations

import logging
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from aaiclick.audit import store
from aaiclick.auth import config
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.orch_context import orch_context

from .auth import AUTH_KIND_NONE, SAFE_METHODS, Principal

logger = logging.getLogger(__name__)

AUDITED_PREFIXES = ("/api/v0/", "/mcp")
UNAUDITED_PATHS = frozenset({"/api/v0/docs", "/api/v0/redoc", "/api/v0/openapi.json"})


def should_audit(policy: config.AuditPolicy, method: str, path: str, action: str | None) -> bool:
    if policy == config.AUDIT_OFF or path in UNAUDITED_PATHS or not path.startswith(AUDITED_PREFIXES):
        return False
    if policy == config.AUDIT_ALL:
        return True
    if path.startswith("/mcp"):
        return action is not None  # a tool call; not initialize / list / SSE
    return method.upper() not in SAFE_METHODS


class AuditMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        state: dict = scope.setdefault("state", {})
        started_at = utc_now()
        clock = time.monotonic()
        status = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = int((time.monotonic() - clock) * 1000)
            method, path = scope["method"], scope["path"]
            action = state.get("audit_action")
            if should_audit(config.audit_policy(), method, path, action):
                await self._record(scope, state, started_at, method, path, action, status, duration_ms)

    async def _record(self, scope, state, started_at, method, path, action, status, duration_ms) -> None:
        principal: Principal | None = state.get("principal")
        client = scope.get("client")
        try:
            async with orch_context(with_ch=False):
                await store.insert(
                    at=started_at,
                    user_id=principal.user_id if principal else None,
                    username=state.get("audit_username"),
                    auth_kind=principal.kind if principal else AUTH_KIND_NONE,
                    tenant_id=state.get("tenant_id"),
                    method=method,
                    path=path,
                    action=action,
                    status=status,
                    duration_ms=duration_ms,
                    client_ip=client[0] if client else None,
                )
        except Exception:  # noqa: BLE001 - auditing must never fail the request
            logger.exception("audit_log insert failed for %s %s", method, path)
