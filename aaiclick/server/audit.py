"""ASGI middleware writing one ``audit_log`` row per audited request.

Outermost on the app, so it sees the final status from ``http.response.start``
and, after the response is done, reads what the request left on its
``RequestAudit`` carrier (``request_state.py``). Insert failures are logged,
never raised: auditing must not turn a served request into an error.
See ``docs/designs/auth.md`` — Audit Log.

It also opens the request's SQL context: every ``orch_scope`` dependency,
API-token lookup, and the audit insert itself nest inside it and share one
engine instead of each building and disposing their own.
"""

from __future__ import annotations

import logging
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from aaiclick.audit import config, store
from aaiclick.audit.models import AuditLog
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.snowflake import get_snowflake_id

from .auth import AUTH_KIND_NONE, SAFE_METHODS
from .request_state import audit_state

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
    return method not in SAFE_METHODS


class AuditMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not scope["path"].startswith(AUDITED_PREFIXES):
            await self.app(scope, receive, send)
            return
        audit = audit_state(scope)
        started_at = utc_now()
        clock = time.monotonic()
        status = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        async with orch_context(with_ch=False):
            try:
                await self.app(scope, receive, send_wrapper)
            finally:
                method, path = scope["method"], scope["path"]
                if should_audit(config.audit_policy(), method, path, audit.action):
                    client = scope.get("client")
                    row = AuditLog(
                        id=get_snowflake_id(),
                        at=started_at,
                        user_id=audit.principal.user_id if audit.principal else None,
                        username=audit.username,
                        auth_kind=audit.principal.kind if audit.principal else AUTH_KIND_NONE,
                        tenant_id=audit.tenant_id,
                        method=method,
                        path=path,
                        action=audit.action,
                        status=status,
                        duration_ms=int((time.monotonic() - clock) * 1000),
                        client_ip=client[0] if client else None,
                    )
                    try:
                        await store.insert(row)
                    except Exception:  # noqa: BLE001 - auditing must never fail the request
                        logger.exception("audit_log insert failed for %s %s", method, path)
