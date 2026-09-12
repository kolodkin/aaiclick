"""Per-tool RBAC for the ``/mcp`` mount.

Each tool in ``server/mcp.py`` carries one tag — ``read``, ``write``,
``admin``, or ``superadmin`` — naming the level on the scope ladder it needs.
This FastMCP middleware resolves the caller from the current
HTTP request (the principal the mount middleware recorded, plus the
``X-Tenant-Id`` header), applies the same tenant / role / scope rules as the
REST dependencies (``server/auth.py``), pins the tenancy contextvar around the
call, and hides tools the caller may not invoke from ``tools/list``.
See ``docs/designs/auth.md`` — MCP Surface.

Without an HTTP request (in-process ``fastmcp.Client(mcp)``, stdio) there is
no credential to check and every tool is open, matching the CLI's trust model.
"""

from __future__ import annotations

from collections.abc import Sequence

import mcp.types as mt
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.base import Tool, ToolResult
from starlette.requests import Request

from aaiclick.auth.models import SCOPE_ADMIN, SCOPE_READ, SCOPE_SUPERADMIN, SCOPE_WRITE, ScopeLevel
from aaiclick.internal_api.errors import Forbidden, Invalid, Unauthorized
from aaiclick.tenancy import active_tenant

from .auth import (
    TENANT_HEADER,
    Principal,
    TenantContext,
    check_superadmin,
    check_tenant_admin,
    enforce_scope,
    resolve_principal,
    resolve_tenant,
)
from .request_state import audit_state

TAG_READ = "read"
TAG_WRITE = "write"
TAG_ADMIN = "admin"
TAG_SUPERADMIN = "superadmin"

_TAG_LEVELS: tuple[tuple[str, ScopeLevel], ...] = (
    (TAG_SUPERADMIN, SCOPE_SUPERADMIN),
    (TAG_ADMIN, SCOPE_ADMIN),
    (TAG_WRITE, SCOPE_WRITE),
    (TAG_READ, SCOPE_READ),
)


def required_level(tags: set[str]) -> ScopeLevel:
    """The level a tool's tag demands. The highest tag present wins, so a
    mistagged tool fails closed rather than open."""
    for tag, level in _TAG_LEVELS:
        if tag in tags:
            return level
    return SCOPE_SUPERADMIN


def authorize_tool(principal: Principal, tags: set[str], tenant_header: str | None) -> TenantContext | None:
    """Decide whether ``principal`` may call a tool with ``tags``.

    Returns the tenant to act in, or ``None`` for instance-level tools. Raises
    ``Forbidden`` / ``Invalid`` exactly like the REST guards.
    """
    required = required_level(tags)
    enforce_scope(principal, required)
    if required == SCOPE_SUPERADMIN:
        check_superadmin(principal)
        return None
    ctx = resolve_tenant(principal, tenant_header)
    if required == SCOPE_ADMIN:
        check_tenant_admin(ctx)
    return ctx


def _current_request() -> Request | None:
    try:
        return get_http_request()
    except RuntimeError:  # no HTTP transport: in-process client or stdio
        return None


async def _principal_for(request: Request) -> Principal:
    """The principal the mount middleware resolved, or resolve it here when the
    MCP app runs standalone (``mcp.run()`` without the FastAPI mount)."""
    stored = audit_state(request.scope).principal
    if stored is not None:
        return stored
    return await resolve_principal(request.headers.get("authorization"))


class McpRbacMiddleware(Middleware):
    async def on_list_tools(
        self,
        context: MiddlewareContext[mt.ListToolsRequest],
        call_next: CallNext[mt.ListToolsRequest, Sequence[Tool]],
    ) -> Sequence[Tool]:
        tools = await call_next(context)
        request = _current_request()
        if request is None:
            return tools
        principal = await _principal_for(request)
        tenant_header = request.headers.get(TENANT_HEADER)
        visible: list[Tool] = []
        for tool in tools:
            try:
                authorize_tool(principal, tool.tags, tenant_header)
            except Forbidden:
                continue
            except Invalid:
                # No tenant named yet (e.g. a superadmin listing without the
                # header): the tool is callable in principle, so keep it.
                pass
            visible.append(tool)
        return visible

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        request = _current_request()
        if request is None or context.fastmcp_context is None:
            return await call_next(context)
        tool_name = context.message.name
        audit = audit_state(request.scope)
        audit.action = tool_name
        tool = await context.fastmcp_context.fastmcp.get_tool(tool_name)
        if tool is None:
            raise ToolError(f"unknown tool {tool_name!r}")
        try:
            principal = await _principal_for(request)
            ctx = authorize_tool(principal, tool.tags, request.headers.get(TENANT_HEADER))
        except (Unauthorized, Forbidden, Invalid) as exc:
            raise ToolError(f"{tool_name}: {exc}") from exc
        if ctx is None:
            return await call_next(context)
        audit.tenant_id = ctx.tenant_id
        with active_tenant(ctx.tenant_id):
            return await call_next(context)
