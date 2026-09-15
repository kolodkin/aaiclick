"""Per-tool RBAC for the ``/mcp`` mount.

Each tool in ``server/mcp.py`` carries one tag — ``read``, ``write`` or
``admin`` — naming the level on the scope ladder it needs. This FastMCP
middleware resolves the caller from the current HTTP request (the principal
the mount middleware recorded), applies the same scope rules as the REST
dependencies (``server/auth.py``), and hides tools the caller may not invoke
from ``tools/list``. See ``docs/designs/auth.md`` — MCP Surface.

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

from aaiclick.auth.models import SCOPE_ADMIN, SCOPE_LEVELS, SCOPE_READ, SCOPE_WRITE, ScopeLevel
from aaiclick.internal_api.errors import Forbidden, Unauthorized

from .auth import Principal, check_scope, resolve_principal
from .request_state import audit_state

# A tool's tag *is* the scope it needs; these are aliases so ``mcp.py`` reads in
# the tag vocabulary while the ladder stays defined in one place.
TAG_READ = SCOPE_READ
TAG_WRITE = SCOPE_WRITE
TAG_ADMIN = SCOPE_ADMIN


def required_level(tags: set[str]) -> ScopeLevel:
    """The level a tool's tag demands. The highest tag present wins, and an
    untagged tool falls back to the top rung, so a mistagged tool fails closed."""
    for level in reversed(SCOPE_LEVELS):
        if level in tags:
            return level
    return SCOPE_LEVELS[-1]


def authorize_tool(principal: Principal, tags: set[str]) -> None:
    """Decide whether ``principal`` may call a tool with ``tags``.

    Raises ``Forbidden`` exactly like the REST guards — the gate itself is the
    one the REST routes use, so a tool and its REST twin agree.
    """
    check_scope(principal, required_level(tags))


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


def _listable(principal: Principal, tool: Tool) -> bool:
    """Whether ``tools/list`` should show ``tool`` to this caller."""
    try:
        authorize_tool(principal, tool.tags)
    except Forbidden:
        return False
    return True


class McpRbacMiddleware(Middleware):
    """Two hooks, one gate.

    ``tools/list`` trims the catalogue to what the caller could invoke.
    ``tools/call`` authorizes the one tool being called — through
    ``authorize_tool``, the same scope gate the REST routes use.

    Both start by asking FastMCP for the current HTTP request. ``None`` means an
    in-process client or stdio: there is no credential to check and every tool
    is open, the CLI's trust model.
    """

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
        return [tool for tool in tools if _listable(principal, tool)]

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        request = _current_request()
        if request is None or context.fastmcp_context is None:
            return await call_next(context)
        tool_name = context.message.name
        # Named before the gate, so the audit row identifies the tool even when
        # the call is refused.
        audit_state(request.scope).action = tool_name
        tool = await context.fastmcp_context.fastmcp.get_tool(tool_name)
        if tool is None:
            raise ToolError(f"unknown tool {tool_name!r}")
        try:
            authorize_tool(await _principal_for(request), tool.tags)
        except (Unauthorized, Forbidden) as exc:
            # MCP has no per-call HTTP status; a refusal is a tool error.
            raise ToolError(f"{tool_name}: {exc}") from exc
        return await call_next(context)
