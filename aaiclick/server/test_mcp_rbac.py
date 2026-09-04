"""Per-tool RBAC on the MCP surface.

``authorize_tool`` carries the role matrix; the HTTP tests drive a stateless
FastMCP app through the ``/mcp`` mount middleware to prove the principal on
the ASGI scope reaches the FastMCP middleware and filters / gates tools.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest

from aaiclick.auth import security
from aaiclick.internal_api.errors import Forbidden, Invalid
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.tenancy import DEFAULT_TENANT_ID

from .auth import Principal, PrincipalAuthMiddleware
from .conftest import TEST_JWT_SECRET
from .mcp import mcp
from .mcp_rbac import TAG_READ, TAG_SUPERADMIN, TAG_WRITE, authorize_tool

MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def _principal(*, superadmin=False, tenants=None, scope="write"):
    return Principal(user_id=5, superadmin=superadmin, tenants=tenants or {}, scope=scope, kind="session")


# --- authorize_tool ------------------------------------------------------


@pytest.mark.parametrize(
    "principal, tags, header, expect",
    [
        pytest.param(_principal(tenants={7: "viewer"}), {TAG_READ}, "7", "ok", id="viewer-reads"),
        pytest.param(_principal(tenants={7: "viewer"}), {TAG_WRITE}, "7", Forbidden, id="viewer-cannot-write"),
        pytest.param(_principal(tenants={7: "admin"}), {TAG_WRITE}, "7", "ok", id="admin-writes"),
        pytest.param(_principal(tenants={7: "admin"}), {TAG_SUPERADMIN}, "7", Forbidden, id="admin-not-superadmin"),
        pytest.param(_principal(superadmin=True), {TAG_SUPERADMIN}, None, "ok", id="superadmin-no-tenant-needed"),
        pytest.param(_principal(superadmin=True), {TAG_WRITE}, None, Invalid, id="superadmin-must-name-tenant"),
        pytest.param(_principal(tenants={7: "admin"}), {TAG_READ}, "8", Forbidden, id="other-tenant"),
        pytest.param(_principal(tenants={7: "admin"}, scope="read"), {TAG_READ}, "7", "ok", id="read-token-reads"),
        pytest.param(
            _principal(tenants={7: "admin"}, scope="read"), {TAG_WRITE}, "7", Forbidden, id="read-token-no-write"
        ),
        pytest.param(
            _principal(superadmin=True, scope="read"), {TAG_SUPERADMIN}, None, Forbidden, id="read-token-no-super"
        ),
    ],
)
def test_authorize_tool_matrix(enabled, principal, tags, header, expect):
    if expect == "ok":
        ctx = authorize_tool(principal, tags, header)
        if TAG_SUPERADMIN in tags:
            assert ctx is None
        else:
            assert ctx is not None and ctx.tenant_id == int(header)
    else:
        with pytest.raises(expect):
            authorize_tool(principal, tags, header)


def test_authorize_tool_local_mode_uses_default_tenant():
    """Local mode's synthetic principal (kind "none") acts as admin of the
    default tenant without naming one — the same rule ``resolve_tenant`` applies
    to the REST routes."""
    synthetic = Principal(user_id=None, superadmin=True, tenants={}, kind="none")
    ctx = authorize_tool(synthetic, {TAG_WRITE}, None)
    assert ctx is not None and ctx.tenant_id == DEFAULT_TENANT_ID and ctx.role == "admin"
    assert authorize_tool(synthetic, {TAG_SUPERADMIN}, None) is None


async def test_every_tool_has_exactly_one_rbac_tag():
    tools = await mcp.list_tools(run_middleware=False)
    assert tools and all(len(t.tags & {TAG_READ, TAG_WRITE, TAG_SUPERADMIN}) == 1 for t in tools)


# --- through the HTTP mount ---------------------------------------------


@asynccontextmanager
async def _mcp_http() -> AsyncIterator[httpx.AsyncClient]:
    """The FastMCP app behind the mount middleware, stateless + JSON so a plain
    POST answers with a JSON-RPC body instead of an SSE stream.

    A context manager rather than a fixture: the session manager's lifespan
    opens an anyio task group, which must be exited in the task that entered
    it — pytest-asyncio tears async fixtures down in a different task.
    """
    http_app = mcp.http_app(path="/", stateless_http=True, json_response=True)
    async with http_app.lifespan(http_app):
        transport = httpx.ASGITransport(app=PrincipalAuthMiddleware(http_app))
        async with httpx.AsyncClient(transport=transport, base_url="http://mcp", headers=MCP_HEADERS) as client:
            yield client


def _token(*, superadmin=False, tenants=None) -> str:
    return security.encode_access_token(
        user_id=3, superadmin=superadmin, tenants=tenants or {}, secret=TEST_JWT_SECRET, ttl=60
    )


async def _rpc(client: httpx.AsyncClient, method: str, params: dict[str, Any], headers: dict[str, str]) -> Any:
    res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()


async def test_tools_list_is_filtered_by_role(orch_ctx, enabled):
    viewer = {"Authorization": f"Bearer {_token(tenants={DEFAULT_TENANT_ID: 'viewer'})}"}
    superadmin = {"Authorization": f"Bearer {_token(superadmin=True)}"}
    async with _mcp_http() as client:
        body = await _rpc(client, "tools/list", {}, viewer)
        names = {t["name"] for t in body["result"]["tools"]}
        assert "list_jobs" in names and "run_job" not in names and "setup" not in names

        body = await _rpc(client, "tools/list", {}, superadmin)
        names = {t["name"] for t in body["result"]["tools"]}
        assert {"list_jobs", "run_job", "setup"} <= names and len(names) == 24


async def test_viewer_can_read_but_not_write(orch_ctx, enabled):
    job = await create_job("mcp_rbac_job", simple_task)
    viewer = {"Authorization": f"Bearer {_token(tenants={DEFAULT_TENANT_ID: 'viewer'})}"}
    async with _mcp_http() as client:
        ok = await _rpc(client, "tools/call", {"name": "get_job", "arguments": {"ref": job.id}}, viewer)
        assert ok["result"]["structuredContent"]["name"] == "mcp_rbac_job"

        denied = await _rpc(client, "tools/call", {"name": "cancel_job", "arguments": {"ref": job.id}}, viewer)
        assert denied["result"]["isError"] is True
        assert "tenant admin" in denied["result"]["content"][0]["text"]


async def test_anonymous_gets_401_problem(orch_ctx, enabled):
    async with _mcp_http() as client:
        res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert res.status_code == 401 and res.json()["code"] == "unauthorized"
