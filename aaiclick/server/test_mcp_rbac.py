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

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER, SCOPE_SUPERADMIN, SCOPE_WRITE
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Forbidden, Invalid
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.tenancy import DEFAULT_TENANT_ID, active_tenant

from .auth import Principal, PrincipalAuthMiddleware
from .mcp import mcp
from .mcp_rbac import TAG_ADMIN, TAG_READ, TAG_SUPERADMIN, TAG_WRITE, authorize_tool

MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def _principal(*, superadmin=False, tenants=None, scope="superadmin"):
    return Principal(user_id=5, superadmin=superadmin, tenants=tenants or {}, scope=scope, kind="token")


# --- authorize_tool ------------------------------------------------------


@pytest.mark.parametrize(
    "principal, tags, header, expect",
    [
        pytest.param(_principal(tenants={7: "viewer"}, scope="read"), {TAG_READ}, "7", "ok", id="read-token-reads"),
        pytest.param(
            _principal(tenants={7: "viewer"}, scope="read"), {TAG_WRITE}, "7", Forbidden, id="read-token-no-write"
        ),
        pytest.param(_principal(tenants={7: "viewer"}, scope="write"), {TAG_WRITE}, "7", "ok", id="member-writes"),
        pytest.param(
            _principal(tenants={7: "viewer"}, scope="write"), {TAG_ADMIN}, "7", Forbidden, id="write-token-no-admin"
        ),
        pytest.param(_principal(tenants={7: "admin"}, scope="admin"), {TAG_ADMIN}, "7", "ok", id="admin-token-admins"),
        pytest.param(
            _principal(tenants={7: "viewer"}, scope="admin"), {TAG_ADMIN}, "7", Forbidden, id="ceiling-caps-below-token"
        ),
        pytest.param(
            _principal(superadmin=True, scope="superadmin"), {TAG_SUPERADMIN}, None, "ok", id="superadmin-instance"
        ),
        pytest.param(
            _principal(tenants={7: "admin"}, scope="admin"), {TAG_SUPERADMIN}, "7", Forbidden, id="admin-not-superadmin"
        ),
        pytest.param(
            _principal(superadmin=True, scope="superadmin"),
            {TAG_WRITE},
            None,
            Invalid,
            id="superadmin-must-name-tenant",
        ),
        pytest.param(_principal(tenants={7: "admin"}, scope="admin"), {TAG_READ}, "8", Forbidden, id="other-tenant"),
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
    ctx = authorize_tool(synthetic, {TAG_ADMIN}, None)
    assert ctx is not None and ctx.tenant_id == DEFAULT_TENANT_ID and ctx.role == "admin"
    assert authorize_tool(synthetic, {TAG_SUPERADMIN}, None) is None


async def test_every_tool_has_exactly_one_rbac_tag():
    tools = await mcp.list_tools(run_middleware=False)
    assert tools and all(len(t.tags & {TAG_READ, TAG_WRITE, TAG_ADMIN, TAG_SUPERADMIN}) == 1 for t in tools)


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


HOME_SLUG = "home"
"""A real tenant the HTTP tests bind tokens to.

Not ``DEFAULT_TENANT_ID``: that constant is the data plane's fallback and has
no ``tenants`` row, so a membership naming it violates the
``tenant_memberships`` foreign key under Postgres.
"""


async def _home() -> int:
    tenant = await store.get_tenant_by_slug(HOME_SLUG)
    if tenant is None:
        tenant = await store.create_tenant(slug=HOME_SLUG, name="Home")
    return tenant.id


async def _api_token(scope: str, *, superadmin: bool = False, role: str = ROLE_ADMIN) -> str:
    """Mint a real token — the mount takes API tokens only, never a session JWT."""
    user = await users.create_user(
        CreateUserRequest(username=f"t_{scope}_{superadmin}", password="pw", superadmin=superadmin)
    )
    tenant_id = None if scope == SCOPE_SUPERADMIN else await _home()
    if not superadmin and tenant_id is not None:
        await store.set_membership(tenant_id=tenant_id, user_id=user.id, role=role)
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name=scope, scope=scope, tenant_id=tenant_id)
    )
    return created.token


async def _rpc(client: httpx.AsyncClient, method: str, params: dict[str, Any], headers: dict[str, str]) -> Any:
    res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()


async def test_tools_list_is_filtered_by_role(orch_ctx, enabled):
    viewer = {"Authorization": f"Bearer {await _api_token(SCOPE_WRITE, role=ROLE_VIEWER)}"}
    superadmin = {"Authorization": f"Bearer {await _api_token(SCOPE_SUPERADMIN, superadmin=True)}"}
    async with _mcp_http() as client:
        body = await _rpc(client, "tools/list", {}, viewer)
        names = {t["name"] for t in body["result"]["tools"]}
        assert "list_jobs" in names and "run_job" not in names and "setup" not in names

        body = await _rpc(client, "tools/list", {}, superadmin)
        names = {t["name"] for t in body["result"]["tools"]}
        assert {"list_jobs", "run_job", "setup"} <= names
        assert len(names) == len(await mcp.list_tools(run_middleware=False))


async def test_viewer_can_read_but_not_write(orch_ctx, enabled):
    viewer = {"Authorization": f"Bearer {await _api_token(SCOPE_WRITE, role=ROLE_VIEWER)}"}
    with active_tenant(await _home()):
        job = await create_job("mcp_rbac_job", simple_task)
    async with _mcp_http() as client:
        ok = await _rpc(client, "tools/call", {"name": "get_job", "arguments": {"ref": job.id}}, viewer)
        assert ok["result"]["structuredContent"]["name"] == "mcp_rbac_job"

        denied = await _rpc(client, "tools/call", {"name": "cancel_job", "arguments": {"ref": job.id}}, viewer)
        assert denied["result"]["isError"] is True
        assert "cannot perform 'admin'" in denied["result"]["content"][0]["text"]


async def test_anonymous_gets_401_problem(orch_ctx, enabled):
    async with _mcp_http() as client:
        res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert res.status_code == 401 and res.json()["code"] == "unauthorized"
