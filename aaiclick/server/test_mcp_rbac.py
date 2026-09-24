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

from aaiclick.auth.models import ROLE_ADMIN, ROLE_MEMBER, SCOPE_ADMIN, SCOPE_WRITE, Role, ScopeLevel
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Forbidden
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task

from .auth import Principal, PrincipalAuthMiddleware
from .mcp import mcp
from .mcp_rbac import TAG_ADMIN, TAG_READ, TAG_WRITE, authorize_tool

MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def _principal(*, role: Role = ROLE_ADMIN, scope: ScopeLevel = SCOPE_ADMIN):
    return Principal(user_id=5, role=role, scope=scope, kind="token")


# --- authorize_tool ------------------------------------------------------


@pytest.mark.parametrize(
    ("principal", "tags", "expect"),
    [
        pytest.param(_principal(role="viewer", scope="read"), {TAG_READ}, "ok", id="read-token-reads"),
        pytest.param(_principal(role="viewer", scope="read"), {TAG_WRITE}, Forbidden, id="read-token-no-write"),
        pytest.param(_principal(role="member", scope="write"), {TAG_WRITE}, "ok", id="member-writes"),
        pytest.param(_principal(role="member", scope="write"), {TAG_ADMIN}, Forbidden, id="member-no-admin"),
        pytest.param(_principal(role="admin", scope="admin"), {TAG_ADMIN}, "ok", id="admin-token-admins"),
        pytest.param(_principal(role="admin", scope="admin"), {TAG_READ}, "ok", id="admin-reads-too"),
    ],
)
def test_authorize_tool_matrix(enabled, principal, tags, expect):
    if expect == "ok":
        authorize_tool(principal, tags)
    else:
        with pytest.raises(expect):
            authorize_tool(principal, tags)


def test_authorize_tool_local_mode_is_admin():
    """Local mode's synthetic principal (kind "none") acts as admin."""
    synthetic = Principal(user_id=None, role=ROLE_ADMIN, kind="none")
    authorize_tool(synthetic, {TAG_ADMIN})


async def test_every_tool_has_exactly_one_rbac_tag():
    tools = await mcp.list_tools(run_middleware=False)
    assert tools and all(len(t.tags & {TAG_READ, TAG_WRITE, TAG_ADMIN}) == 1 for t in tools)


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


async def _api_token(scope: ScopeLevel, *, role: Role = ROLE_ADMIN) -> str:
    """Mint a real token — the mount takes API tokens only, never a session JWT."""
    user = await users.create_user(CreateUserRequest(username=f"t_{scope}_{role}", password="pw", role=role))
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name=scope, scope=scope))
    return created.token


async def _rpc(client: httpx.AsyncClient, method: str, params: dict[str, Any], headers: dict[str, str]) -> Any:
    res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()


async def test_tools_list_is_filtered_by_role(orch_ctx, enabled):
    member = {"Authorization": f"Bearer {await _api_token(SCOPE_WRITE, role=ROLE_MEMBER)}"}
    admin = {"Authorization": f"Bearer {await _api_token(SCOPE_ADMIN)}"}
    async with _mcp_http() as client:
        body = await _rpc(client, "tools/list", {}, member)
        names = {t["name"] for t in body["result"]["tools"]}
        assert "list_jobs" in names and "run_job" not in names and "setup" not in names

        body = await _rpc(client, "tools/list", {}, admin)
        names = {t["name"] for t in body["result"]["tools"]}
        assert {"list_jobs", "run_job", "setup"} <= names
        assert len(names) == len(await mcp.list_tools(run_middleware=False))


async def test_member_can_read_but_not_admin(orch_ctx, enabled):
    """A member reads and makes their own writes; job mutations need admin."""
    member = {"Authorization": f"Bearer {await _api_token(SCOPE_WRITE, role=ROLE_MEMBER)}"}
    job = await create_job("mcp_rbac_job", simple_task)
    async with _mcp_http() as client:
        ok = await _rpc(client, "tools/call", {"name": "get_job", "arguments": {"ref": job.id}}, member)
        assert ok["result"]["structuredContent"]["name"] == "mcp_rbac_job"

        denied = await _rpc(client, "tools/call", {"name": "cancel_job", "arguments": {"ref": job.id}}, member)
        assert denied["result"]["isError"] is True
        assert "'admin' scope required" in denied["result"]["content"][0]["text"]


async def test_anonymous_gets_401_problem(orch_ctx, enabled):
    async with _mcp_http() as client:
        res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert res.status_code == 401 and res.json()["code"] == "unauthorized"
    assert res.headers["www-authenticate"] == "Bearer"
