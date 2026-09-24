"""Per-tool RBAC on the MCP surface.

``authorize_tool`` carries the role matrix; the HTTP tests drive a stateless
FastMCP app through the ``/mcp`` mount middleware to prove the principal on
the ASGI scope reaches the FastMCP middleware and filters / gates tools.
"""

from __future__ import annotations

from typing import TypedDict

import httpx
import pytest

from aaiclick.auth.models import ROLE_ADMIN, ROLE_MEMBER, SCOPE_ADMIN, SCOPE_WRITE, Role, ScopeLevel
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Forbidden
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task

from .auth import Principal
from .conftest import mcp_http
from .mcp import mcp
from .mcp_rbac import TAG_ADMIN, TAG_READ, TAG_WRITE, authorize_tool

JsonObject = dict[str, object]


class _Tool(TypedDict):
    name: str


class _ToolsListResult(TypedDict):
    tools: list[_Tool]


class _ToolsListResponse(TypedDict):
    result: _ToolsListResult


class _TextContent(TypedDict):
    text: str


class _ToolCallResult(TypedDict):
    isError: bool
    content: list[_TextContent]
    structuredContent: JsonObject


class _ToolCallResponse(TypedDict):
    result: _ToolCallResult


def _principal(*, role: Role = ROLE_ADMIN, scope: ScopeLevel = SCOPE_ADMIN):
    return Principal(user_id=5, role=role, scope=scope, kind="token")


# --- authorize_tool ------------------------------------------------------


@pytest.mark.parametrize(
    ("principal", "tags"),
    [
        pytest.param(_principal(role="viewer", scope="read"), {TAG_READ}, id="read-token-reads"),
        pytest.param(_principal(role="member", scope="write"), {TAG_WRITE}, id="member-writes"),
        pytest.param(_principal(role="admin", scope="admin"), {TAG_ADMIN}, id="admin-token-admins"),
        pytest.param(_principal(role="admin", scope="admin"), {TAG_READ}, id="admin-reads-too"),
    ],
)
def test_authorize_tool_allows(enabled, principal, tags):
    authorize_tool(principal, tags)


@pytest.mark.parametrize(
    ("principal", "tags"),
    [
        pytest.param(_principal(role="viewer", scope="read"), {TAG_WRITE}, id="read-token-no-write"),
        pytest.param(_principal(role="member", scope="write"), {TAG_ADMIN}, id="member-no-admin"),
    ],
)
def test_authorize_tool_forbids(enabled, principal, tags):
    with pytest.raises(Forbidden):
        authorize_tool(principal, tags)


def test_authorize_tool_local_mode_is_admin():
    """Local mode's synthetic principal (kind "none") acts as admin."""
    synthetic = Principal(user_id=None, role=ROLE_ADMIN, kind="none")
    authorize_tool(synthetic, {TAG_ADMIN})


async def test_every_tool_has_exactly_one_rbac_tag():
    tools = await mcp.list_tools(run_middleware=False)
    assert tools and all(len(t.tags & {TAG_READ, TAG_WRITE, TAG_ADMIN}) == 1 for t in tools)


# --- through the HTTP mount ---------------------------------------------


async def _api_token(scope: ScopeLevel, *, role: Role = ROLE_ADMIN) -> str:
    """Mint a real token — the mount takes API tokens only, never a session JWT."""
    user = await users.create_user(CreateUserRequest(username=f"t_{scope}_{role}", password="pw", role=role))
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name=scope, scope=scope))
    return created.token


async def _rpc(client: httpx.AsyncClient, method: str, params: JsonObject, headers: dict[str, str]) -> httpx.Response:
    res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers)
    assert res.status_code == 200, res.text
    return res


async def _tool_names(client: httpx.AsyncClient, headers: dict[str, str]) -> set[str]:
    body: _ToolsListResponse = (await _rpc(client, "tools/list", {}, headers)).json()
    return {t["name"] for t in body["result"]["tools"]}


async def _call_tool(
    client: httpx.AsyncClient, name: str, arguments: JsonObject, headers: dict[str, str]
) -> _ToolCallResult:
    body: _ToolCallResponse = (await _rpc(client, "tools/call", {"name": name, "arguments": arguments}, headers)).json()
    return body["result"]


async def test_tools_list_is_filtered_by_role(orch_ctx, enabled):
    member = {"Authorization": f"Bearer {await _api_token(SCOPE_WRITE, role=ROLE_MEMBER)}"}
    admin = {"Authorization": f"Bearer {await _api_token(SCOPE_ADMIN)}"}
    async with mcp_http() as client:
        names = await _tool_names(client, member)
        assert "list_jobs" in names and "run_job" not in names and "setup" not in names

        names = await _tool_names(client, admin)
        assert {"list_jobs", "run_job", "setup"} <= names
        assert len(names) == len(await mcp.list_tools(run_middleware=False))


async def test_member_can_read_but_not_admin(orch_ctx, enabled):
    """A member reads and makes their own writes; job mutations need admin."""
    member = {"Authorization": f"Bearer {await _api_token(SCOPE_WRITE, role=ROLE_MEMBER)}"}
    job = await create_job("mcp_rbac_job", simple_task)
    async with mcp_http() as client:
        ok = await _call_tool(client, "get_job", {"ref": job.id}, member)
        assert ok["structuredContent"]["name"] == "mcp_rbac_job"

        denied = await _call_tool(client, "cancel_job", {"ref": job.id}, member)
        assert denied["isError"] is True
        assert "'admin' scope required" in denied["content"][0]["text"]


async def test_anonymous_gets_401_problem(orch_ctx, enabled):
    async with mcp_http() as client:
        res = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert res.status_code == 401 and res.json()["code"] == "unauthorized"
    assert res.headers["www-authenticate"] == "Bearer"
