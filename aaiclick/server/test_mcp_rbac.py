"""Per-tool RBAC on the MCP surface.

``authorize_tool`` carries the role matrix; the HTTP tests drive a stateless
FastMCP app through the ``/mcp`` mount middleware to prove the principal on
the ASGI scope reaches the FastMCP middleware and filters / gates tools.
"""

from __future__ import annotations

import pytest

from aaiclick.auth.models import ROLE_ADMIN, ROLE_MEMBER, SCOPE_ADMIN, SCOPE_WRITE, Role, ScopeLevel
from aaiclick.internal_api.errors import Forbidden
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task

from .auth import Principal
from .conftest import api_token_headers, mcp_http, mcp_rpc, mcp_tool_names
from .mcp import mcp
from .mcp_rbac import TAG_ADMIN, TAG_READ, TAG_WRITE, authorize_tool


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


async def test_tools_list_is_filtered_by_role(orch_ctx, enabled):
    member = await api_token_headers(SCOPE_WRITE, role=ROLE_MEMBER)
    admin = await api_token_headers(SCOPE_ADMIN)
    async with mcp_http() as client:
        names = await mcp_tool_names(client, member)
        assert "list_jobs" in names and "run_job" not in names and "setup" not in names

        names = await mcp_tool_names(client, admin)
        assert {"list_jobs", "run_job", "setup"} <= names
        assert len(names) == len(await mcp.list_tools(run_middleware=False))


async def test_member_can_read_but_not_admin(orch_ctx, enabled):
    """A member reads and makes their own writes; job mutations need admin."""
    member = await api_token_headers(SCOPE_WRITE, role=ROLE_MEMBER)
    job = await create_job("mcp_rbac_job", simple_task)
    async with mcp_http() as client:
        ok = await mcp_rpc(client, "tools/call", {"name": "get_job", "arguments": {"ref": job.id}}, member)
        assert ok.status_code == 200, ok.text
        assert ok.json()["result"]["structuredContent"]["name"] == "mcp_rbac_job"

        denied = await mcp_rpc(client, "tools/call", {"name": "cancel_job", "arguments": {"ref": job.id}}, member)
        assert denied.status_code == 200, denied.text
        result = denied.json()["result"]
        assert result["isError"] is True
        assert "'admin' scope required" in result["content"][0]["text"]


async def test_anonymous_gets_401_problem(orch_ctx, enabled):
    async with mcp_http() as client:
        res = await mcp_rpc(client, "tools/list")
    assert res.status_code == 401 and res.json()["code"] == "unauthorized"
    assert res.headers["www-authenticate"] == "Bearer"
