"""Tests for the principal-resolution layer and the /mcp mount guard.

``resolve_principal`` is shared by the REST dependency and the ``/mcp`` ASGI
middleware. HTTP end-to-end coverage (login -> access -> protected route, RBAC
403s) lives in the router tests; here we exercise the core resolver directly
and drive the middleware through the real ``/mcp`` FastMCP app.
"""

from __future__ import annotations

from unittest.mock import patch

import jwt
import pytest

from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER, SCOPE_READ
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Forbidden, Unauthorized
from aaiclick.view_models import Problem, ProblemCode

from . import auth
from .app import API_PREFIX
from .auth import warn_if_open
from .conftest import api_token_headers, bearer, mcp_http, mcp_rpc, mcp_tool_names

OTHER_SECRET = "a-different-secret-also-32-plus-bytes-long"

# --- resolve_principal ---------------------------------------------------


async def test_local_mode_returns_synthetic_admin(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)
    principal = await auth.resolve_principal(authorization=None)
    assert principal.role == "admin" and principal.kind == "none"


async def test_enabled_missing_token_unauthorized(enabled):
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization=None)


async def test_enabled_bad_signature_unauthorized(orch_ctx, enabled, anon_client):
    token = jwt.encode({"sub": "1", "type": "access", "role": "admin"}, OTHER_SECRET, algorithm="HS256")
    res = await anon_client.get(f"{API_PREFIX}/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 401
    assert Problem.model_validate(res.json()).code is ProblemCode.UNAUTHORIZED


async def test_api_token_resolves_live_owner_state(enabled, orch_ctx):
    """An ``aaic_`` credential is looked up in the DB and carries the owner's
    current role and the token's scope."""
    user = await users.create_user(CreateUserRequest(username="bot", password="pw", role="member"))
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name="ci", scope="read"))

    principal = await auth.resolve_principal(authorization=f"Bearer {created.token}")
    assert principal.user_id == user.id and principal.kind == "token" and principal.scope == "read"
    assert principal.role == "member"

    await users.disable_user(user.id, True)
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization=f"Bearer {created.token}")


async def test_unknown_api_token_unauthorized(enabled, orch_ctx):
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization="Bearer aaic_not-a-real-token")


@pytest.mark.parametrize(
    "held, required",
    [
        pytest.param("read", "read", id="read-reads"),
        pytest.param("admin", "write", id="admin-can-write"),
        pytest.param("admin", "admin", id="admin-can-admin"),
    ],
)
def test_check_scope_admits_down_the_ladder(held, required):
    principal = auth.Principal(user_id=1, role="viewer", scope=held, kind="token")
    auth.check_scope(principal, required)


@pytest.mark.parametrize(
    "held, required",
    [
        pytest.param("read", "write", id="read-cannot-write"),
        pytest.param("write", "admin", id="write-cannot-admin"),
    ],
)
def test_check_scope_forbids_up_the_ladder(held, required):
    principal = auth.Principal(user_id=1, role="viewer", scope=held, kind="token")
    with pytest.raises(Forbidden):
        auth.check_scope(principal, required)


def test_unscoped_principal_is_bounded_by_role_alone():
    """A session carries no scope; its role sets the ceiling."""
    auth.check_scope(auth.Principal(user_id=1, role="admin", kind="session"), "admin")


# --- principal_to_scope --------------------------------------------------


@pytest.mark.parametrize(
    ("principal", "expected"),
    [
        pytest.param(auth.Principal(user_id=5, role="viewer"), "read", id="viewer-session-reads"),
        pytest.param(auth.Principal(user_id=5, role="member"), "write", id="member-session-writes"),
        pytest.param(auth.Principal(user_id=5, role="admin"), "admin", id="admin-session-admins"),
        pytest.param(
            auth.Principal(user_id=5, role="admin", scope="read", kind="token"), "read", id="token-stands-alone"
        ),
        pytest.param(auth.Principal(user_id=None, role="admin", kind="none"), "admin", id="local-mode-is-admin"),
    ],
)
def test_principal_to_scope(principal, expected):
    assert auth.principal_to_scope(principal) == expected


def test_check_scope_forbids_too_little():
    with pytest.raises(Forbidden):
        auth.check_scope(auth.Principal(user_id=5, role="member"), "admin")
    auth.check_scope(auth.Principal(user_id=5, role="member"), "write")


# --- PrincipalAuthMiddleware ---------------------------------------------


async def test_mcp_mount_admits_an_api_token_and_stores_it(orch_ctx, enabled):
    """Per-tool RBAC lives in mcp_rbac.py — the mount only needs a principal.
    The stored principal is what FastMCP filters on: a read token sees no
    write tools."""
    headers = await api_token_headers(SCOPE_READ, role=ROLE_VIEWER)

    async with mcp_http() as client:
        names = await mcp_tool_names(client, headers)

    assert "list_jobs" in names and "run_job" not in names


async def test_mcp_mount_refuses_a_session_jwt(enabled):
    """MCP is the machine door; a session JWT belongs on REST."""
    async with mcp_http() as client:
        res = await mcp_rpc(client, "tools/list", headers=bearer(2, role=ROLE_ADMIN))

    assert res.status_code == 401


async def test_mcp_middleware_open_in_local_mode(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)

    async with mcp_http() as client:
        res = await mcp_rpc(client, "tools/list")

    assert res.status_code == 200, res.text


# --- warn_if_open --------------------------------------------------------


def test_warn_if_open_logs_in_local_mode(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)
    with patch.object(auth.logger, "warning") as warning:
        warn_if_open()
    warning.assert_called_once()


def test_warn_if_open_silent_in_distributed_mode(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    with patch.object(auth.logger, "warning") as warning:
        warn_if_open()
    warning.assert_not_called()
