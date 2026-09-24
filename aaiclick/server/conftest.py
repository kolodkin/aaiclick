from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from aaiclick.auth import config, security
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER, Role, ScopeLevel
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users

from .app import API_PREFIX, app
from .auth import PrincipalAuthMiddleware
from .mcp import mcp

TEST_JWT_SECRET = "server-test-jwt-secret-key-at-least-32-bytes-long"


@pytest.fixture
def enabled(monkeypatch):
    """Force distributed mode (auth on) + a signing secret, independent of the
    local/dist matrix the suite runs under. Request it *before* ``app_client``
    so that fixture mints its admin header."""
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", TEST_JWT_SECRET)


def bearer(user_id: int, *, role: Role = ROLE_VIEWER) -> dict[str, str]:
    """An ``Authorization`` header for a freshly minted access JWT."""
    token = security.encode_access_token(user_id=user_id, role=role, secret=TEST_JWT_SECRET, ttl=60)
    return {"Authorization": f"Bearer {token}"}


async def login(client: httpx.AsyncClient, username: str, password: str = "pw") -> dict[str, str]:
    """Log in through the API and return the ``Authorization`` header."""
    res = await client.post(f"{API_PREFIX}/auth/login", json={"username": username, "password": password})
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


def _admin_headers() -> dict[str, str]:
    """An admin bearer header when auth is enforced (distributed mode), else empty.

    Auth is mode-derived (``config.auth_enabled()`` → ``not is_local()``), so the
    distributed test matrix runs with auth ON and every protected route needs a
    token. Minting an admin access JWT directly is enough — the access-token path
    trusts claims and never hits the DB, so no seeded user row is required. In
    local mode auth is off and no headers are attached (synthetic admin applies).
    """
    if not config.auth_enabled():
        return {}
    token = security.encode_access_token(user_id=1, role=ROLE_ADMIN, secret=config.require_jwt_secret(), ttl=3600)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def app_client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver", headers=_admin_headers()) as client:
        yield client


@pytest.fixture
async def anon_client() -> AsyncIterator[httpx.AsyncClient]:
    """An unauthenticated client — for asserting 401 on protected routes."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@asynccontextmanager
async def mcp_http() -> AsyncIterator[httpx.AsyncClient]:
    """The FastMCP app behind the mount middleware, stateless + JSON so a plain
    POST answers with a JSON-RPC body instead of an SSE stream.

    A context manager rather than a fixture: the session manager's lifespan
    opens an anyio task group, which must be exited in the task that entered
    it — pytest-asyncio tears async fixtures down in a different task.
    """
    http_app = mcp.http_app(path="/", stateless_http=True, json_response=True)
    async with http_app.lifespan(http_app):
        transport = httpx.ASGITransport(app=PrincipalAuthMiddleware(http_app))
        headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
        async with httpx.AsyncClient(transport=transport, base_url="http://mcp", headers=headers) as client:
            yield client


async def mcp_rpc(
    client: httpx.AsyncClient,
    method: str,
    params: dict[str, object] | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """POST one JSON-RPC request to an ``mcp_http`` client."""
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    return await client.post("/", json=body, headers=headers)


async def mcp_tool_names(client: httpx.AsyncClient, headers: dict[str, str] | None = None) -> set[str]:
    """Names from a successful ``tools/list`` — the set the principal may see."""
    res = await mcp_rpc(client, "tools/list", headers=headers)
    assert res.status_code == 200, res.text
    return {t["name"] for t in res.json()["result"]["tools"]}


async def api_token_headers(scope: ScopeLevel, *, role: Role = ROLE_ADMIN) -> dict[str, str]:
    """An ``Authorization`` header for a real API token owned by a fresh user.

    The ``/mcp`` mount takes API tokens only, never a session JWT.
    """
    user = await users.create_user(CreateUserRequest(username=f"t_{scope}_{role}", password="pw", role=role))
    created = await api_tokens.create_token(user.id, CreateApiTokenRequest(name=scope, scope=scope))
    return {"Authorization": f"Bearer {created.token}"}
