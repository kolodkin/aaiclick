from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from aaiclick.auth import config, security
from aaiclick.tenancy import DEFAULT_TENANT_ID

from .app import API_PREFIX, app

TEST_JWT_SECRET = "server-test-jwt-secret-key-at-least-32-bytes-long"


@pytest.fixture
def enabled(monkeypatch):
    """Force distributed mode (auth on) + a signing secret, independent of the
    local/dist matrix the suite runs under. Request it *before* ``app_client``
    so that fixture mints its admin header."""
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", TEST_JWT_SECRET)


def bearer(user_id: int, *, superadmin: bool = False, tenants: dict[int, str] | None = None) -> dict[str, str]:
    """An ``Authorization`` header for a freshly minted access JWT."""
    token = security.encode_access_token(
        user_id=user_id, superadmin=superadmin, tenants=tenants or {}, secret=TEST_JWT_SECRET, ttl=60
    )
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
    token. Minting a superadmin access JWT directly is enough — the access-token
    path trusts claims and never hits the DB, so no seeded user row is required.
    Superadmins must name the active tenant on tenant-scoped routes, so the
    default tenant rides along as ``X-Tenant-Id``. In local mode auth is off and
    no headers are attached (synthetic superadmin + default tenant apply).
    """
    if not config.auth_enabled():
        return {}
    token = security.encode_access_token(
        user_id=1, superadmin=True, tenants={}, secret=config.require_jwt_secret(), ttl=3600
    )
    return {"Authorization": f"Bearer {token}", "X-Tenant-Id": str(DEFAULT_TENANT_ID)}


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
