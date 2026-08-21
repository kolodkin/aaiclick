from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from aaiclick.auth import config, security
from aaiclick.tenancy import DEFAULT_TENANT_ID

from .app import app


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
