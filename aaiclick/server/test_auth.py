"""Tests for the JWT principal-resolution layer and the admin-only /mcp guard.

``resolve_principal`` is shared by the REST dependency and the ``/mcp`` ASGI
middleware. HTTP end-to-end coverage (login -> access -> protected route, RBAC
403s) lives in the router tests; here we exercise the core resolver and the
middleware directly so they do not depend on the MCP session-manager lifespan.
"""

from __future__ import annotations

from unittest.mock import patch

import jwt
import pytest

from aaiclick.auth import security
from aaiclick.internal_api.errors import Forbidden, Invalid, Unauthorized

from . import auth
from .auth import AdminAuthMiddleware, warn_if_open

SECRET = "server-auth-test-secret-key-32-plus-bytes"
OTHER_SECRET = "a-different-secret-also-32-plus-bytes-long"


@pytest.fixture
def enabled(monkeypatch):
    # Auth is mode-derived: force distributed mode (auth on) + a signing secret.
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def _admin_token() -> str:
    return security.encode_access_token(user_id=1, superadmin=True, tenants={}, secret=SECRET, ttl=60)


# --- resolve_principal ---------------------------------------------------


def test_local_mode_returns_synthetic_admin(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)
    principal = auth.resolve_principal(authorization=None)
    assert principal.superadmin is True


def test_enabled_missing_token_unauthorized(enabled):
    with pytest.raises(Unauthorized):
        auth.resolve_principal(authorization=None)


def test_enabled_valid_jwt(enabled):
    token = security.encode_access_token(user_id=7, superadmin=False, tenants={3: "viewer"}, secret=SECRET, ttl=60)
    principal = auth.resolve_principal(authorization=_bearer(token))
    assert principal.user_id == 7 and principal.superadmin is False
    assert principal.tenants == {3: "viewer"}


def test_enabled_bad_signature_unauthorized(enabled):
    token = jwt.encode({"sub": "1", "type": "access"}, OTHER_SECRET, algorithm="HS256")
    with pytest.raises(Unauthorized):
        auth.resolve_principal(authorization=_bearer(token))


# --- resolve_tenant ------------------------------------------------------


def _principal(superadmin=False, tenants=None):
    return auth.Principal(user_id=5, superadmin=superadmin, tenants=tenants or {})


def test_tenant_header_resolves_membership_role():
    ctx = auth.resolve_tenant(_principal(tenants={7: "viewer"}), "7")
    assert ctx == auth.TenantContext(tenant_id=7, role="viewer")


def test_tenant_header_superadmin_gets_admin_anywhere():
    ctx = auth.resolve_tenant(_principal(superadmin=True), "7")
    assert ctx.role == "admin" and ctx.tenant_id == 7


def test_tenant_header_non_member_forbidden():
    with pytest.raises(Forbidden):
        auth.resolve_tenant(_principal(tenants={7: "admin"}), "8")


def test_tenant_header_bad_int_invalid():
    with pytest.raises(Invalid):
        auth.resolve_tenant(_principal(tenants={7: "admin"}), "acme")


def test_tenant_header_missing_single_membership_implied():
    ctx = auth.resolve_tenant(_principal(tenants={7: "admin"}), None)
    assert ctx == auth.TenantContext(tenant_id=7, role="admin")


def test_tenant_header_missing_zero_or_many_invalid():
    with pytest.raises(Invalid):
        auth.resolve_tenant(_principal(), None)
    with pytest.raises(Invalid):
        auth.resolve_tenant(_principal(tenants={7: "admin", 8: "viewer"}), None)


def test_tenant_header_missing_superadmin_requires_header():
    with pytest.raises(Invalid):
        auth.resolve_tenant(_principal(superadmin=True), None)


# --- AdminAuthMiddleware -------------------------------------------------


async def _drive(scope, middleware_inner_flag):
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    async def inner(scope, receive, send):
        middleware_inner_flag.append(True)

    await AdminAuthMiddleware(inner)(scope, receive, send)
    return sent


async def test_mcp_middleware_rejects_missing_token(enabled):
    called: list[bool] = []
    sent = await _drive({"type": "http", "headers": []}, called)
    assert not called
    assert sent[0]["status"] == 401
    assert (b"www-authenticate", b"Bearer") in sent[0]["headers"]


async def test_mcp_middleware_rejects_viewer(enabled):
    called: list[bool] = []
    token = security.encode_access_token(user_id=2, superadmin=False, tenants={3: "admin"}, secret=SECRET, ttl=60)
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]}
    sent = await _drive(scope, called)
    assert not called
    assert sent[0]["status"] == 403


async def test_mcp_middleware_delegates_on_admin(enabled):
    called: list[bool] = []
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {_admin_token()}".encode())]}
    await _drive(scope, called)
    assert called == [True]


async def test_mcp_middleware_open_in_local_mode(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)
    called: list[bool] = []
    await _drive({"type": "http", "headers": []}, called)
    assert called == [True]


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
