"""Tests for the principal-resolution layer and the /mcp mount guard.

``resolve_principal`` is shared by the REST dependency and the ``/mcp`` ASGI
middleware. HTTP end-to-end coverage (login -> access -> protected route, RBAC
403s) lives in the router tests; here we exercise the core resolver and the
middleware directly so they do not depend on the MCP session-manager lifespan.
"""

from __future__ import annotations

from unittest.mock import patch

import jwt
import pytest

from aaiclick.auth import security, store
from aaiclick.auth.view_models import CreateApiTokenRequest, CreateUserRequest
from aaiclick.internal_api import api_tokens, users
from aaiclick.internal_api.errors import Forbidden, Invalid, Unauthorized
from aaiclick.tenancy import DEFAULT_TENANT_ID

from . import auth
from .auth import PrincipalAuthMiddleware, warn_if_open
from .conftest import TEST_JWT_SECRET
from .request_state import audit_state

OTHER_SECRET = "a-different-secret-also-32-plus-bytes-long"


def _bearer(token: str) -> str:
    return f"Bearer {token}"


# --- resolve_principal ---------------------------------------------------


async def test_local_mode_returns_synthetic_admin(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: True)
    principal = await auth.resolve_principal(authorization=None)
    assert principal.superadmin is True and principal.kind == "none"


async def test_enabled_missing_token_unauthorized(enabled):
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization=None)


async def test_enabled_valid_jwt(enabled):
    token = security.encode_access_token(
        user_id=7, superadmin=False, tenants={3: "viewer"}, secret=TEST_JWT_SECRET, ttl=60
    )
    principal = await auth.resolve_principal(authorization=_bearer(token))
    assert principal.user_id == 7 and principal.superadmin is False
    assert principal.tenants == {3: "viewer"}
    assert principal.kind == "session" and principal.scope is None


async def test_enabled_bad_signature_unauthorized(enabled):
    token = jwt.encode({"sub": "1", "type": "access"}, OTHER_SECRET, algorithm="HS256")
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization=_bearer(token))


async def test_api_token_resolves_live_owner_state(enabled, orch_ctx):
    """An ``aaic_`` credential is looked up in the DB and carries the owner's
    current flag, live role in the token's tenant, and the token's scope."""
    user = await users.create_user(CreateUserRequest(username="bot", password="pw"))
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await store.set_membership(tenant_id=tenant.id, user_id=user.id, role="viewer")
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="ci", scope="read", tenant_id=tenant.id)
    )

    principal = await auth.resolve_principal(authorization=_bearer(created.token))
    assert principal.user_id == user.id and principal.kind == "token" and principal.scope == "read"
    assert principal.tenants == {tenant.id: "viewer"}

    await users.disable_user(user.id, True)
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization=_bearer(created.token))


async def test_unknown_api_token_unauthorized(enabled, orch_ctx):
    with pytest.raises(Unauthorized):
        await auth.resolve_principal(authorization=_bearer("aaic_not-a-real-token"))


@pytest.mark.parametrize(
    "held, required, allowed",
    [
        pytest.param("read", "read", True, id="read-reads"),
        pytest.param("read", "write", False, id="read-cannot-write"),
        pytest.param("write", "admin", False, id="write-cannot-admin"),
        pytest.param("admin", "write", True, id="admin-can-write"),
        pytest.param("admin", "superadmin", False, id="admin-cannot-superadmin"),
        pytest.param("superadmin", "superadmin", True, id="superadmin-can"),
    ],
)
def test_enforce_scope_walks_the_ladder(held, required, allowed):
    principal = auth.Principal(user_id=1, superadmin=True, tenants={}, scope=held, kind="token")
    if allowed:
        auth.enforce_scope(principal, required)
    else:
        with pytest.raises(Forbidden):
            auth.enforce_scope(principal, required)


def test_unscoped_principal_is_never_blocked_by_the_ladder():
    """A session is bounded by its user's role, not by a scope."""
    session = auth.Principal(user_id=1, superadmin=True, tenants={}, kind="session")
    auth.enforce_scope(session, "superadmin")


def test_resolve_tenant_local_mode_defaults():
    synthetic = auth.Principal(user_id=None, superadmin=True, tenants={}, kind="none")
    assert auth.resolve_tenant(synthetic, None) == auth.TenantContext(tenant_id=DEFAULT_TENANT_ID, role="admin")


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


# --- PrincipalAuthMiddleware ---------------------------------------------


async def _drive(scope, middleware_inner_flag):
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    async def inner(scope, receive, send):
        middleware_inner_flag.append(True)

    await PrincipalAuthMiddleware(inner)(scope, receive, send)
    return sent


async def test_mcp_middleware_rejects_missing_token(enabled):
    called: list[bool] = []
    sent = await _drive({"type": "http", "headers": []}, called)
    assert not called
    assert sent[0]["status"] == 401
    assert (b"www-authenticate", b"Bearer") in sent[0]["headers"]


async def test_mcp_mount_admits_an_api_token_and_stores_it(orch_ctx, enabled):
    """Per-tool RBAC lives in mcp_rbac.py — the mount only needs a principal."""
    called: list[bool] = []
    user = await users.create_user(CreateUserRequest(username="m", password="pw"))
    await store.set_membership(tenant_id=DEFAULT_TENANT_ID, user_id=user.id, role="viewer")
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="m", scope="read", tenant_id=DEFAULT_TENANT_ID)
    )
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {created.token}".encode())]}
    await _drive(scope, called)
    assert called == [True]
    recorded = audit_state(scope).principal
    assert recorded is not None and recorded.user_id == user.id


async def test_mcp_mount_refuses_a_session_jwt(enabled):
    """MCP is the machine door; a session JWT belongs on REST."""
    called: list[bool] = []
    token = security.encode_access_token(user_id=2, superadmin=True, tenants={}, secret=TEST_JWT_SECRET, ttl=60)
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]}
    sent = await _drive(scope, called)
    assert not called
    assert sent[0]["status"] == 401


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


# --- tenant-bound API tokens ---------------------------------------------


def _bound(level="admin", tenant_id=7, role="admin"):
    return auth.Principal(
        user_id=5, superadmin=False, tenants={tenant_id: role}, scope=level, kind="token", tenant_id=tenant_id
    )


def test_bound_token_ignores_a_missing_header():
    ctx = auth.resolve_tenant(_bound(), None)
    assert ctx == auth.TenantContext(tenant_id=7, role="admin")


def test_bound_token_rejects_a_mismatched_header():
    """A client naming a different tenant has a bug — surface it, don't ignore it."""
    with pytest.raises(Invalid, match="bound"):
        auth.resolve_tenant(_bound(), "8")


def test_bound_token_accepts_a_matching_header():
    assert auth.resolve_tenant(_bound(), "7").tenant_id == 7


def test_bound_token_forbidden_once_membership_is_gone():
    stripped = _bound()._replace(tenants={})
    with pytest.raises(Forbidden):
        auth.resolve_tenant(stripped, None)


def test_session_principal_is_unscoped():
    session = auth.Principal(user_id=1, superadmin=True, tenants={}, kind="session")
    assert session.scope is None and session.tenant_id is None
