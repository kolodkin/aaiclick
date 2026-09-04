"""Audit middleware: policy selection and the rows it writes for REST and MCP."""

from __future__ import annotations

import pytest

from aaiclick.audit.view_models import AuditListFilter
from aaiclick.auth import config
from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import audit as audit_api
from aaiclick.internal_api import users
from aaiclick.tenancy import DEFAULT_TENANT_ID

from .app import API_PREFIX
from .audit import should_audit

SECRET = "server-audit-test-secret-key-at-least-32-bytes"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr("aaiclick.auth.config.is_local", lambda: False)
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


@pytest.mark.parametrize(
    "policy, method, path, action, expected",
    [
        pytest.param("writes", "GET", "/api/v0/jobs", None, False, id="writes-skips-reads"),
        pytest.param("writes", "POST", "/api/v0/auth/login", None, True, id="writes-logs-post"),
        pytest.param("writes", "DELETE", "/api/v0/auth/tokens/1", None, True, id="writes-logs-delete"),
        pytest.param("writes", "POST", "/mcp/", None, False, id="writes-skips-mcp-non-tool"),
        pytest.param("writes", "POST", "/mcp/", "run_job", True, id="writes-logs-mcp-tool"),
        pytest.param("all", "GET", "/api/v0/jobs", None, True, id="all-logs-reads"),
        pytest.param("all", "POST", "/mcp/", None, True, id="all-logs-mcp"),
        pytest.param("all", "GET", "/health", None, False, id="never-health"),
        pytest.param("all", "GET", "/api/v0/docs", None, False, id="never-docs"),
        pytest.param("all", "GET", "/assets/x.js", None, False, id="never-static"),
        pytest.param("off", "POST", "/api/v0/auth/login", None, False, id="off"),
    ],
)
def test_should_audit(policy, method, path, action, expected):
    assert should_audit(policy, method, path, action) is expected


def test_audit_policy_env(monkeypatch):
    monkeypatch.delenv("AAICLICK_AUDIT_LOG", raising=False)
    assert config.audit_policy() == "writes"
    monkeypatch.setenv("AAICLICK_AUDIT_LOG", "ALL")
    assert config.audit_policy() == "all"
    monkeypatch.setenv("AAICLICK_AUDIT_LOG", "bogus")
    assert config.audit_policy() == "writes"


async def _rows(**filters):
    return (await audit_api.list_audit(AuditListFilter(**filters))).items


async def test_login_attempts_are_attributed(orch_ctx, enabled, anon_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_AUDIT_LOG", "writes")
    await users.create_user(CreateUserRequest(username="alice", password="pw", superadmin=True))
    await anon_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "wrong"})
    await anon_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "pw"})

    rows = await _rows(path="/api/v0/auth/login")
    assert [r.status for r in rows] == [200, 401]  # newest first
    assert all(r.username == "alice" and r.auth_kind == "none" and r.method == "POST" for r in rows)
    assert rows[0].duration_ms >= 0


async def test_authenticated_write_carries_principal_and_tenant(orch_ctx, enabled, anon_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_AUDIT_LOG", "writes")
    admin = await users.create_user(CreateUserRequest(username="root", password="pw", superadmin=True))
    login = await anon_client.post(f"{API_PREFIX}/auth/login", json={"username": "root", "password": "pw"})
    headers = {"Authorization": f"Bearer {login.json()['access_token']}", "X-Tenant-Id": str(DEFAULT_TENANT_ID)}

    await anon_client.get(f"{API_PREFIX}/jobs", headers=headers)  # read: not logged under "writes"
    res = await anon_client.post(f"{API_PREFIX}/jobs/999999/cancel", headers=headers)
    assert res.status_code == 404

    rows = await _rows(path="/api/v0/jobs")
    assert len(rows) == 1
    row = rows[0]
    assert row.user_id == admin.id and row.auth_kind == "session" and row.tenant_id == DEFAULT_TENANT_ID
    assert row.status == 404 and row.method == "POST"


async def test_policy_all_logs_reads_and_off_logs_nothing(orch_ctx, enabled, anon_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_AUDIT_LOG", "all")
    await anon_client.get(f"{API_PREFIX}/auth/oidc/config")
    assert len(await _rows(path="/api/v0/auth/oidc/config")) == 1
    monkeypatch.setenv("AAICLICK_AUDIT_LOG", "off")
    await anon_client.get(f"{API_PREFIX}/auth/oidc/config")
    assert len(await _rows(path="/api/v0/auth/oidc/config")) == 1


async def test_audit_route_is_superadmin_only(orch_ctx, enabled, anon_client, app_client):
    await users.create_user(CreateUserRequest(username="viewer", password="pw"))
    login = await anon_client.post(f"{API_PREFIX}/auth/login", json={"username": "viewer", "password": "pw"})
    viewer = {"Authorization": f"Bearer {login.json()['access_token']}"}
    assert (await anon_client.get(f"{API_PREFIX}/audit", headers=viewer)).status_code == 403

    res = await app_client.get(f"{API_PREFIX}/audit", params={"method": "post", "path": "/api/v0/auth"})
    assert res.status_code == 200
    assert all(r["method"] == "POST" for r in res.json()["items"]) and res.json()["total"] >= 1
