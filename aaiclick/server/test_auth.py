"""Tests for the bearer-token auth layer.

The REST dependency and the ``/mcp`` ASGI middleware share one check
(``_rejection_detail``). HTTP-level tests cover the REST dependency end to
end; the FastMCP mount is exercised through the middleware directly so the
test does not depend on the MCP session-manager lifespan.
"""

from __future__ import annotations

import logging

from aaiclick.view_models import Problem, ProblemCode

from .app import API_PREFIX
from .auth import BearerAuthMiddleware, warn_if_open


async def test_open_mode_allows_requests(orch_ctx, app_client, monkeypatch):
    monkeypatch.delenv("AAICLICK_API_TOKEN", raising=False)

    response = await app_client.get(f"{API_PREFIX}/workers")

    assert response.status_code == 200


async def test_missing_token_returns_401_with_challenge(orch_ctx, app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    response = await app_client.get(f"{API_PREFIX}/workers")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert Problem.model_validate(response.json()).code is ProblemCode.UNAUTHORIZED


async def test_invalid_token_returns_401(orch_ctx, app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    response = await app_client.get(f"{API_PREFIX}/workers", headers={"Authorization": "Bearer wrong"})

    assert response.status_code == 401
    assert Problem.model_validate(response.json()).code is ProblemCode.UNAUTHORIZED


async def test_valid_token_allows_request(orch_ctx, app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    response = await app_client.get(f"{API_PREFIX}/workers", headers={"Authorization": "Bearer secret"})

    assert response.status_code == 200


async def test_health_stays_open_with_token_set(app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    response = await app_client.get("/health")

    assert response.status_code == 200


async def test_openapi_stays_open_with_token_set(app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    response = await app_client.get(f"{API_PREFIX}/openapi.json")

    assert response.status_code == 200


async def test_mcp_middleware_rejects_missing_token(monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    async def inner(scope, receive, send):
        raise AssertionError("middleware must not delegate on a missing token")

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    await BearerAuthMiddleware(inner)({"type": "http", "headers": []}, receive, send)

    start = sent[0]
    assert start["status"] == 401
    assert (b"www-authenticate", b"Bearer") in start["headers"]


async def test_mcp_middleware_delegates_on_valid_token(monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")
    delegated = False

    async def inner(scope, receive, send):
        nonlocal delegated
        delegated = True

    async def send(message):
        pass

    async def receive():
        return {"type": "http.request"}

    scope = {"type": "http", "headers": [(b"authorization", b"Bearer secret")]}
    await BearerAuthMiddleware(inner)(scope, receive, send)

    assert delegated


async def test_mcp_middleware_passes_through_non_http_scope(monkeypatch):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")
    delegated = False

    async def inner(scope, receive, send):
        nonlocal delegated
        delegated = True

    async def send(message):
        pass

    async def receive():
        return {}

    await BearerAuthMiddleware(inner)({"type": "lifespan"}, receive, send)

    assert delegated


def test_warn_if_open_logs_when_token_unset(monkeypatch, caplog):
    monkeypatch.delenv("AAICLICK_API_TOKEN", raising=False)

    with caplog.at_level(logging.WARNING):
        warn_if_open()

    assert any("unset" in r.getMessage() for r in caplog.records)


def test_warn_if_open_silent_when_token_set(monkeypatch, caplog):
    monkeypatch.setenv("AAICLICK_API_TOKEN", "secret")

    with caplog.at_level(logging.WARNING):
        warn_if_open()

    assert not caplog.records
