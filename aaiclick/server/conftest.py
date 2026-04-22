"""Shared fixtures for ``aaiclick.server`` tests.

Tests use ``httpx.AsyncClient`` + ``ASGITransport`` — the async pattern
documented at https://fastapi.tiangolo.com/advanced/async-tests/. Running
the ASGI app in-process keeps contextvars on the test's event loop so the
outer ``orch_ctx`` fixture and the per-request nested ``orch_context``
see consistent state.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from .app import create_app


@pytest.fixture
async def app_client() -> AsyncIterator[httpx.AsyncClient]:
    """In-process async client against the FastAPI app — no network socket."""
    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
