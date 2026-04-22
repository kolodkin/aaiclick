from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from .app import app


@pytest.fixture
async def app_client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
