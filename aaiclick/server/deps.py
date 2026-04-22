"""FastAPI dependencies that scope each request to an ``orch_context``.

``internal_api`` functions read resources through contextvar getters, so the
only work a dependency has to do is enter the surrounding context on request
start and exit on response. The ``with_ch`` variant is required for routes
that touch ClickHouse (run_job, object listing/reading/purging).
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from aaiclick.orchestration.orch_context import orch_context


async def orch_scope() -> AsyncIterator[None]:
    """SQL-only orch_context for read-heavy routes — no ClickHouse client."""
    async with orch_context(with_ch=False):
        yield


async def orch_scope_with_ch() -> AsyncIterator[None]:
    """Full orch_context with ClickHouse client for routes that execute tasks or query CH."""
    async with orch_context(with_ch=True):
        yield
