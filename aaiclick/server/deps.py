from __future__ import annotations

from collections.abc import AsyncIterator

from aaiclick.orchestration.orch_context import orch_context


async def orch_scope() -> AsyncIterator[None]:
    async with orch_context(with_ch=False):
        yield


async def orch_scope_with_ch() -> AsyncIterator[None]:
    async with orch_context(with_ch=True):
        yield
