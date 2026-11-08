from __future__ import annotations

from collections.abc import AsyncIterator

from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.orchestration.orch_context import orch_context, reuse_orch_resources

# Lifespan-state key for the ``OrchResources`` requests share.
ORCH_STATE_KEY = "orch"


async def orch_scope() -> AsyncIterator[None]:
    async with orch_context():
        yield


class SharedOrchMiddleware:
    """Runs each request on the lifespan's orch resources, so its ``orch_context``
    calls nest instead of each opening a SQL engine and ClickHouse client."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        resources = scope.get("state", {}).get(ORCH_STATE_KEY)
        if resources is None:
            await self.app(scope, receive, send)
            return
        with reuse_orch_resources(resources):
            await self.app(scope, receive, send)
