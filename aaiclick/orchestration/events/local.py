"""Local mode (chdb + SQLite): publish straight onto the in-process bus.

Local mode runs the API server, execution worker and background worker in
one process, so the committing session can hand the signal to the bus
directly — no network hop, nothing to listen on. Writers outside this
process are not observed, which the chdb file lock already rules out.
"""

from __future__ import annotations

import asyncio

from sqlalchemy.orm import Session

from .bus import EventBus, get_event_bus


class LocalTransport:
    def before_commit(self, session: Session) -> None:
        return None

    def after_commit(self, session: Session) -> None:
        get_event_bus().publish()

    async def feed(self, bus: EventBus, *, stop: asyncio.Event) -> None:
        bus.publish()
        await stop.wait()
