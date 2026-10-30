"""``get_shared_session``: the embedded engine's startup is warmed and logged."""

from __future__ import annotations

import logging

from aaiclick.data.object_context import chdb_client
from aaiclick.data.object_context.chdb_client import get_shared_session


class _FakeSession:
    """Stands in for ``chdb.session.Session``: chdb allows one engine per
    process, and the test suite's real session is already open."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.queries: list[str] = []

    def query(self, sql: str) -> None:
        self.queries.append(sql)


def test_first_session_is_warmed_and_its_startup_logged(monkeypatch, tmp_path, caplog):
    """The first statement, not the constructor, pays for loading the engine, so
    the session is warmed before it is handed out and the total is logged once."""
    monkeypatch.setattr(chdb_client, "Session", _FakeSession)
    path = str(tmp_path / "chdb_data")
    caplog.set_level(logging.INFO, logger="aaiclick.data.object_context.chdb_client")
    try:
        session = get_shared_session(path)
        again = get_shared_session(path)
    finally:
        chdb_client._sessions.pop(path, None)

    assert again is session
    assert isinstance(session, _FakeSession)
    assert session.queries == ["SELECT 1"]
    ready = [r.getMessage() for r in caplog.records if "chdb session ready" in r.getMessage()]
    assert len(ready) == 1
    assert ready[0].endswith(f"s ({path})")
