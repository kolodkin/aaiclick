"""Tests for local_runtime() — the lifespan-shared worker helper.

End-to-end coverage (worker startup, job completion, shutdown
cancellation) lives in aaiclick/server/test_app.py — that file's
app_client fixture already enters the FastAPI lifespan, which is the
helper's only production caller.
"""

from __future__ import annotations

import pytest

from . import local_runtime as lr


class _StopEarly(Exception):
    """Ends the helper once the setup decision under test has been made."""


class _RaisingWorker:
    async def start(self) -> None:
        raise _StopEarly


async def test_local_runtime_rejects_distributed_mode(monkeypatch):
    """Outside local mode the helper raises before touching any resource."""
    monkeypatch.setattr(lr, "is_local", lambda: False)
    with pytest.raises(RuntimeError, match="requires local mode"):
        async with lr.local_runtime():
            pass


async def test_local_runtime_rejects_a_stale_local_db(monkeypatch):
    """The ``setup_done`` marker carries no schema version, so an upgrade over
    an existing install would skip ``setup()`` and start the workers against a
    database that is behind the models. The helper refuses instead."""
    monkeypatch.setattr(lr, "is_local", lambda: True)
    monkeypatch.setattr(lr, "is_setup_done", lambda: True)
    monkeypatch.setattr(lr, "stale_local_db_reason", lambda: "stale: jobs.tenant_id")

    with pytest.raises(RuntimeError, match="--force"):
        async with lr.local_runtime():
            pass


async def test_local_runtime_rebuilds_when_the_marker_outlives_the_schema(monkeypatch):
    """A ``setup_done`` marker beside a database missing its tables is not
    set up. ``stale_local_db_reason`` cannot see this — it compares columns of
    tables that exist — so the helper checks for missing tables and re-runs
    setup instead of starting workers that fail on the first query."""
    monkeypatch.setattr(lr, "is_local", lambda: True)
    monkeypatch.setattr(lr, "is_setup_done", lambda: True)
    monkeypatch.setattr(lr, "missing_local_tables", lambda: ["jobs", "tasks"])
    monkeypatch.setattr(lr, "stale_local_db_reason", lambda: None)
    called: list[bool] = []
    monkeypatch.setattr(lr, "setup", lambda: called.append(True))
    monkeypatch.setattr(lr, "render_setup_result", lambda result: None)
    monkeypatch.setattr(lr, "BackgroundWorker", _RaisingWorker)

    with pytest.raises(_StopEarly):
        async with lr.local_runtime():
            pass
    assert called == [True]
