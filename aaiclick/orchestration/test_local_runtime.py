"""Tests for local_runtime() — the lifespan-shared worker helper.

End-to-end coverage (worker startup, job completion, shutdown
cancellation) lives in aaiclick/server/test_app.py — that file's
app_client fixture already enters the FastAPI lifespan, which is the
helper's only production caller.
"""

from __future__ import annotations

import pytest

from . import local_runtime as lr


async def test_local_runtime_rejects_distributed_mode(monkeypatch):
    """Outside local mode the helper raises before touching any resource."""
    monkeypatch.setattr(lr, "is_local", lambda: False)
    with pytest.raises(RuntimeError, match="requires local mode"):
        async with lr.local_runtime():
            pass


async def test_local_runtime_rejects_a_stale_local_db(monkeypatch):
    """The ``setup_done`` marker carries no schema version, so an upgrade over
    an existing install would skip ``setup()`` and start the workers against a
    database missing columns. The helper refuses instead."""
    monkeypatch.setattr(lr, "is_local", lambda: True)
    monkeypatch.setattr(lr, "is_setup_done", lambda: True)
    monkeypatch.setattr(lr, "stale_local_db", lambda: ["jobs.tenant_id"])
    monkeypatch.setattr(lr, "stale_local_db_message", lambda stale: f"stale: {stale}")

    with pytest.raises(RuntimeError, match="--force"):
        async with lr.local_runtime():
            pass
