"""Tests for local_runtime() — the lifespan-shared worker helper.

End-to-end coverage (worker startup, job completion, shutdown
cancellation) lives in aaiclick/server/test_app.py — that file's
app_client fixture already enters the FastAPI lifespan, which is the
helper's only production caller.
"""

from __future__ import annotations

import pytest

from .local_runtime import local_runtime


async def test_local_runtime_rejects_distributed_mode(monkeypatch):
    """Outside local mode the helper raises before touching any resource."""
    monkeypatch.setattr(
        "aaiclick.orchestration.local_runtime.is_local",
        lambda: False,
    )
    with pytest.raises(RuntimeError, match="requires local mode"):
        async with local_runtime():
            pytest.fail("local_runtime() should have raised before yielding")
