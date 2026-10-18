"""Tests for the orchestration CLI entry points."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.cli import start_local


async def test_start_local_refuses_non_loopback_host(monkeypatch):
    """Local mode has auth off, so binding a reachable address opens the server to the network."""
    monkeypatch.setattr("aaiclick.orchestration.cli.is_local", lambda: True)
    with pytest.raises(RuntimeError, match="loopback"):
        await start_local(host="0.0.0.0")
