"""
Tests for LifecycleHandler ABC and LocalLifecycleHandler.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from aaiclick.data.data_context import LifecycleHandler, LocalLifecycleHandler


def test_lifecycle_handler_is_abstract():
    """LifecycleHandler cannot be instantiated directly."""
    with pytest.raises(TypeError):
        LifecycleHandler()  # type: ignore[abstract]


def test_local_lifecycle_delegates_incref():
    """LocalLifecycleHandler.incref delegates to AsyncTableWorker."""
    handler = LocalLifecycleHandler(MagicMock())
    handler._worker = MagicMock()

    handler.incref("table_123")

    handler._worker.incref.assert_called_once_with("table_123")


def test_local_lifecycle_delegates_decref():
    """LocalLifecycleHandler.decref delegates to AsyncTableWorker."""
    handler = LocalLifecycleHandler(MagicMock())
    handler._worker = MagicMock()

    handler.decref("table_456")

    handler._worker.decref.assert_called_once_with("table_456")


async def test_local_lifecycle_start_delegates(ctx):
    """LocalLifecycleHandler.start delegates to AsyncTableWorker.start."""
    handler = LocalLifecycleHandler(MagicMock())
    mock_worker = AsyncMock()
    handler._worker = mock_worker

    await handler.start()

    mock_worker.start.assert_called_once()


async def test_local_lifecycle_stop_delegates(ctx):
    """LocalLifecycleHandler.stop delegates to AsyncTableWorker.stop."""
    handler = LocalLifecycleHandler(MagicMock())
    mock_worker = AsyncMock()
    handler._worker = mock_worker

    await handler.stop()

    mock_worker.stop.assert_called_once()


# Note: ``test_data_context_always_creates_local_lifecycle`` lives in
# ``aaiclick/data_extra_tests/test_lifecycle.py`` because it requires a real
# ``async with data_context():`` block — the ``ctx`` fixture wraps
# orch_context and gives an ``OrchLifecycleHandler``, not the
# ``LocalLifecycleHandler`` this assertion targets.
