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


async def test_local_lifecycle_delegates_to_worker(ctx):
    """LocalLifecycleHandler passes incref/decref/start/stop through to AsyncTableWorker."""
    handler = LocalLifecycleHandler(MagicMock())
    mock_worker = AsyncMock()
    # incref/decref are sync on AsyncTableWorker — plain mocks avoid
    # unawaited-coroutine warnings from AsyncMock child attributes.
    mock_worker.incref = MagicMock()
    mock_worker.decref = MagicMock()
    handler._worker = mock_worker

    handler.incref("table_123")
    handler.decref("table_456")
    await handler.start()
    await handler.stop()

    mock_worker.incref.assert_called_once_with("table_123")
    mock_worker.decref.assert_called_once_with("table_456")
    mock_worker.start.assert_called_once()
    mock_worker.stop.assert_called_once()


# Note: ``test_data_context_always_creates_local_lifecycle`` lives in
# ``aaiclick/data_extra_tests/test_lifecycle.py`` because it requires a real
# ``async with data_context():`` block — the ``ctx`` fixture wraps
# orch_context and gives an ``OrchLifecycleHandler``, not the
# ``LocalLifecycleHandler`` this assertion targets.
