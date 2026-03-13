"""
Tests for LifecycleHandler ABC and LocalLifecycleHandler.
"""

from unittest.mock import MagicMock, patch

import pytest

from aaiclick import create_object_from_value
from aaiclick.backend import get_ch_url
from aaiclick.data.data_context import _get_data_state, data_context, incref, decref
from aaiclick.data.lifecycle import LifecycleHandler, LocalLifecycleHandler


def test_lifecycle_handler_is_abstract():
    """LifecycleHandler cannot be instantiated directly."""
    with pytest.raises(TypeError):
        LifecycleHandler()


def test_local_lifecycle_delegates_incref():
    """LocalLifecycleHandler.incref delegates to TableWorker."""
    handler = LocalLifecycleHandler(get_ch_url())
    handler._worker = MagicMock()

    handler.incref("table_123")

    handler._worker.incref.assert_called_once_with("table_123")


def test_local_lifecycle_delegates_decref():
    """LocalLifecycleHandler.decref delegates to TableWorker."""
    handler = LocalLifecycleHandler(get_ch_url())
    handler._worker = MagicMock()

    handler.decref("table_456")

    handler._worker.decref.assert_called_once_with("table_456")


async def test_local_lifecycle_start_delegates():
    """LocalLifecycleHandler.start delegates to TableWorker.start."""
    handler = LocalLifecycleHandler(get_ch_url())
    handler._worker = MagicMock()

    await handler.start()

    handler._worker.start.assert_called_once()


async def test_local_lifecycle_stop_delegates():
    """LocalLifecycleHandler.stop delegates to TableWorker.stop."""
    handler = LocalLifecycleHandler(get_ch_url())
    handler._worker = MagicMock()

    await handler.stop()

    handler._worker.stop.assert_called_once()


async def test_data_context_creates_local_lifecycle_by_default():
    """data_context creates LocalLifecycleHandler when no lifecycle injected."""
    async with data_context():
        state = _get_data_state()
        assert isinstance(state.lifecycle, LocalLifecycleHandler)
        assert state.owns_lifecycle is True

        obj = await create_object_from_value([1, 2, 3])
        data = await obj.data()
        assert data == [1, 2, 3]


async def test_data_context_uses_injected_lifecycle():
    """data_context uses injected lifecycle handler without owning it."""
    mock_lifecycle = MagicMock(spec=LifecycleHandler)

    async with data_context(lifecycle=mock_lifecycle):
        state = _get_data_state()
        assert state.lifecycle is mock_lifecycle
        assert state.owns_lifecycle is False

        # start should NOT have been called (not owned)
        mock_lifecycle.start.assert_not_called()

    # stop should NOT have been called (not owned)
    mock_lifecycle.stop.assert_not_called()


async def test_data_context_injected_lifecycle_receives_incref_decref():
    """Injected lifecycle handler receives incref/decref calls."""
    mock_lifecycle = MagicMock(spec=LifecycleHandler)

    async with data_context(lifecycle=mock_lifecycle):
        incref("test_table")
        decref("test_table")

    mock_lifecycle.incref.assert_called_once_with("test_table")
    mock_lifecycle.decref.assert_called_once_with("test_table")
