"""
Tests for LifecycleHandler ABC and LocalLifecycleHandler.
"""

from unittest.mock import MagicMock

import pytest

from aaiclick import create_object_from_value
from aaiclick.backend import get_ch_url
from aaiclick.data.data_context import data_context
from aaiclick.data.lifecycle import LifecycleHandler, LocalLifecycleHandler, get_data_lifecycle


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


async def test_data_context_always_creates_local_lifecycle():
    """data_context always creates and owns a LocalLifecycleHandler."""
    async with data_context():
        assert isinstance(get_data_lifecycle(), LocalLifecycleHandler)

        obj = await create_object_from_value([1, 2, 3])
        data = await obj.data()
        assert data == [1, 2, 3]
