"""
Tests for LifecycleHandler ABC and LocalLifecycleHandler.
"""

from unittest.mock import MagicMock, patch

from aaiclick.data.lifecycle import LifecycleHandler, LocalLifecycleHandler
from aaiclick.data.models import ClickHouseCreds


def test_lifecycle_handler_is_abstract():
    """LifecycleHandler cannot be instantiated directly."""
    import pytest

    with pytest.raises(TypeError):
        LifecycleHandler()


def test_local_lifecycle_delegates_incref():
    """LocalLifecycleHandler.incref delegates to TableWorker."""
    creds = ClickHouseCreds()
    handler = LocalLifecycleHandler(creds)
    handler._worker = MagicMock()

    handler.incref("table_123")

    handler._worker.incref.assert_called_once_with("table_123")


def test_local_lifecycle_delegates_decref():
    """LocalLifecycleHandler.decref delegates to TableWorker."""
    creds = ClickHouseCreds()
    handler = LocalLifecycleHandler(creds)
    handler._worker = MagicMock()

    handler.decref("table_456")

    handler._worker.decref.assert_called_once_with("table_456")


async def test_local_lifecycle_start_delegates():
    """LocalLifecycleHandler.start delegates to TableWorker.start."""
    creds = ClickHouseCreds()
    handler = LocalLifecycleHandler(creds)
    handler._worker = MagicMock()

    await handler.start()

    handler._worker.start.assert_called_once()


async def test_local_lifecycle_stop_delegates():
    """LocalLifecycleHandler.stop delegates to TableWorker.stop."""
    creds = ClickHouseCreds()
    handler = LocalLifecycleHandler(creds)
    handler._worker = MagicMock()

    await handler.stop()

    handler._worker.stop.assert_called_once()


async def test_data_context_creates_local_lifecycle_by_default():
    """DataContext creates LocalLifecycleHandler when no lifecycle injected."""
    from aaiclick import DataContext, create_object_from_value

    async with DataContext() as ctx:
        assert isinstance(ctx._lifecycle, LocalLifecycleHandler)
        assert ctx._owns_lifecycle is True

        obj = await create_object_from_value([1, 2, 3])
        data = await obj.data()
        assert data == [1, 2, 3]


async def test_data_context_uses_injected_lifecycle():
    """DataContext uses injected lifecycle handler without owning it."""
    from aaiclick import DataContext, create_object_from_value

    mock_lifecycle = MagicMock(spec=LifecycleHandler)

    async with DataContext(lifecycle=mock_lifecycle) as ctx:
        assert ctx._lifecycle is mock_lifecycle
        assert ctx._owns_lifecycle is False

        # start should NOT have been called (not owned)
        mock_lifecycle.start.assert_not_called()

    # stop should NOT have been called (not owned)
    mock_lifecycle.stop.assert_not_called()


async def test_data_context_injected_lifecycle_receives_incref_decref():
    """Injected lifecycle handler receives incref/decref calls."""
    from aaiclick import DataContext, create_object_from_value

    mock_lifecycle = MagicMock(spec=LifecycleHandler)

    async with DataContext(lifecycle=mock_lifecycle) as ctx:
        ctx.incref("test_table")
        ctx.decref("test_table")

    mock_lifecycle.incref.assert_called_once_with("test_table")
    mock_lifecycle.decref.assert_called_once_with("test_table")
